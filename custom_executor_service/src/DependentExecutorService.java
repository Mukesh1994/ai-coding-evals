import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * Custom executor service for executing batches of dependent tasks.
 * Automatically resolves dependencies and executes tasks in parallel when possible.
 * Passes results from completed tasks to dependent tasks.
 */
public class DependentExecutorService {
    private static final Logger logger = LoggerFactory.getLogger(DependentExecutorService.class);

    private final ExecutorService executorService;
    private final Map<String, TaskBatch> batches;
    private final Map<String, CompletableFuture<BatchResult>> batchFutures;
    private final int maxConcurrency;
    private final boolean failFastOnError;

    private volatile boolean running = false;

    /**
     * Creates a new dependent executor service with default settings.
     */
    public DependentExecutorService() {
        this(Runtime.getRuntime().availableProcessors() * 2, true);
    }

    /**
     * Creates a new dependent executor service with custom settings.
     *
     * @param maxConcurrency Maximum number of concurrent tasks
     * @param failFastOnError Whether to stop execution on first error
     */
    public DependentExecutorService(int maxConcurrency, boolean failFastOnError) {
        this.maxConcurrency = maxConcurrency;
        this.failFastOnError = failFastOnError;
        this.executorService = Executors.newFixedThreadPool(maxConcurrency, r -> {
            Thread t = new Thread(r, "DependentExecutor-Worker");
            t.setDaemon(false);
            return t;
        });
        this.batches = new ConcurrentHashMap<>();
        this.batchFutures = new ConcurrentHashMap<>();
        this.running = true;

        logger.info("Dependent executor service created with maxConcurrency={}, failFast={}",
                maxConcurrency, failFastOnError);
    }

    /**
     * Submits a batch of dependent tasks for execution.
     *
     * @param batch The task batch to execute
     * @return CompletableFuture with the batch result
     */
    public CompletableFuture<BatchResult> submitBatch(TaskBatch batch) {
        if (!running) {
            throw new IllegalStateException("Executor service is not running");
        }

        String batchId = batch.getBatchId();
        logger.info("Submitting batch: {} with {} tasks", batchId, batch.getTaskCount());

        batches.put(batchId, batch);
        batch.setState(TaskBatch.BatchState.QUEUED);

        CompletableFuture<BatchResult> future = CompletableFuture.supplyAsync(() -> {
            try {
                return executeBatch(batch);
            } catch (Exception e) {
                logger.error("Error executing batch: " + batchId, e);
                return new BatchResult(batch, false, e);
            }
        }, executorService);

        batchFutures.put(batchId, future);
        return future;
    }

    /**
     * Executes a batch of tasks, respecting dependencies.
     */
    private BatchResult executeBatch(TaskBatch batch) {
        String batchId = batch.getBatchId();
        logger.info("Starting execution of batch: {}", batchId);

        batch.setState(TaskBatch.BatchState.EXECUTING);
        long startTime = System.currentTimeMillis();

        Set<String> completedTasks = ConcurrentHashMap.newKeySet();
        Set<String> failedTasks = ConcurrentHashMap.newKeySet();
        Map<String, Future<?>> runningTasks = new ConcurrentHashMap<>();
        Exception batchError = null;

        try {
            // Execute tasks in waves based on dependencies
            while (!batch.isComplete()) {
                // Get tasks that are ready to execute
                List<DependentTask<?>> readyTasks = batch.getReadyTasks();

                if (readyTasks.isEmpty() && runningTasks.isEmpty()) {
                    // No ready tasks and no running tasks - likely all failed
                    break;
                }

                // Submit ready tasks
                for (DependentTask<?> task : readyTasks) {
                    if (failFastOnError && !failedTasks.isEmpty()) {
                        logger.info("Stopping batch execution due to previous failures");
                        break;
                    }

                    task.setState(DependentTask.TaskState.READY);

                    Future<?> taskFuture = executorService.submit(() -> {
                        executeTask(batch, task, completedTasks, failedTasks);
                    });

                    runningTasks.put(task.getTaskId(), taskFuture);
                }

                // Wait for at least one task to complete before checking for more ready tasks
                if (!runningTasks.isEmpty()) {
                    waitForAnyTaskCompletion(runningTasks);
                }

                // Small delay to avoid busy-waiting
                if (readyTasks.isEmpty() && !runningTasks.isEmpty()) {
                    Thread.sleep(10);
                }
            }

            // Wait for all remaining tasks to complete
            for (Future<?> future : runningTasks.values()) {
                try {
                    future.get();
                } catch (Exception e) {
                    logger.error("Error waiting for task completion", e);
                }
            }

        } catch (Exception e) {
            logger.error("Error during batch execution: " + batchId, e);
            batchError = e;
        }

        long endTime = System.currentTimeMillis();
        boolean success = failedTasks.isEmpty() && batchError == null;

        batch.setState(success ? TaskBatch.BatchState.COMPLETED : TaskBatch.BatchState.FAILED);

        logger.info("Batch {} completed in {}ms. Success: {}, Completed: {}, Failed: {}",
                batchId, (endTime - startTime), success, completedTasks.size(), failedTasks.size());

        return new BatchResult(batch, success, batchError);
    }

    /**
     * Executes a single task with its dependencies' results.
     */
    private void executeTask(TaskBatch batch, DependentTask<?> task,
                             Set<String> completedTasks, Set<String> failedTasks) {
        String taskId = task.getTaskId();
        logger.debug("Executing task: {}", taskId);

        try {
            // Get results from dependency tasks
            Map<String, Object> dependencyResults = batch.getDependencyResults(taskId);

            // Execute the task
            Object result = task.execute(dependencyResults);

            // Record the result
            batch.recordResult(taskId, result);
            completedTasks.add(taskId);

            logger.debug("Task {} completed successfully", taskId);

        } catch (Exception e) {
            failedTasks.add(taskId);
            logger.error("Task {} failed", taskId, e);

            // Mark dependent tasks as cancelled if fail-fast is enabled
            if (failFastOnError) {
                cancelDependentTasks(batch, taskId);
            }
        }
    }

    /**
     * Cancels tasks that depend on a failed task.
     */
    private void cancelDependentTasks(TaskBatch batch, String failedTaskId) {
        for (DependentTask<?> task : batch.getAllTasks()) {
            if (task.getDependencyIds().contains(failedTaskId) &&
                    task.getState() == DependentTask.TaskState.PENDING) {
                task.setState(DependentTask.TaskState.CANCELLED);
                logger.debug("Cancelled task {} due to dependency failure", task.getTaskId());
            }
        }
    }

    /**
     * Waits for any task to complete.
     */
    private void waitForAnyTaskCompletion(Map<String, Future<?>> runningTasks) {
        Iterator<Map.Entry<String, Future<?>>> iterator = runningTasks.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Future<?>> entry = iterator.next();
            if (entry.getValue().isDone()) {
                iterator.remove();
                return;
            }
        }
    }

    /**
     * Gets the status of a batch.
     */
    public TaskBatch.BatchStatistics getBatchStatistics(String batchId) {
        TaskBatch batch = batches.get(batchId);
        if (batch == null) {
            throw new IllegalArgumentException("Batch not found: " + batchId);
        }
        return batch.getStatistics();
    }

    /**
     * Gets a batch by ID.
     */
    public TaskBatch getBatch(String batchId) {
        return batches.get(batchId);
    }

    /**
     * Waits for a batch to complete.
     */
    public BatchResult waitForBatch(String batchId) throws InterruptedException, ExecutionException {
        CompletableFuture<BatchResult> future = batchFutures.get(batchId);
        if (future == null) {
            throw new IllegalArgumentException("Batch not found: " + batchId);
        }
        return future.get();
    }

    /**
     * Waits for a batch to complete with timeout.
     */
    public BatchResult waitForBatch(String batchId, long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<BatchResult> future = batchFutures.get(batchId);
        if (future == null) {
            throw new IllegalArgumentException("Batch not found: " + batchId);
        }
        return future.get(timeout, unit);
    }

    /**
     * Gets all batch IDs.
     */
    public Set<String> getAllBatchIds() {
        return new HashSet<>(batches.keySet());
    }

    /**
     * Shuts down the executor service gracefully.
     */
    public void shutdown() {
        if (!running) {
            return;
        }

        logger.info("Shutting down dependent executor service...");
        running = false;
        executorService.shutdown();

        try {
            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }

        logger.info("Dependent executor service shut down");
    }

    /**
     * Shuts down the executor immediately.
     */
    public void shutdownNow() {
        if (!running) {
            return;
        }

        logger.warn("Force shutting down dependent executor service...");
        running = false;
        executorService.shutdownNow();
    }

    /**
     * Result of batch execution.
     */
    public static class BatchResult {
        private final TaskBatch batch;
        private final boolean success;
        private final Exception error;
        private final long completionTime;

        public BatchResult(TaskBatch batch, boolean success, Exception error) {
            this.batch = batch;
            this.success = success;
            this.error = error;
            this.completionTime = System.currentTimeMillis();
        }

        public TaskBatch getBatch() { return batch; }
        public boolean isSuccess() { return success; }
        public Exception getError() { return error; }
        public long getCompletionTime() { return completionTime; }

        /**
         * Gets all task results.
         */
        public Map<String, Object> getAllResults() {
            Map<String, Object> results = new HashMap<>();
            for (DependentTask<?> task : batch.getAllTasks()) {
                if (task.getState() == DependentTask.TaskState.COMPLETED) {
                    results.put(task.getTaskId(), task.getResult());
                }
            }
            return results;
        }

        /**
         * Gets result of a specific task.
         */
        public <T> T getResult(String taskId, Class<T> type) {
            DependentTask<?> task = batch.getTask(taskId);
            if (task == null) {
                return null;
            }
            Object result = task.getResult();
            return type.isInstance(result) ? type.cast(result) : null;
        }

        /**
         * Gets failed tasks.
         */
        public List<DependentTask<?>> getFailedTasks() {
            return batch.getAllTasks().stream()
                    .filter(t -> t.getState() == DependentTask.TaskState.FAILED)
                    .collect(Collectors.toList());
        }

        @Override
        public String toString() {
            return String.format("BatchResult{batch=%s, success=%s, stats=%s}",
                    batch.getBatchId(), success, batch.getStatistics());
        }
    }
}

