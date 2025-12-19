

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Represents a batch of dependent tasks that can be executed together.
 * Maintains dependency relationships and execution state.
 */
public class TaskBatch {
    private final String batchId;
    private final Map<String, DependentTask<?>> tasks;
    private final Map<String, Object> results;
    private final Map<String, Set<String>> dependencyGraph;
    private final Map<String, Set<String>> reverseDependencyGraph;
    private final long creationTime;

    private volatile BatchState state;
    private long startTime;
    private long endTime;

    public TaskBatch(String batchId, Collection<DependentTask<?>> tasks) {
        this.batchId = batchId;
        this.tasks = new ConcurrentHashMap<>();
        this.results = new ConcurrentHashMap<>();
        this.dependencyGraph = new HashMap<>();
        this.reverseDependencyGraph = new HashMap<>();
        this.creationTime = System.currentTimeMillis();
        this.state = BatchState.CREATED;

        // Add tasks and build dependency graph
        for (DependentTask<?> task : tasks) {
            addTask(task);
        }

        // Validate no circular dependencies
        validateNoCycles();
        
    }

    private void addTask(DependentTask<?> task) {
        String taskId = task.getTaskId();

        if (tasks.containsKey(taskId)) {
            throw new IllegalArgumentException("Duplicate task ID: " + taskId);
        }

        tasks.put(taskId, task);
        dependencyGraph.put(taskId, new HashSet<>(task.getDependencyIds()));

        // Build reverse dependency graph (who depends on me)
        for (String depId : task.getDependencyIds()) {
            reverseDependencyGraph
                    .computeIfAbsent(depId, k -> new HashSet<>())
                    .add(taskId);
        }
    }

    /**
     * Validates that there are no circular dependencies.
     */
    private void validateNoCycles() {
        Set<String> visited = new HashSet<>();
        Set<String> recursionStack = new HashSet<>();

        for (String taskId : tasks.keySet()) {
            if (hasCycle(taskId, visited, recursionStack)) {
                throw new IllegalArgumentException("Circular dependency detected involving task: " + taskId);
            }
        }
    }

    private boolean hasCycle(String taskId, Set<String> visited, Set<String> recursionStack) {
        if (recursionStack.contains(taskId)) {
            return true;
        }
        if (visited.contains(taskId)) {
            return false;
        }

        visited.add(taskId);
        recursionStack.add(taskId);

        Set<String> dependencies = dependencyGraph.get(taskId);
        if (dependencies != null) {
            for (String depId : dependencies) {
                if (!tasks.containsKey(depId)) {
                    throw new IllegalArgumentException(
                            "Task " + taskId + " depends on non-existent task: " + depId
                    );
                }
                if (hasCycle(depId, visited, recursionStack)) {
                    return true;
                }
            }
        }

        recursionStack.remove(taskId);
        return false;
    }

    /**
     * Gets tasks that are ready to execute (all dependencies satisfied).
     */
    public List<DependentTask<?>> getReadyTasks() {
        Set<String> completedTaskIds = tasks.values().stream()
                .filter(t -> t.getState() == DependentTask.TaskState.COMPLETED)
                .map(DependentTask::getTaskId)
                .collect(Collectors.toSet());

        return tasks.values().stream()
                .filter(t -> t.getState() == DependentTask.TaskState.PENDING)
                .filter(t -> t.isReady(completedTaskIds))
                .sorted((t1, t2) -> Integer.compare(
                        t2.getPriority().getValue(),
                        t1.getPriority().getValue()
                ))
                .collect(Collectors.toList());
    }

    /**
     * Gets tasks that have no dependencies (can run immediately).
     */
    public List<DependentTask<?>> getRootTasks() {
        return tasks.values().stream()
                .filter(t -> !t.hasDependencies())
                .sorted((t1, t2) -> Integer.compare(
                        t2.getPriority().getValue(),
                        t1.getPriority().getValue()
                ))
                .collect(Collectors.toList());
    }

    /**
     * Records the result of a completed task.
     */
    public void recordResult(String taskId, Object result) {
        if (result != null) {
            results.put(taskId, result);
        }
    }

    /**
     * Gets the result of a completed task.
     */
    public Object getResult(String taskId) {
        return results.get(taskId);
    }

    /**
     * Gets results from all dependency tasks.
     */
    public Map<String, Object> getDependencyResults(String taskId) {
        DependentTask<?> task = tasks.get(taskId);
        if (task == null) {
            return Collections.emptyMap();
        }

        Map<String, Object> depResults = new HashMap<>();
        for (String depId : task.getDependencyIds()) {
            Object result = results.get(depId);
            if (result != null) {
                depResults.put(depId, result);
            }
        }
        return depResults;
    }

    /**
     * Gets task by ID.
     */
    public DependentTask<?> getTask(String taskId) {
        return tasks.get(taskId);
    }

    /**
     * Gets all tasks in the batch.
     */
    public Collection<DependentTask<?>> getAllTasks() {
        return new ArrayList<>(tasks.values());
    }

    /**
     * Checks if the batch is complete (all tasks finished).
     */
    public boolean isComplete() {
        return tasks.values().stream().allMatch(t ->
                t.getState() == DependentTask.TaskState.COMPLETED ||
                        t.getState() == DependentTask.TaskState.FAILED ||
                        t.getState() == DependentTask.TaskState.CANCELLED
        );
    }

    /**
     * Checks if any task in the batch has failed.
     */
    public boolean hasFailed() {
        return tasks.values().stream().anyMatch(t ->
                t.getState() == DependentTask.TaskState.FAILED
        );
    }

    /**
     * Gets summary statistics for the batch.
     */
    public BatchStatistics getStatistics() {
        long pending = tasks.values().stream()
                .filter(t -> t.getState() == DependentTask.TaskState.PENDING).count();
        long running = tasks.values().stream()
                .filter(t -> t.getState() == DependentTask.TaskState.RUNNING).count();
        long completed = tasks.values().stream()
                .filter(t -> t.getState() == DependentTask.TaskState.COMPLETED).count();
        long failed = tasks.values().stream()
                .filter(t -> t.getState() == DependentTask.TaskState.FAILED).count();

        return new BatchStatistics(
                tasks.size(),
                (int) pending,
                (int) running,
                (int) completed,
                (int) failed,
                startTime > 0 ? System.currentTimeMillis() - startTime : 0
        );
    }

    // Getters
    public String getBatchId() { return batchId; }
    public BatchState getState() { return state; }
    public int getTaskCount() { return tasks.size(); }

    void setState(BatchState state) {
        this.state = state;
        if (state == BatchState.EXECUTING && startTime == 0) {
            startTime = System.currentTimeMillis();
        } else if (state == BatchState.COMPLETED || state == BatchState.FAILED) {
            endTime = System.currentTimeMillis();
        }
    }

    public enum BatchState {
        CREATED,    // Batch created, not yet submitted
        QUEUED,     // Submitted, waiting to execute
        EXECUTING,  // Currently executing
        COMPLETED,  // All tasks completed successfully
        FAILED,     // One or more tasks failed
        CANCELLED   // Batch was cancelled
    }

    public static class BatchStatistics {
        private final int totalTasks;
        private final int pendingTasks;
        private final int runningTasks;
        private final int completedTasks;
        private final int failedTasks;
        private final long elapsedTimeMs;

        public BatchStatistics(int totalTasks, int pendingTasks, int runningTasks,
                               int completedTasks, int failedTasks, long elapsedTimeMs) {
            this.totalTasks = totalTasks;
            this.pendingTasks = pendingTasks;
            this.runningTasks = runningTasks;
            this.completedTasks = completedTasks;
            this.failedTasks = failedTasks;
            this.elapsedTimeMs = elapsedTimeMs;
        }

        public int getTotalTasks() { return totalTasks; }
        public int getPendingTasks() { return pendingTasks; }
        public int getRunningTasks() { return runningTasks; }
        public int getCompletedTasks() { return completedTasks; }
        public int getFailedTasks() { return failedTasks; }
        public long getElapsedTimeMs() { return elapsedTimeMs; }
        public double getCompletionPercentage() {
            return totalTasks > 0 ? (double) completedTasks / totalTasks * 100 : 0;
        }

        @Override
        public String toString() {
            return String.format(
                    "BatchStats{total=%d, completed=%d, running=%d, pending=%d, failed=%d, " +
                            "progress=%.1f%%, elapsed=%dms}",
                    totalTasks, completedTasks, runningTasks, pendingTasks, failedTasks,
                    getCompletionPercentage(), elapsedTimeMs
            );
        }
    }
}

