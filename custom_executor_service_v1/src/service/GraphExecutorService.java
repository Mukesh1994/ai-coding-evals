package service;
import model.DependentTask;
import model.TaskContext;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class GraphExecutorService {
    private final ExecutorService executor;

    public GraphExecutorService(int poolSize) {
        this.executor = Executors.newFixedThreadPool(poolSize);
    }

    public CompletableFuture<TaskContext> executeBatch(List<DependentTask<?>> tasks) {
        // 1. Validate the graph structure (Cycles and Missing dependencies)
        validateGraph(tasks);

        TaskContext context = new TaskContext();
        Map<String, DependentTask<?>> taskMap = tasks.stream()
                .collect(Collectors.toMap(DependentTask::getId, t -> t));

        // Use ConcurrentHashMap to track futures as they are created
        Map<String, CompletableFuture<Object>> futures = new ConcurrentHashMap<>();

        // 2. Build the Execution Chain recursively
        for (DependentTask<?> task : tasks) {
            getOrCreateFuture(task.getId(), taskMap, futures, context);
        }

        // 3. Return a future that signals when the entire batch is finished
        return CompletableFuture.allOf(futures.values().toArray(new CompletableFuture[0]))
                .thenApply(v -> context);
    }

    private CompletableFuture<Object> getOrCreateFuture(
            String taskId,
            Map<String, DependentTask<?>> taskMap,
            Map<String, CompletableFuture<Object>> futures,
            TaskContext context) {

        // Use computeIfAbsent to avoid manual synchronization and duplicate creation
        return futures.computeIfAbsent(taskId, id -> {
            DependentTask<?> task = taskMap.get(id);

            // Collect futures for all dependencies
            List<CompletableFuture<Object>> dependencyFutures = task.getDependencyIds().stream()
                    .map(depId -> getOrCreateFuture(depId, taskMap, futures, context))
                    .collect(Collectors.toList());

            if (dependencyFutures.isEmpty()) {
                // Root tasks (no parents): Execute immediately
                return CompletableFuture.supplyAsync(() ->
                        performExecution(task, Collections.emptyMap(), context), executor);
            } else {
                // Dependent tasks: Wait for all parents, then gather results and execute
                return CompletableFuture.allOf(dependencyFutures.toArray(new CompletableFuture[0]))
                        .thenApplyAsync(v -> {
                            // OPTIMAL LOGIC: Extract parent results to pass to child
                            Map<String, Object> inputData = new HashMap<>();
                            for (String depId : task.getDependencyIds()) {
                                // .join() is safe here because allOf() guaranteed completion
                                inputData.put(depId, futures.get(depId).join());
                            }
                            return performExecution(task, inputData, context);
                        }, executor);
            }
        });
    }

    private Object performExecution(DependentTask<?> task, Map<String, Object> inputData, TaskContext context) {
        try {
            System.out.println("Starting Task: " + task.getId() + " on thread " + Thread.currentThread().getName());
            Object result = task.execute(inputData, context);
            context.addResult(task.getId(), result);
            return result;
        } catch (Exception e) {
            System.err.println("Task " + task.getId() + " failed: " + e.getMessage());
            throw new CompletionException(e); // Properly propagate exceptions in the future chain
        }
    }

    private void validateGraph(List<DependentTask<?>> tasks) {
        Map<String, DependentTask<?>> taskMap = tasks.stream()
                .collect(Collectors.toMap(DependentTask::getId, t -> t));
        Set<String> visited = new HashSet<>();
        Set<String> recursionStack = new HashSet<>();

        for (DependentTask<?> task : tasks) {
            if (detectCycle(task.getId(), taskMap, visited, recursionStack)) {
                throw new IllegalArgumentException("Circular dependency detected involving task: " + task.getId());
            }
        }
    }

    private boolean detectCycle(String taskId, Map<String, DependentTask<?>> taskMap,
                                Set<String> visited, Set<String> recursionStack) {
        if (recursionStack.contains(taskId)) return true;
        if (visited.contains(taskId)) return false;

        visited.add(taskId);
        recursionStack.add(taskId);

        DependentTask<?> task = taskMap.get(taskId);
        if (task != null) {
            for (String depId : task.getDependencyIds()) {
                if (!taskMap.containsKey(depId)) {
                    throw new IllegalArgumentException("Task " + taskId + " depends on missing task " + depId);
                }
                if (detectCycle(depId, taskMap, visited, recursionStack)) return true;
            }
        }
        recursionStack.remove(taskId);
        return false;
    }

    public void shutdown() {
        executor.shutdown();
    }
}