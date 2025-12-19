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
        // Validate dependencies and cycles
        validateGraph(tasks);

        TaskContext context = new TaskContext();
        Map<String, DependentTask<?>> taskMap = tasks.stream()
                .collect(Collectors.toMap(DependentTask::getId, t -> t));

        Map<String, CompletableFuture<Object>> futures = new ConcurrentHashMap<>();

        // Create futures for all tasks
        for (DependentTask<?> task : tasks) {
            getOrCreateFuture(task.getId(), taskMap, futures, context);
        }

        // Return a future that completes when all task futures complete
        return CompletableFuture.allOf(futures.values().toArray(new CompletableFuture[0]))
                .thenApply(v -> context);
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
        if (recursionStack.contains(taskId)) {
            return true;
        }
        if (visited.contains(taskId)) {
            return false;
        }

        visited.add(taskId);
        recursionStack.add(taskId);

        DependentTask<?> task = taskMap.get(taskId);
        if (task != null) {
            for (String depId : task.getDependencyIds()) {
                if (!taskMap.containsKey(depId)) {
                    throw new IllegalArgumentException("Task " + taskId + " depends on missing task " + depId);
                }
                if (detectCycle(depId, taskMap, visited, recursionStack)) {
                    return true;
                }
            }
        }

        recursionStack.remove(taskId);
        return false;
    }

    private synchronized CompletableFuture<Object> getOrCreateFuture(
            String taskId,
            Map<String, DependentTask<?>> taskMap,
            Map<String, CompletableFuture<Object>> futures,
            TaskContext context) {

        if (futures.containsKey(taskId)) {
            return futures.get(taskId);
        }

        DependentTask<?> task = taskMap.get(taskId);
        // Note: Missing task check is done in validateGraph, but safe to keep check here or assume valid

        // Get futures for all dependencies
        List<CompletableFuture<Object>> dependencyFutures = new ArrayList<>();
        for (String depId : task.getDependencyIds()) {
            dependencyFutures.add(getOrCreateFuture(depId, taskMap, futures, context));
        }

        CompletableFuture<Object> taskFuture;
        if (dependencyFutures.isEmpty()) {
            // No dependencies, run immediately
            taskFuture = CompletableFuture.supplyAsync(() -> executeTask(task, context), executor);
        } else {
            // Run after all dependencies complete
            taskFuture = CompletableFuture.allOf(dependencyFutures.toArray(new CompletableFuture[0]))
                    .thenApplyAsync(v -> executeTask(task, context), executor);
        }

        futures.put(taskId, taskFuture);
        return taskFuture;
    }

    private Object executeTask(DependentTask<?> task, TaskContext context) {
        System.out.println("Executing task: " + task.getId());
        Object result = task.execute(context);
        context.addResult(task.getId(), result);
        return result;
    }

    public void shutdown() {
        executor.shutdown();
    }
}