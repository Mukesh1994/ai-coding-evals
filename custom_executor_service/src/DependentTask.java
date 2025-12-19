
import java.util.*;
import java.util.concurrent.Callable;
import java.util.function.Function;

/**
 * Represents a task that can depend on results from other tasks.
 * Supports both Callable (returns result) and Runnable (no result) patterns.
 *
 * @param <T> The type of result this task produces
 */
public class DependentTask<T> {
    private final String taskId;
    private final Callable<T> callable;
    private final Set<String> dependencyIds;
    private final Map<String, Class<?>> expectedDependencyTypes;
    private final TaskPriority priority;

    private T result;
    private Throwable error;
    private TaskState state;
    private long startTime;
    private long endTime;

    private DependentTask(Builder<T> builder) {
        this.taskId = builder.taskId;
        this.callable = builder.callable;
        this.dependencyIds = new HashSet<>(builder.dependencyIds);
        this.expectedDependencyTypes = new HashMap<>(builder.expectedDependencyTypes);
        this.priority = builder.priority != null ? builder.priority : TaskPriority.NORMAL;
        this.state = TaskState.PENDING;
    }

    /**
     * Executes the task with results from its dependencies.
     */
    public T execute(Map<String, Object> dependencyResults) throws Exception {
        if (state != TaskState.PENDING && state != TaskState.READY) {
            throw new IllegalStateException("Task " + taskId + " is in state " + state);
        }

        state = TaskState.RUNNING;
        startTime = System.currentTimeMillis();

        try {
            result = callable.call();
            state = TaskState.COMPLETED;
            endTime = System.currentTimeMillis();
            return result;
        } catch (Exception e) {
            error = e;
            state = TaskState.FAILED;
            endTime = System.currentTimeMillis();
            throw e;
        }
    }

    /**
     * Checks if this task is ready to execute (all dependencies satisfied).
     */
    public boolean isReady(Set<String> completedTaskIds) {
        return completedTaskIds.containsAll(dependencyIds);
    }

    // Getters
    public String getTaskId() { return taskId; }
    public Set<String> getDependencyIds() { return new HashSet<>(dependencyIds); }
    public TaskState getState() { return state; }
    public T getResult() { return result; }
    public Throwable getError() { return error; }
    public long getExecutionTime() {
        return endTime > 0 ? endTime - startTime : 0;
    }
    public TaskPriority getPriority() { return priority; }
    public boolean hasDependencies() { return !dependencyIds.isEmpty(); }

    void setState(TaskState state) { this.state = state; }

    @Override
    public String toString() {
        return String.format("Task{id=%s, state=%s, deps=%s}", taskId, state, dependencyIds);
    }

    /**
     * Builder for creating dependent tasks.
     */
    public static class Builder<T> {
        private String taskId;
        private Callable<T> callable;
        private final Set<String> dependencyIds = new HashSet<>();
        private final Map<String, Class<?>> expectedDependencyTypes = new HashMap<>();
        private TaskPriority priority;

        public Builder(String taskId) {
            this.taskId = taskId;
        }

        /**
         * Sets the task logic as a Callable that returns a result.
         */
        public Builder<T> callable(Callable<T> callable) {
            this.callable = callable;
            return this;
        }

        /**
         * Sets the task logic as a Function that takes dependency results.
         */
        public Builder<T> function(Function<Map<String, Object>, T> function) {
            this.callable = () -> function.apply(new HashMap<>());
            return this;
        }

        /**
         * Sets the task logic as a Runnable (no result).
         */
        public Builder<T> runnable(Runnable runnable) {
            this.callable = () -> {
                runnable.run();
                return null;
            };
            return this;
        }

        /**
         * Adds a dependency on another task.
         */
        public Builder<T> dependsOn(String taskId) {
            this.dependencyIds.add(taskId);
            return this;
        }

        /**
         * Adds a dependency with expected result type.
         */
        public Builder<T> dependsOn(String taskId, Class<?> expectedType) {
            this.dependencyIds.add(taskId);
            this.expectedDependencyTypes.put(taskId, expectedType);
            return this;
        }

        /**
         * Adds multiple dependencies.
         */
        public Builder<T> dependsOn(String... taskIds) {
            Collections.addAll(this.dependencyIds, taskIds);
            return this;
        }

        /**
         * Sets task priority.
         */
        public Builder<T> priority(TaskPriority priority) {
            this.priority = priority;
            return this;
        }

        public DependentTask<T> build() {
            if (taskId == null || taskId.trim().isEmpty()) {
                throw new IllegalArgumentException("Task ID cannot be null or empty");
            }
            if (callable == null) {
                throw new IllegalArgumentException("Task must have callable logic");
            }
            return new DependentTask<>(this);
        }
    }

    /**
     * Task execution state.
     */
    public enum TaskState {
        PENDING,    // Not yet ready (dependencies not satisfied)
        READY,      // Ready to execute (all dependencies satisfied)
        RUNNING,    // Currently executing
        COMPLETED,  // Successfully completed
        FAILED,     // Failed with error
        CANCELLED   // Cancelled by user or system
    }

    /**
     * Task priority for scheduling.
     */
    public enum TaskPriority {
        HIGH(3),
        NORMAL(2),
        LOW(1);

        private final int value;

        TaskPriority(int value) {
            this.value = value;
        }

        public int getValue() { return value; }
    }
}

