package model;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public class DependentTask<T> {
    private final String id;
    private final Set<String> dependencyIds;
    private final Function<TaskContext, T> action;

    public DependentTask(String id, Set<String> dependencyIds, Function<TaskContext, T> action) {
        this.id = id;
        this.dependencyIds = dependencyIds != null ? dependencyIds : Collections.emptySet();
        this.action = action;
    }

    public DependentTask(String id, Function<TaskContext, T> action) {
        this(id, new HashSet<>(), action);
    }

    public String getId() {
        return id;
    }

    public Set<String> getDependencyIds() {
        return dependencyIds;
    }

    public T execute(Map<String, Object> dependencyData, TaskContext context) {
        // use dependencyData for results,
        return action.apply(context);
    }
}
