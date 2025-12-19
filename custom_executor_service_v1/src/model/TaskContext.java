package model;



import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TaskContext {
    private final Map<String, Object> results = new ConcurrentHashMap<>();

    public void addResult(String taskId, Object result) {
        if (result != null) {
            results.put(taskId, result);
        }
    }

    public Object getResult(String taskId) {
        return results.get(taskId);
    }

    public Map<String, Object> getAllResults() {
        return results;
    }
}

