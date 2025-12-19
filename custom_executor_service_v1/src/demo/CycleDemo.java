package demo;

import model.DependentTask;
import service.GraphExecutorService;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class CycleDemo {
    public static void main(String[] args) {
        GraphExecutorService service = new GraphExecutorService(3);

        DependentTask<String> task1 = new DependentTask<>("task1", Set.of("task2"), (context) -> "A");
        DependentTask<String> task2 = new DependentTask<>("task2", Set.of("task1"), (context) -> "B");

        List<DependentTask<?>> tasks = Arrays.asList(task1, task2);

        try {
            System.out.println("Submitting cyclic batch...");
            service.executeBatch(tasks).get();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            service.shutdown();
        }
    }
}

