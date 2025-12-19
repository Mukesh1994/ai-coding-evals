package demo;

import model.DependentTask;
import model.TaskContext;
import service.GraphExecutorService;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class ExecutorDemo {
    public static void main(String[] args) {
        GraphExecutorService service = new GraphExecutorService(3);

        DependentTask<String> task1 = new DependentTask<>("task1", (context) -> {
            System.out.println("Running Task 1");
            try { Thread.sleep(500); } catch (InterruptedException e) {}
            return "Result from Task 1";
        });

        DependentTask<String> task2 = new DependentTask<>("task2", Set.of("task1"), (context) -> {
            System.out.println("Running Task 2");
            String t1Result = (String) context.getResult("task1");
            return "Task 2 received: " + t1Result;
        });

        DependentTask<String> task3 = new DependentTask<>("task3", Set.of("task1"), (context) -> {
            System.out.println("Running Task 3");
            String t1Result = (String) context.getResult("task1");
            return "Task 3 received: " + t1Result;
        });

        DependentTask<String> task4 = new DependentTask<>("task4", Set.of("task2", "task3"), (context) -> {
            System.out.println("Running Task 4");
            String t2Result = (String) context.getResult("task2");
            String t3Result = (String) context.getResult("task3");
            return "Task 4 aggregated: [" + t2Result + "] and [" + t3Result + "]";
        });

        List<DependentTask<?>> tasks = Arrays.asList(task4, task3, task2, task1);

        try {
            System.out.println("Submitting batch...");
            TaskContext context = service.executeBatch(tasks).get();
            System.out.println("Batch execution finished.");
            System.out.println("Final Result (Task 4): " + context.getResult("task4"));
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        } finally {
            service.shutdown();
        }
    }
}

