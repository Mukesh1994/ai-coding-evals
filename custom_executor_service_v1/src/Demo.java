import model.DependentTask;
import model.TaskContext;
import service.GraphExecutorService;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.ExecutionException;

public class Demo {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        GraphExecutorService executorService = new GraphExecutorService(3);

        // Task 1: No dependencies
        DependentTask<String> task1 = new DependentTask<>("task1", context -> {
            System.out.println("Running Task 1");
            try { Thread.sleep(500); } catch (InterruptedException e) { }
            return "Result from Task 1";
        });

        // Task 2: Depends on Task 1
        DependentTask<String> task2 = new DependentTask<>("task2", new HashSet<>(Collections.singletonList("task1")), context -> {
            System.out.println("Running Task 2");
            String t1Result = (String) context.getResult("task1");
            return "Result from Task 2 (using " + t1Result + ")";
        });

        // Task 3: Depends on Task 1
        DependentTask<String> task3 = new DependentTask<>("task3", new HashSet<>(Collections.singletonList("task1")), context -> {
            System.out.println("Running Task 3");
            return "Result from Task 3";
        });

        // Task 4: Depends on Task 2 and Task 3
        DependentTask<String> task4 = new DependentTask<>("task4", new HashSet<>(Arrays.asList("task2", "task3")), context -> {
            System.out.println("Running Task 4");
            String t2Result = (String) context.getResult("task2");
            String t3Result = (String) context.getResult("task3");
            return "Final Result: " + t2Result + " & " + t3Result;
        });

        TaskContext context = executorService.executeBatch(Arrays.asList(task1, task2, task3, task4)).get();

        System.out.println("Execution finished.");
        System.out.println("Task 4 Result: " + context.getResult("task4"));

        executorService.shutdown();
    }
}


