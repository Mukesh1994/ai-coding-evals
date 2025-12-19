package examples;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Example showing error handling and how failures propagate through dependent tasks.
 */
public class ErrorHandlingExample {
    private static final Logger logger = LoggerFactory.getLogger(ErrorHandlingExample.class);

    public static void main(String[] args) throws Exception {
        logger.info("=== Error Handling Example ===\n");

        // Example 1: Fail-fast mode (default)
        runFailFastExample();

        // Example 2: Continue on error
        runContinueOnErrorExample();
    }

    private static void runFailFastExample() throws Exception {
        logger.info("--- Example 1: Fail-Fast Mode ---\n");

        DependentExecutorService executor = new DependentExecutorService(4, true);

        // Task 1: Succeeds
        DependentTask<String> task1 = new DependentTask.Builder<String>("task1")
                .callable(() -> {
                    logger.info("Task 1: Running...");
                    Thread.sleep(200);
                    logger.info("Task 1: Success");
                    return "Result 1";
                })
                .build();

        // Task 2: Fails
        DependentTask<String> task2 = new DependentTask.Builder<String>("task2")
                .callable(() -> {
                    logger.info("Task 2: Running...");
                    Thread.sleep(300);
                    logger.error("Task 2: Simulating failure!");
                    throw new RuntimeException("Simulated error in task2");
                })
                .build();

        // Task 3: Depends on task2 (should be cancelled)
        DependentTask<String> task3 = new DependentTask.Builder<String>("task3")
                .callable(() -> {
                    logger.info("Task 3: Running...");
                    return "Result 3";
                })
                .dependsOn("task2")
                .build();

        // Task 4: Independent (should still run)
        DependentTask<String> task4 = new DependentTask.Builder<String>("task4")
                .callable(() -> {
                    logger.info("Task 4: Running...");
                    Thread.sleep(400);
                    logger.info("Task 4: Success");
                    return "Result 4";
                })
                .build();

        List<DependentTask<?>> tasks = List.of(task1, task2, task3, task4);
        TaskBatch batch = new TaskBatch("fail-fast-batch", tasks);

        DependentExecutorService.BatchResult result = executor.submitBatch(batch).get();

        logger.info("\n--- Results ---");
        logger.info("Batch success: {}", result.isSuccess());
        logger.info("Statistics: {}", batch.getStatistics());
        logger.info("Task 1 state: {}", batch.getTask("task1").getState());
        logger.info("Task 2 state: {}", batch.getTask("task2").getState());
        logger.info("Task 3 state: {} (cancelled due to task2 failure)", batch.getTask("task3").getState());
        logger.info("Task 4 state: {}", batch.getTask("task4").getState());

        logger.info("\nFailed tasks:");
        for (DependentTask<?> failedTask : result.getFailedTasks()) {
            logger.error("  - {} failed: {}", failedTask.getTaskId(), failedTask.getError().getMessage());
        }

        executor.shutdown();
        logger.info("");
    }

    private static void runContinueOnErrorExample() throws Exception {
        logger.info("--- Example 2: Continue on Error ---\n");

        DependentExecutorService executor = new DependentExecutorService(4, false);

        // Task 1: Fails
        DependentTask<String> task1 = new DependentTask.Builder<String>("taskA")
                .callable(() -> {
                    logger.info("Task A: Running...");
                    Thread.sleep(200);
                    throw new RuntimeException("Error in Task A");
                })
                .build();

        // Task 2: Succeeds
        DependentTask<String> task2 = new DependentTask.Builder<String>("taskB")
                .callable(() -> {
                    logger.info("Task B: Running...");
                    Thread.sleep(300);
                    logger.info("Task B: Success");
                    return "Result B";
                })
                .build();

        // Task 3: Depends on task1 (will not run due to dependency failure)
        DependentTask<String> task3 = new DependentTask.Builder<String>("taskC")
                .callable(() -> {
                    logger.info("Task C: Running...");
                    return "Result C";
                })
                .dependsOn("taskA")
                .build();

        List<DependentTask<?>> tasks = List.of(task1, task2, task3);
        TaskBatch batch = new TaskBatch("continue-on-error-batch", tasks);

        DependentExecutorService.BatchResult result = executor.submitBatch(batch).get();

        logger.info("\n--- Results ---");
        logger.info("Batch success: {} (some tasks succeeded)", result.isSuccess());
        logger.info("Statistics: {}", batch.getStatistics());
        logger.info("Task A state: {}", batch.getTask("taskA").getState());
        logger.info("Task B state: {} (ran despite Task A failure)", batch.getTask("taskB").getState());
        logger.info("Task C state: {} (couldn't run - dependency failed)", batch.getTask("taskC").getState());

        executor.shutdown();
        logger.info("\n=== Example Complete ===");
    }
}

