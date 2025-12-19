package examples;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Simple example showing a chain of dependent tasks: Task1 → Task2 → Task3
 * Each task depends on the result of the previous one.
 */
public class SimpleChainExample {
    private static final Logger logger = LoggerFactory.getLogger(SimpleChainExample.class);

    public static void main(String[] args) throws Exception {
        logger.info("=== Simple Chain Example: Task1 → Task2 → Task3 ===\n");

        // Create executor service
        DependentExecutorService executor = new DependentExecutorService();

        // Task 1: Fetch user ID
        DependentTask<Integer> task1 = new DependentTask.Builder<Integer>("fetchUserId")
                .callable(() -> {
                    logger.info("Task 1: Fetching user ID...");
                    Thread.sleep(500);
                    int userId = 12345;
                    logger.info("Task 1: Got user ID: {}", userId);
                    return userId;
                })
                .build();

        // Task 2: Fetch user details (depends on Task 1)
        DependentTask<String> task2 = new DependentTask.Builder<String>("fetchUserDetails")
                .callable(() -> {
                    logger.info("Task 2: Fetching user details...");
                    Thread.sleep(500);
                    // In real scenario, would use result from task1
                    String userDetails = "User{id=12345, name=John Doe, email=john@example.com}";
                    logger.info("Task 2: Got user details: {}", userDetails);
                    return userDetails;
                })
                .dependsOn("fetchUserId")
                .build();

        // Task 3: Send notification (depends on Task 2)
        DependentTask<Boolean> task3 = new DependentTask.Builder<Boolean>("sendNotification")
                .callable(() -> {
                    logger.info("Task 3: Sending notification...");
                    Thread.sleep(300);
                    logger.info("Task 3: Notification sent successfully");
                    return true;
                })
                .dependsOn("fetchUserDetails")
                .build();

        // Create batch
        List<DependentTask<?>> tasks = List.of(task1, task2, task3);
        TaskBatch batch = new TaskBatch("simple-chain-batch", tasks);

        // Submit and wait
        logger.info("Submitting batch...\n");
        DependentExecutorService.BatchResult result = executor.submitBatch(batch).get();

        // Print results
        logger.info("\n=== Results ===");
        logger.info("Batch success: {}", result.isSuccess());
        logger.info("Statistics: {}", batch.getStatistics());
        logger.info("User ID: {}", result.getResult("fetchUserId", Integer.class));
        logger.info("User Details: {}", result.getResult("fetchUserDetails", String.class));
        logger.info("Notification sent: {}", result.getResult("sendNotification", Boolean.class));

        executor.shutdown();
        logger.info("\n=== Example Complete ===");
    }
}

