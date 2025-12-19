package examples;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Complex example with multiple dependency paths and result passing.
 *
 * Dependency Graph:
 *        Task1 (Auth)
 *       /           \
 *    Task2         Task3
 *  (Get User)   (Get Permissions)
 *       \           /
 *        Task4
 *   (Validate Access)
 *           |
 *        Task5
 *   (Execute Action)
 */
public class ComplexDependencyExample {
    private static final Logger logger = LoggerFactory.getLogger(ComplexDependencyExample.class);

    public static void main(String[] args) throws Exception {
        logger.info("=== Complex Dependency Example ===\n");

        DependentExecutorService executor = new DependentExecutorService();

        // Task 1: Authenticate and get auth token
        DependentTask<String> task1 = new DependentTask.Builder<String>("authenticate")
                .callable(() -> {
                    logger.info("Task 1: Authenticating user...");
                    Thread.sleep(500);
                    String token = "AUTH_TOKEN_XYZ";
                    logger.info("Task 1: Authentication successful, token: {}", token);
                    return token;
                })
                .priority(DependentTask.TaskPriority.HIGH)
                .build();

        // Task 2: Get user profile (depends on auth)
        DependentTask<String> task2 = new DependentTask.Builder<String>("getUserProfile")
                .callable(() -> {
                    logger.info("Task 2: Fetching user profile...");
                    Thread.sleep(400);
                    String profile = "User{id=100, role=ADMIN}";
                    logger.info("Task 2: User profile: {}", profile);
                    return profile;
                })
                .dependsOn("authenticate")
                .build();

        // Task 3: Get user permissions (depends on auth)
        DependentTask<List<String>> task3 = new DependentTask.Builder<List<String>>("getPermissions")
                .callable(() -> {
                    logger.info("Task 3: Fetching permissions...");
                    Thread.sleep(300);
                    List<String> permissions = List.of("READ", "WRITE", "DELETE");
                    logger.info("Task 3: Permissions: {}", permissions);
                    return permissions;
                })
                .dependsOn("authenticate")
                .build();

        // Task 4: Validate access (depends on user profile and permissions)
        DependentTask<Boolean> task4 = new DependentTask.Builder<Boolean>("validateAccess")
                .callable(() -> {
                    logger.info("Task 4: Validating access rights...");
                    Thread.sleep(200);
                    // In real scenario, would check profile and permissions
                    boolean hasAccess = true;
                    logger.info("Task 4: Access validation: {}", hasAccess);
                    return hasAccess;
                })
                .dependsOn("getUserProfile", "getPermissions")
                .build();

        // Task 5: Execute action (depends on validation)
        DependentTask<String> task5 = new DependentTask.Builder<String>("executeAction")
                .callable(() -> {
                    logger.info("Task 5: Executing secured action...");
                    Thread.sleep(500);
                    String result = "Action executed successfully";
                    logger.info("Task 5: {}", result);
                    return result;
                })
                .dependsOn("validateAccess")
                .build();

        // Create and submit batch
        List<DependentTask<?>> tasks = List.of(task1, task2, task3, task4, task5);
        TaskBatch batch = new TaskBatch("complex-dependency-batch", tasks);

        logger.info("Submitting batch with complex dependencies...\n");
        logger.info("Dependency graph:");
        logger.info("  Task1 → Task2 \\");
        logger.info("  Task1 → Task3  → Task4 → Task5");
        logger.info("  (Tasks 2 & 3 run in parallel)\n");

        long startTime = System.currentTimeMillis();
        DependentExecutorService.BatchResult result = executor.submitBatch(batch).get();
        long endTime = System.currentTimeMillis();

        // Print results
        logger.info("\n=== Results ===");
        logger.info("Batch success: {}", result.isSuccess());
        logger.info("Total time: {}ms", (endTime - startTime));
        logger.info("Statistics: {}", batch.getStatistics());

        logger.info("\nTask Results:");
        logger.info("  Auth Token: {}", result.getResult("authenticate", String.class));
        logger.info("  User Profile: {}", result.getResult("getUserProfile", String.class));
        logger.info("  Permissions: {}", result.getResult("getPermissions", List.class));
        logger.info("  Access Valid: {}", result.getResult("validateAccess", Boolean.class));
        logger.info("  Action Result: {}", result.getResult("executeAction", String.class));

        executor.shutdown();
        logger.info("\n=== Example Complete ===");
    }
}

