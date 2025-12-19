package examples;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Example showing parallel task execution with a final aggregation task.
 *
 * Structure:
 *        Task1 (Fetch Orders)
 *        Task2 (Fetch Inventory)  →→→  Task4 (Aggregate Report)
 *        Task3 (Fetch Customers)
 */
public class ParallelTasksExample {
    private static final Logger logger = LoggerFactory.getLogger(ParallelTasksExample.class);

    public static void main(String[] args) throws Exception {
        logger.info("=== Parallel Tasks Example ===\n");

        DependentExecutorService executor = new DependentExecutorService();

        // Task 1: Fetch orders (runs in parallel)
        DependentTask<List<String>> task1 = new DependentTask.Builder<List<String>>("fetchOrders")
                .callable(() -> {
                    logger.info("Task 1: Fetching orders...");
                    Thread.sleep(800);
                    List<String> orders = List.of("Order-1", "Order-2", "Order-3");
                    logger.info("Task 1: Fetched {} orders", orders.size());
                    return orders;
                })
                .build();

        // Task 2: Fetch inventory (runs in parallel)
        DependentTask<Integer> task2 = new DependentTask.Builder<Integer>("fetchInventory")
                .callable(() -> {
                    logger.info("Task 2: Fetching inventory count...");
                    Thread.sleep(600);
                    int count = 150;
                    logger.info("Task 2: Inventory count: {}", count);
                    return count;
                })
                .build();

        // Task 3: Fetch customers (runs in parallel)
        DependentTask<Integer> task3 = new DependentTask.Builder<Integer>("fetchCustomers")
                .callable(() -> {
                    logger.info("Task 3: Fetching customer count...");
                    Thread.sleep(700);
                    int count = 42;
                    logger.info("Task 3: Customer count: {}", count);
                    return count;
                })
                .build();

        // Task 4: Aggregate report (waits for all 3 tasks)
        DependentTask<String> task4 = new DependentTask.Builder<String>("aggregateReport")
                .callable(() -> {
                    logger.info("Task 4: Aggregating report...");
                    Thread.sleep(300);
                    // In real scenario, would use results from task1, task2, task3
                    String report = "Report{orders=3, inventory=150, customers=42}";
                    logger.info("Task 4: Report generated: {}", report);
                    return report;
                })
                .dependsOn("fetchOrders", "fetchInventory", "fetchCustomers")
                .priority(DependentTask.TaskPriority.HIGH)
                .build();

        // Create and submit batch
        List<DependentTask<?>> tasks = List.of(task1, task2, task3, task4);
        TaskBatch batch = new TaskBatch("parallel-batch", tasks);

        logger.info("Submitting batch... (Tasks 1-3 will run in parallel)\n");
        long startTime = System.currentTimeMillis();

        DependentExecutorService.BatchResult result = executor.submitBatch(batch).get();

        long endTime = System.currentTimeMillis();

        // Print results
        logger.info("\n=== Results ===");
        logger.info("Batch success: {}", result.isSuccess());
        logger.info("Total time: {}ms (parallel execution saved time!)", (endTime - startTime));
        logger.info("Statistics: {}", batch.getStatistics());
        logger.info("Report: {}", result.getResult("aggregateReport", String.class));

        executor.shutdown();
        logger.info("\n=== Example Complete ===");
    }
}

