import model.DependentTask;
import model.TaskContext;
import service.GraphExecutorService;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class SimpleChainTest {
    public static void main(String[] args) throws ExecutionException, InterruptedException, TimeoutException {
            GraphExecutorService executor = new GraphExecutorService(2);

            // Task 1: Produces "Hello"
            DependentTask<String> task1 = new DependentTask<>("t1", Collections.emptySet(),
                    (deps) -> "Hello");

            // Task 2: Receives "Hello", produces "Hello World"
            DependentTask<String> task2 = new DependentTask<>("t2", Set.of("t1"),
                    (deps) -> deps.getResult("t1") + " World");

            // Task 3: Receives "Hello World", produces "Hello World!"
            DependentTask<String> task3 = new DependentTask<>("t3", Set.of("t2"),
                    (deps) -> deps.getResult("t2") + "!");

            CompletableFuture<TaskContext> result = executor.executeBatch(Arrays.asList(task1, task2, task3));
            TaskContext ctx = result.get(5, TimeUnit.SECONDS);

            System.out.println("Test 1 Result: " + ctx.getResult("t3")); // Expected: Hello World!
            executor.shutdown();
    }
}
