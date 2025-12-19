import model.DependentTask;
import service.GraphExecutorService;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

public class CycleTest {
    public static void main(String[] args) {
        GraphExecutorService executorService = new GraphExecutorService(2);

        // Task A depends on Task B
        DependentTask<String> taskA = new DependentTask<>("taskA", new HashSet<>(Collections.singletonList("taskB")), context -> "A");

        // Task B depends on Task A
        DependentTask<String> taskB = new DependentTask<>("taskB", new HashSet<>(Collections.singletonList("taskA")), context -> "B");

        try {
            System.out.println("Submitting cyclic batch...");
            executorService.executeBatch(Arrays.asList(taskA, taskB));
            System.out.println("FAILED: Should have thrown exception");
        } catch (IllegalArgumentException e) {
            System.out.println("SUCCESS: Caught expected exception: " + e.getMessage());
        } catch (Exception e) {
            System.out.println("FAILED: Caught unexpected exception: " + e);
            e.printStackTrace();
        } finally {
            executorService.shutdown();
        }
    }
}


