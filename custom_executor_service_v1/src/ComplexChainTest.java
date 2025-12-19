import model.DependentTask;
import model.TaskContext;
import service.GraphExecutorService;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.ExecutionException;

public class ComplexChainTest {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        GraphExecutorService executor = new GraphExecutorService(4);

        // Root Task
        DependentTask<Integer> startNode = new DependentTask<>("start", null, ctx -> 100);

        // Two parallel subtasks depending on "start"
        DependentTask<Integer> subTaskB = new DependentTask<>("B", Collections.singleton("start"),
                ctx -> (Integer)ctx.getResult("start") / 2); // 50

        DependentTask<Integer> subTaskC = new DependentTask<>("C", Collections.singleton("start"),
                ctx -> (Integer)ctx.getResult("start") + 50); // 150

        // Final task depending on both B and C
        DependentTask<Integer> finalTask = new DependentTask<>("final", new HashSet<>(Arrays.asList("B", "C")),
                ctx -> (Integer)ctx.getResult("B") + (Integer)ctx.getResult("C")); // 200

        TaskContext ctx = executor.executeBatch(Arrays.asList(startNode, subTaskB, subTaskC, finalTask)).get();

        System.out.println("Test 3 Diamond Result: " + ctx.getResult("final")); // Expected: 200
        executor.shutdown();
    }
}
