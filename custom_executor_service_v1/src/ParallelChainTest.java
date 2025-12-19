import model.DependentTask;
import service.GraphExecutorService;

import java.util.Arrays;
import java.util.concurrent.ExecutionException;

public class ParallelChainTest {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        GraphExecutorService executor = new GraphExecutorService(3);
        long startTime = System.currentTimeMillis();

        // Three tasks that each sleep for 1 second
        DependentTask<String> t1 = new DependentTask<>("t1", null, ctx -> {
            try { Thread.sleep(1000); } catch (InterruptedException e) {}
            return "Done1";
        });
        DependentTask<String> t2 = new DependentTask<>("t2", null, ctx -> {
            try { Thread.sleep(1000); } catch (InterruptedException e) {}
            return "Done2";
        });
        DependentTask<String> t3 = new DependentTask<>("t3", null, ctx -> {
            try { Thread.sleep(1000); } catch (InterruptedException e) {}
            return "Done3";
        });

        executor.executeBatch(Arrays.asList(t1, t2, t3)).get();
        long duration = System.currentTimeMillis() - startTime;

        System.out.println("Test 2 Parallel Duration: " + duration + "ms");
        // Success if duration is significantly less than 3000ms
        executor.shutdown();
    }

}
