package org.ebs.publication;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class PublicationGenerator {

    private final List<FieldConfig> fieldConfigs;

    public PublicationGenerator(List<FieldConfig> fieldConfigs) {
        this.fieldConfigs = fieldConfigs;
    }

    public List<Publication> generate(int count) {
        List<Publication> result = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            result.add(buildOne());
        }
        return result;
    }

    public List<Publication> generateParallel(int count, int numThreads)
            throws InterruptedException, ExecutionException {

        // Pre-aloca lista (set() la indici specifici, fara add concurent)
        List<Publication> result = new ArrayList<>(count);
        for (int i = 0; i < count; i++) result.add(null);

        ExecutorService pool = Executors.newFixedThreadPool(numThreads);
        List<Future<?>> futures = new ArrayList<>();

        int[] batchSizes = splitWork(count, numThreads);
        int start = 0;

        for (int t = 0; t < numThreads; t++) {
            int from = start;
            int to   = start + batchSizes[t];
            // Intervalele [from,to) sunt disjuncte => fara race condition
            futures.add(pool.submit(() -> {
                for (int i = from; i < to; i++) {
                    result.set(i, buildOne());
                }
            }));
            start += batchSizes[t];
        }

        for (Future<?> f : futures) f.get(); // asteapta toate threadurile
        pool.shutdown();

        return result;
    }

    private Publication buildOne() {
        Publication pub = new Publication();
        for (FieldConfig cfg : fieldConfigs) {
            pub.addField(new PublicationField(cfg.getName(), cfg.generateValue()));
        }
        return pub;
    }

    public static int[] splitWork(int count, int numThreads) {
        int[] sizes = new int[numThreads];
        int base = count / numThreads;
        int rem  = count % numThreads;
        for (int i = 0; i < numThreads; i++) {
            sizes[i] = base + (i < rem ? 1 : 0);
        }
        return sizes;
    }
}