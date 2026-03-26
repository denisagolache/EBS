package org.ebs.subscription;

import org.ebs.publication.FieldConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class SubscriptionGenerator {

    private final List<FieldConfig> fieldConfigs;

    public SubscriptionGenerator(List<FieldConfig> fieldConfigs) {
        this.fieldConfigs = fieldConfigs;
    }

    public List<Subscription> generate(int count,
                                       Map<String, Double> fieldFreq,
                                       Map<String, Double> eqFreq) {
        List<FieldPlan>    plans  = buildPlans(count, fieldFreq, eqFreq);
        List<Subscription> result = allocate(count);
        int[] opCounters = new int[plans.size()];
        fillRange(result, plans, 0, count, opCounters);
        return result;
    }

    public List<Subscription> generateParallel(int count,
                                               Map<String, Double> fieldFreq,
                                               Map<String, Double> eqFreq,
                                               int numThreads)
            throws InterruptedException, ExecutionException {

        // FAZA 1 – plan read-only (secvential, rapid)
        List<FieldPlan>    plans  = buildPlans(count, fieldFreq, eqFreq);
        List<Subscription> result = allocate(count);

        // FAZA 2 – generare paralela
        ExecutorService pool = Executors.newFixedThreadPool(numThreads);
        List<Future<?>> futures = new ArrayList<>();

        int[] batchSizes = splitWork(count, numThreads);
        int start = 0;

        for (int t = 0; t < numThreads; t++) {
            int from = start;
            int to   = start + batchSizes[t];

            // Calculeaza offset-urile in operators[] pentru fiecare camp,
            // tinand cont de aparitiile din [0, from).
            // Calculat INAINTE de launch => fara race condition.
            int[] offsets = computeOpOffsets(plans, from);

            futures.add(pool.submit(() -> {
                // Fiecare thread are propria copie a offset-urilor
                int[] localOffsets = offsets.clone();
                fillRange(result, plans, from, to, localOffsets);
            }));

            start += batchSizes[t];
        }

        for (Future<?> f : futures) f.get(); // asteapta toate threadurile
        pool.shutdown();

        return result;
    }

    private List<FieldPlan> buildPlans(int count,
                                       Map<String, Double> fieldFreq,
                                       Map<String, Double> eqFreq) {
        List<FieldPlan> plans = new ArrayList<>();
        for (FieldConfig cfg : fieldConfigs) {
            double freq = fieldFreq.getOrDefault(cfg.getName(), 0.0);
            Double eq   = eqFreq.getOrDefault(cfg.getName(), null);
            plans.add(new FieldPlan(cfg, count, freq, eq));
        }
        return plans;
    }

    private static List<Subscription> allocate(int count) {
        List<Subscription> list = new ArrayList<>(count);
        for (int i = 0; i < count; i++) list.add(new Subscription());
        return list;
    }

    private static int[] computeOpOffsets(List<FieldPlan> plans, int from) {
        int[] offsets = new int[plans.size()];
        for (int p = 0; p < plans.size(); p++) {
            FieldPlan plan = plans.get(p);
            int cnt = 0;
            for (int i = 0; i < from; i++) {
                if (plan.isPresent(i)) cnt++;
            }
            offsets[p] = cnt;
        }
        return offsets;
    }

    private static void fillRange(List<Subscription> result,
                                  List<FieldPlan> plans,
                                  int from, int to,
                                  int[] opCounters) {
        for (int i = from; i < to; i++) {
            Subscription sub = result.get(i);
            for (int p = 0; p < plans.size(); p++) {
                FieldPlan plan = plans.get(p);
                if (plan.isPresent(i)) {
                    String op  = plan.getOperator(opCounters[p]++);
                    Object val = plan.getFieldConfig().generateValue();
                    sub.addField(new SubscriptionField(
                            plan.getFieldConfig().getName(), op, val));
                }
            }
        }
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