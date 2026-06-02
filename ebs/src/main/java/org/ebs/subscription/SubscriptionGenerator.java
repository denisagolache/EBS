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

        // ✅ ELIMINARE SUBSCRIPTII GOALE
        eliminateEmptySubscriptions(result, plans);

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

        // ✅ ELIMINARE SUBSCRIPTII GOALE
        eliminateEmptySubscriptions(result, plans);

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

    // ============================================
    // RESHUFFLING - ELIMINARE SUBSCRIPTII GOALE
    // ============================================

    /**
     * Elimina subscriptiile goale prin reshuffling de campuri
     * intre subscriptii cu 2+ campuri
     */
    private void eliminateEmptySubscriptions(List<Subscription> subscriptions,
                                             List<FieldPlan> plans) {
        boolean hasEmpty = true;
        int iterations = 0;
        final int MAX_ITERATIONS = subscriptions.size(); // pentru siguranta

        while (hasEmpty && iterations < MAX_ITERATIONS) {
            hasEmpty = false;
            iterations++;

            // Gaseste subscriptii goale
            for (int i = 0; i < subscriptions.size(); i++) {
                if (subscriptions.get(i).getFields().isEmpty()) {
                    hasEmpty = true;

                    // Gaseste o subscriptie donor cu >= 2 campuri
                    int donor = findDonorSubscription(subscriptions);
                    if (donor != -1) {
                        // Schimba un camp din donor in subscriptia goala
                        swapFieldPresence(plans, donor, i);

                        // Regenereaza subscriptiile afectate
                        regenerateSubscription(subscriptions, plans, donor);
                        regenerateSubscription(subscriptions, plans, i);
                    } else {
                        // Cazul rar: nu gasim donor, iesim
                        hasEmpty = false;
                        break;
                    }
                }
            }
        }
    }

    /**
     * Gaseste o subscriptie cu >= 2 campuri (donor)
     */
    private int findDonorSubscription(List<Subscription> subscriptions) {
        for (int i = 0; i < subscriptions.size(); i++) {
            if (subscriptions.get(i).getFields().size() >= 2) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Schimba prezenta unui camp intre doua subscriptii
     * din: presence[p][donor] = true  -> false
     * in:  presence[p][receiver] = false -> true
     */
    private void swapFieldPresence(List<FieldPlan> plans, int donor, int receiver) {
        for (FieldPlan plan : plans) {
            if (plan.isPresent(donor) && !plan.isPresent(receiver)) {
                // Gaseste un camp prezent la donor si absent la receiver
                plan.setPresence(donor, false);
                plan.setPresence(receiver, true);
                return;
            }
        }
    }

    /**
     * Regenereaza campurile unei subscriptii pe baza planului curent
     */
    private void regenerateSubscription(List<Subscription> subscriptions,
                                        List<FieldPlan> plans,
                                        int subscriptionIndex) {
        Subscription sub = subscriptions.get(subscriptionIndex);
        sub.getFields().clear();

        for (int p = 0; p < plans.size(); p++) {
            FieldPlan plan = plans.get(p);
            if (plan.isPresent(subscriptionIndex)) {
                // Conteaza de cate ori apare campul inainte de aceasta subscriptie
                int occurrenceIndex = countOccurrencesBefore(plan, subscriptionIndex);
                String op = plan.getOperator(occurrenceIndex);
                Object val = plan.getFieldConfig().generateValue();
                sub.addField(new SubscriptionField(
                        plan.getFieldConfig().getName(), op, val));
            }
        }
    }

    /**
     * Numara de cate ori apare campul inainte de subscriptia data
     */
    private int countOccurrencesBefore(FieldPlan plan, int subscriptionIndex) {
        int count = 0;
        for (int i = 0; i < subscriptionIndex; i++) {
            if (plan.isPresent(i)) count++;
        }
        return count;
    }
}
