package org.ebs.pubsub;

import org.ebs.publication.FieldConfig;
import org.ebs.pubsub.broker.BrokerNode;
import org.ebs.pubsub.broker.SubscriptionDatabase;
import org.ebs.pubsub.config.SystemConfig;
import org.ebs.pubsub.msg.KafkaBus;
import org.ebs.pubsub.msg.MessageBus;
import org.ebs.pubsub.publisher.PublisherNode;
import org.ebs.pubsub.subscriber.SubscriberNode;

import java.util.*;
import java.util.concurrent.*;

public class EvaluationMain {

    static final int BROKER_COUNT      = SystemConfig.BROKER_COUNT;
    static final int SUBSCRIBER_COUNT  = SystemConfig.SUBSCRIBER_COUNT;
    static final int PUBLISHER_COUNT   = SystemConfig.PUBLISHER_COUNT;
    static final int SUBSCRIPTION_COUNT = SystemConfig.SUBSCRIPTION_COUNT;
    static final int PUBS_PER_SECOND   = SystemConfig.PUBS_PER_SECOND;
    static final List<FieldConfig> FIELD_CONFIGS = SystemConfig.buildFieldConfigs();

    static class ScenarioResult {
        final String label;
        final long   published;
        final long   delivered;
        final long   subEvaluations;   // evaluari individuale subscriptie x publicatie
        final long   subMatches;       // din care au dat match
        final double matchingRate;     // subMatches / subEvaluations * 100
        final double throughput;
        final double avgLatency;
        final long   durationMs;

        ScenarioResult(String label, long published, long delivered,
                       long subEvaluations, long subMatches, double matchingRate,
                       double throughput, double avgLatency, long durationMs) {
            this.label          = label;
            this.published      = published;
            this.delivered      = delivered;
            this.subEvaluations = subEvaluations;
            this.subMatches     = subMatches;
            this.matchingRate   = matchingRate;
            this.throughput     = throughput;
            this.avgLatency     = avgLatency;
            this.durationMs     = durationMs;
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println();
        System.out.println("=".repeat(80));
        System.out.println("  SISTEM PUB-SUB CU RUTARE AVANSATA PE BROKERI");
        System.out.println("  Arhitectura: Apache Kafka (livrare) + Java (filtrare, stocare, rutare)");
        System.out.println("  Suport persistenta: PostgreSQL 17 (Docker)");
        System.out.println("=".repeat(80));
        System.out.printf("  Brokeri: %d | Publisheri: %d | Subscriberi: %d%n",
                BROKER_COUNT, PUBLISHER_COUNT, SUBSCRIBER_COUNT);
        System.out.printf("  Subscriptii totale: %,d | Rata publicare: %d/sec%n",
                SUBSCRIPTION_COUNT, PUBS_PER_SECOND);
        System.out.println("=".repeat(80));
        System.out.println();

        System.out.println("#".repeat(72));
        System.out.println("  TEST A: eqFreq = 100% pe 'value', doar campul value");
        System.out.println("  Durata: 3 minute | Subscriptii: 10.000");
        System.out.println("#".repeat(72));
        System.out.println(">>> PORNIRE TEST A <<<");
        ScenarioResult rA = runAndPrint("A (100% eq, 3 min)", SystemConfig.FIELD_FREQ_VALUE_ONLY,
                SystemConfig.eqFreqFull(), SystemConfig.LONG_FEED_DURATION_MS);
        System.out.println("<<< FINAL TEST A >>>");
        System.out.println();

        System.out.println("#".repeat(72));
        System.out.println("  TEST B: eqFreq = 25% pe 'value', doar campul value");
        System.out.println("  Durata: 3 minute | Subscriptii: 10.000");
        System.out.println("#".repeat(72));
        System.out.println(">>> PORNIRE TEST B <<<");
        ScenarioResult rB = runAndPrint("B (25% eq, 3 min)", SystemConfig.FIELD_FREQ_VALUE_ONLY,
                SystemConfig.eqFreqQuarter(), SystemConfig.LONG_FEED_DURATION_MS);
        System.out.println("<<< FINAL TEST B >>>");
        System.out.println();

        System.out.println("#".repeat(72));
        System.out.println("  TEST C: Cadere broker cu recuperare din PostgreSQL 17");
        System.out.println("  Durata: 3 minute | Subscriptii: 10.000");
        System.out.println("#".repeat(72));
        System.out.println(">>> PORNIRE TEST C <<<");
        runCrashScenario("C (crash broker 1, 3 min)", SystemConfig.FIELD_FREQ_VALUE_ONLY,
                SystemConfig.eqFreqFull(), SystemConfig.LONG_FEED_DURATION_MS);
        System.out.println("<<< FINAL TEST C >>>");
        System.out.println();

        printReport(rA, rB);
    }

    // ── Colecteaza evaluarile si match-urile din toti brokerii ────────────────
    private static long[] collectSubStats(List<BrokerNode> brokers) {
        long evals = 0, matches = 0;
        for (BrokerNode b : brokers) {
            evals   += b.getStoreEvaluations();
            matches += b.getStoreMatches();
        }
        return new long[]{ evals, matches };
    }

    static void printReport(ScenarioResult a, ScenarioResult b) {
        System.out.println("=".repeat(72));
        System.out.println("  RAPORT DE EVALUARE — COMPARATIE A (100% eq) vs B (25% eq)");
        System.out.println("=".repeat(72));
        System.out.printf("  %-30s %15s %15s%n",   "Metrica",                 "A (100% eq)", "B (25% eq)");
        System.out.println("  " + "-".repeat(64));
        System.out.printf("  (a) %-26s %,15d %,15d%n",   "Publicatii livrate",           a.delivered,      b.delivered);
        System.out.printf("  (b) %-26s %,15.3f ms %,15.3f ms%n", "Latenta medie",        a.avgLatency,     b.avgLatency);
        System.out.printf("  (c) %-26s %,14.4f%% %,14.4f%%%n",  "Rata de potrivire",     a.matchingRate,   b.matchingRate);
        System.out.println("  " + "-".repeat(64));
        System.out.println();
        System.out.println("  Concluzie: Operatorii de egalitate (100%) au selectivitate foarte scazuta");
        System.out.println("  (~0.001%) pe un camp continuu [10, 1000]. La 25% egalitate + 75% operatori");
        System.out.println("  de comparatie, rata de potrivire creste semnificativ (~37%), deoarece");
        System.out.println("  operatorii <, >, <=, >= acopera intervale mai largi din spatiul valorilor.");
        System.out.println();
    }

    static ScenarioResult runAndPrint(String label, Map<String, Double> fieldFreq,
                                      Map<String, Double> eqFreq, long durationMs) throws Exception {
        MessageBus bus = new KafkaBus(SystemConfig.KAFKA_BOOTSTRAP_SERVERS, BROKER_COUNT);

        SubscriptionDatabase database = new SubscriptionDatabase(
                SystemConfig.POSTGRES_JDBC_URL, SystemConfig.POSTGRES_USER, SystemConfig.POSTGRES_PASSWORD);
        database.clearAll();

        List<BrokerNode> brokers = new ArrayList<>();
        ExecutorService brokerPool = Executors.newFixedThreadPool(BROKER_COUNT);
        for (int i = 0; i < BROKER_COUNT; i++) {
            BrokerNode broker = new BrokerNode(i, BROKER_COUNT, bus, database);
            brokers.add(broker);
            brokerPool.submit(broker);
        }

        List<SubscriberNode> subscribers = new ArrayList<>();
        ExecutorService subPool = Executors.newFixedThreadPool(SUBSCRIBER_COUNT);
        for (int i = 0; i < SUBSCRIBER_COUNT; i++) {
            SubscriberNode sub = new SubscriberNode("sub-" + i, FIELD_CONFIGS, bus, BROKER_COUNT);
            subscribers.add(sub);
            subPool.submit(sub);
        }

        Thread.sleep(500);
        bus.start();
        Thread.sleep(500);

        int perSub    = SUBSCRIPTION_COUNT / SUBSCRIBER_COUNT;
        int remainder = SUBSCRIPTION_COUNT % SUBSCRIBER_COUNT;
        for (int i = 0; i < SUBSCRIBER_COUNT; i++) {
            int count = perSub + (i < remainder ? 1 : 0);
            subscribers.get(i).registerSubscriptions(count, FIELD_CONFIGS, fieldFreq, eqFreq);
        }

        Thread.sleep(500);

        List<PublisherNode> publishers = new ArrayList<>();
        ExecutorService pubPool = Executors.newFixedThreadPool(PUBLISHER_COUNT);
        for (int i = 0; i < PUBLISHER_COUNT; i++) {
            PublisherNode pub = new PublisherNode("pub-" + i, FIELD_CONFIGS,
                    bus, BROKER_COUNT, PUBS_PER_SECOND / PUBLISHER_COUNT);
            publishers.add(pub);
            pubPool.submit(pub);
        }

        long feedStart = System.currentTimeMillis();
        Thread.sleep(durationMs);
        long feedTime = System.currentTimeMillis() - feedStart;

        for (PublisherNode p : publishers) p.stop();
        pubPool.shutdown();
        pubPool.awaitTermination(2, TimeUnit.SECONDS);
        Thread.sleep(2000);

        // ── Statistici livrari ────────────────────────────────────────────────
        long totalPublished = 0;
        for (PublisherNode p : publishers) totalPublished += p.getPublishedCount();

        long totalDelivered = 0, totalLatencyMs = 0;
        for (int i = 0; i < SUBSCRIBER_COUNT; i++) {
            long cnt = subscribers.get(i).getReceivedCount();
            totalDelivered += cnt;
            totalLatencyMs += subscribers.get(i).getTotalLatency();
        }

        // ── Rata de matching per subscriptie individuala ──────────────────────
        long[] subStats         = collectSubStats(brokers);
        long totalSubEvaluations = subStats[0];
        long totalSubMatches     = subStats[1];
        double matchingRate      = totalSubEvaluations > 0
                ? (double) totalSubMatches / totalSubEvaluations * 100 : 0;

        double avgLatency      = totalDelivered > 0 ? (double) totalLatencyMs / totalDelivered : 0;
        double deliveredPerSec = feedTime > 0 ? (double) totalDelivered / (feedTime / 1000.0) : 0;

        // ── Afisare — doar ce cere enuntul (a) livrari, (b) latenta, (c) matching ──
        System.out.println();
        System.out.println("  REZULTATE " + label);
        System.out.println("  " + "-".repeat(55));
        System.out.printf("  (a) %-30s %,12d%n", "Publicatii livrate cu succes:", totalDelivered);
        System.out.printf("  (b) %-30s %12.3f ms%n", "Latenta medie de livrare:",  avgLatency);
        System.out.printf("  (c) %-30s %12.4f%%%n",  "Rata de potrivire:",          matchingRate);
        System.out.println();

        for (SubscriberNode s : subscribers) s.stop();
        for (BrokerNode b : brokers) b.stop();
        subPool.shutdownNow();
        brokerPool.shutdownNow();
        bus.stop();
        Thread.sleep(500);
        database.close();

        return new ScenarioResult(label, totalPublished, totalDelivered,
                totalSubEvaluations, totalSubMatches, matchingRate,
                deliveredPerSec, avgLatency, feedTime);
    }

    static void runCrashScenario(String label, Map<String, Double> fieldFreq,
                                 Map<String, Double> eqFreq, long durationMs) throws Exception {
        MessageBus bus = new KafkaBus(SystemConfig.KAFKA_BOOTSTRAP_SERVERS, BROKER_COUNT);

        SubscriptionDatabase database = new SubscriptionDatabase(
                SystemConfig.POSTGRES_JDBC_URL, SystemConfig.POSTGRES_USER, SystemConfig.POSTGRES_PASSWORD);
        database.clearAll();

        List<BrokerNode> brokers = new ArrayList<>();
        ExecutorService brokerPool = Executors.newFixedThreadPool(BROKER_COUNT);
        for (int i = 0; i < BROKER_COUNT; i++) {
            BrokerNode broker = new BrokerNode(i, BROKER_COUNT, bus, database);
            brokers.add(broker);
            brokerPool.submit(broker);
        }

        List<SubscriberNode> subscribers = new ArrayList<>();
        ExecutorService subPool = Executors.newFixedThreadPool(SUBSCRIBER_COUNT);
        for (int i = 0; i < SUBSCRIBER_COUNT; i++) {
            SubscriberNode sub = new SubscriberNode("sub-" + i, FIELD_CONFIGS, bus, BROKER_COUNT);
            subscribers.add(sub);
            subPool.submit(sub);
        }

        Thread.sleep(500);
        bus.start();
        Thread.sleep(500);

        int perSub    = SUBSCRIPTION_COUNT / SUBSCRIBER_COUNT;
        int remainder = SUBSCRIPTION_COUNT % SUBSCRIBER_COUNT;
        for (int i = 0; i < SUBSCRIBER_COUNT; i++) {
            int count = perSub + (i < remainder ? 1 : 0);
            subscribers.get(i).registerSubscriptions(count, FIELD_CONFIGS, fieldFreq, eqFreq);
        }

        Thread.sleep(500);

        List<PublisherNode> publishers = new ArrayList<>();
        ExecutorService pubPool = Executors.newFixedThreadPool(PUBLISHER_COUNT);
        for (int i = 0; i < PUBLISHER_COUNT; i++) {
            PublisherNode pub = new PublisherNode("pub-" + i, FIELD_CONFIGS,
                    bus, BROKER_COUNT, PUBS_PER_SECOND / PUBLISHER_COUNT);
            publishers.add(pub);
            pubPool.submit(pub);
        }

        long feedStart    = System.currentTimeMillis();
        long crashTime    = durationMs / 4;
        long recoveryTime = durationMs / 2;

        System.out.println("  [Simulare] Faza 1: functionare normala (" + crashTime + " ms)...");
        Thread.sleep(crashTime);

        System.out.println();
        System.out.println("  !!! CADERE BROKER 1 !!!");
        System.out.println("  " + "=".repeat(50));
        int subsBefore = brokers.get(1).getStoredSubscriptionCount();
        System.out.println("  Subscriptii in memorie broker 1 INAINTE de cadere: " + subsBefore);
        brokers.get(1).simulateCrash();
        int subsAfterCrash = brokers.get(1).getStoredSubscriptionCount();
        System.out.println("  Subscriptii in memorie DUPA cadere: " + subsAfterCrash);
        System.out.println("  " + "=".repeat(50));
        System.out.println();

        System.out.println("  [Simulare] Faza 2: functionare cu broker 1 cazut ("
                + (recoveryTime - crashTime) + " ms)...");
        Thread.sleep(recoveryTime - crashTime);

        System.out.println();
        System.out.println("  !!! RECUPERARE BROKER 1 DIN BAZA DE DATE !!!");
        System.out.println("  " + "=".repeat(50));
        brokers.get(1).recover();
        int subsAfterRecovery = brokers.get(1).getStoredSubscriptionCount();
        System.out.println("  Subscriptii in memorie DUPA recuperare: " + subsAfterRecovery);
        System.out.println("  Diferenta: " + (subsAfterRecovery - subsBefore) + " (0 = recuperare perfecta)");
        System.out.println("  " + "=".repeat(50));
        System.out.println();

        System.out.println("  [Simulare] Faza 3: functionare cu broker 1 recuperat ("
                + (durationMs - recoveryTime) + " ms)...");
        Thread.sleep(durationMs - recoveryTime);

        long feedTime = System.currentTimeMillis() - feedStart;

        for (PublisherNode p : publishers) p.stop();
        pubPool.shutdown();
        pubPool.awaitTermination(2, TimeUnit.SECONDS);
        Thread.sleep(3000);

        long totalDelivered = 0, totalLatencyMs = 0;
        for (int i = 0; i < SUBSCRIBER_COUNT; i++) {
            totalDelivered += subscribers.get(i).getReceivedCount();
            totalLatencyMs += subscribers.get(i).getTotalLatency();
        }

        long[] subStats     = collectSubStats(brokers);
        double matchingRate = subStats[0] > 0
                ? (double) subStats[1] / subStats[0] * 100 : 0;
        double avgLatency = totalDelivered > 0 ? (double) totalLatencyMs / totalDelivered : 0;

        System.out.println();
        System.out.println("  REZULTATE " + label);
        System.out.println("  " + "=".repeat(55));
        System.out.printf("  (a) %-30s %,10d%n", "Publicatii livrate cu succes:", totalDelivered);
        System.out.printf("  (b) %-30s %10.3f ms%n", "Latenta medie de livrare:",  avgLatency);
        System.out.printf("  (c) %-30s %10.4f%%%n",  "Rata de potrivire:",          matchingRate);
        System.out.println("  " + "=".repeat(55));
        System.out.println("  STATISTICI PERSISTENTA PostgreSQL 17:");
        System.out.printf("  Subscriptii INAINTE de cadere:  %,6d%n", subsBefore);
        System.out.printf("  Subscriptii DUPA recuperare:    %,6d%n", subsAfterRecovery);
        System.out.printf("  Concluzie: %s%n",
                subsBefore == subsAfterRecovery
                        ? "Recuperare perfecta — toate subscriptiile restaurate corect din PostgreSQL 17."
                        : "Diferenta de " + Math.abs(subsAfterRecovery - subsBefore) + " subscriptii.");
        System.out.println();

        for (SubscriberNode s : subscribers) s.stop();
        for (BrokerNode b : brokers) b.stop();
        subPool.shutdownNow();
        brokerPool.shutdownNow();
        bus.stop();
        Thread.sleep(500);
        database.close();
    }
}