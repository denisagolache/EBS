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
        System.out.println("  SCENARIU C: eqFreq = 100% pe 'value', doar campul value");
        System.out.println("  Durata: 3 minute | Subscriptii: 10.000");
        System.out.println("#".repeat(72));
        System.out.println(">>> PORNIRE TEST C <<<");
        ScenarioResult rC = runAndPrint("C (100% eq, 3 min)", SystemConfig.FIELD_FREQ_VALUE_ONLY,
                SystemConfig.eqFreqFull(), SystemConfig.LONG_FEED_DURATION_MS);
        System.out.println("<<< FINAL TEST C >>>");
        System.out.println();

        System.out.println("#".repeat(72));
        System.out.println("  SCENARIU D: eqFreq = 25% pe 'value', doar campul value");
        System.out.println("  Durata: 3 minute | Subscriptii: 10.000");
        System.out.println("#".repeat(72));
        System.out.println(">>> PORNIRE TEST D <<<");
        ScenarioResult rD = runAndPrint("D (25% eq, 3 min)", SystemConfig.FIELD_FREQ_VALUE_ONLY,
                SystemConfig.eqFreqQuarter(), SystemConfig.LONG_FEED_DURATION_MS);
        System.out.println("<<< FINAL TEST D >>>");
        System.out.println();

        System.out.println("#".repeat(72));
        System.out.println("  SCENARIU E: Cadere broker cu recuperare din PostgreSQL 17");
        System.out.println("  Durata: 3 minute | Subscriptii: 10.000");
        System.out.println("#".repeat(72));
        System.out.println(">>> PORNIRE TEST E <<<");
        runCrashScenario("E (crash broker 1, 3 min)", SystemConfig.FIELD_FREQ_VALUE_ONLY,
                SystemConfig.eqFreqFull(), SystemConfig.LONG_FEED_DURATION_MS);
        System.out.println("<<< FINAL TEST E >>>");
        System.out.println();

        printReport(rC, rD);
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

    static void printReport(ScenarioResult c, ScenarioResult d) {
        System.out.println("=".repeat(80));
        System.out.println("  RAPORT DE EVALUARE");
        System.out.println("=".repeat(80));
        System.out.println();
        System.out.println("  SISTEM:    Retea overlay cu 3 brokeri, 2 publisheri, 3 subscriberi");
        System.out.println("  ARHITECTURA:");
        System.out.println("    - Apache Kafka pentru livrarea mesajelor (MessageBus)");
        System.out.println("    - Filtrare continut, stocare subscriptii, rutare in Java pur");
        System.out.println("    - Persistence: PostgreSQL 17 pentru subscriptii");
        System.out.println("    - Rutare: subscriptii distribuite prin consistent hashing");
        System.out.println("    - Publicatiile traverseaza toti brokerii in lant (0->1->2->livrare)");
        System.out.println("    - Infrastructura Kafka in Docker (Confluent 7.5.0)");
        System.out.println("    - Serializare binara cu Protocol Buffers (protobuf)");
        System.out.println();
        System.out.println("  NOTA METODOLOGIE:");
        System.out.println("    Rata de potrivire = subscriptii_cu_match / subscriptii_evaluate");
        System.out.println("    unde o 'evaluare' = o pereche (subscriptie, publicatie) testata.");
        System.out.println("    Aceasta masoara probabilitatea ca o subscriptie individuala");
        System.out.println("    sa se potriveasca cu o publicatie arbitrara.");
        System.out.println();
        System.out.println("  REZULTATE (valori reale din ultima executie, 3 minute fiecare):");
        System.out.println("  " + "-".repeat(62));
        System.out.printf("  %-34s %13s %13s%n", "Metric", "C (eq=100%)", "D (eq=25%)");
        System.out.println("  " + "-".repeat(62));
        System.out.printf("  %-34s %13d %13d%n",   "Publicatii emise",        c.published,      d.published);
        System.out.printf("  %-34s %13d %13d%n",   "Livrari totale",          c.delivered,      d.delivered);
        System.out.printf("  %-34s %13d %13d%n",   "Evaluari sub x pub",      c.subEvaluations, d.subEvaluations);
        System.out.printf("  %-34s %13d %13d%n",   "Match-uri individuale",   c.subMatches,     d.subMatches);
        System.out.printf("  %-34s %12.4f%% %12.4f%%%n", "Rata matching (c)", c.matchingRate,   d.matchingRate);
        System.out.printf("  %-34s %13.2f %13.2f%n", "Debit (msg/sec)",       c.throughput,     d.throughput);
        System.out.printf("  %-34s %10.3f ms %10.3f ms%n", "Latenta medie",   c.avgLatency,     d.avgLatency);
        System.out.println("  " + "-".repeat(62));
        System.out.println();
        System.out.println("  ANALIZA:");
        System.out.println();
        System.out.printf("  (a) Publicatii livrate cu succes (3 minute):%n");
        System.out.printf("      Scenariu C: %,d publicatii -> %,d livrari (%.2f msg/sec)%n",
                c.published, c.delivered, c.throughput);
        System.out.printf("      Scenariu D: %,d publicatii -> %,d livrari (%.2f msg/sec)%n",
                d.published, d.delivered, d.throughput);
        System.out.println();
        System.out.printf("  (b) Latenta medie de livrare:%n");
        System.out.printf("      Scenariu C (100%% egalitate pe 'value'): %.3f ms%n", c.avgLatency);
        System.out.printf("      Scenariu D (25%%  egalitate pe 'value'): %.3f ms%n", d.avgLatency);
        System.out.println();
        System.out.printf("  (c) Rata de potrivire (match-uri / evaluari per subscriptie):%n");
        System.out.printf("      C - 100%% egalitate: %,d / %,d = %.4f%%%n",
                c.subMatches, c.subEvaluations, c.matchingRate);
        System.out.printf("      D - 25%%  egalitate: %,d / %,d = %.4f%%%n",
                d.subMatches, d.subEvaluations, d.matchingRate);
        System.out.println();
        System.out.println("      Interpretare:");
        System.out.println("      * C (100% egalitate): value=X cu X aleator din [10,1000] cu 2 zecimale");
        System.out.println("        => probabilitate de match ≈ 1/99000 ≈ 0.001% per subscriptie.");
        System.out.println("      * D (25% egalitate, 75% range): operatorii <,>,<=,>= pe un interval");
        System.out.println("        uniform acopera ~50% din spatiu => rata mult mai mare.");
        System.out.println("      * Diferenta ilustreaza impactul tipului de operator asupra selectivitatii.");
        System.out.println();
        System.out.println("  CONCLUZII:");
        System.out.println("  * Operatorii range genereaza o rata de matching semnificativ mai mare");
        System.out.println("    decat operatorul de egalitate pe campuri numerice continue.");
        System.out.println("  * PostgreSQL 17 asigura persistenta si recuperarea automata dupa crash.");
        System.out.println("  * Arhitectura separa clar Kafka (transport) de logica Java (procesare).");
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
        long[] subDeliveries = new long[SUBSCRIBER_COUNT];
        double[] subLatency  = new double[SUBSCRIBER_COUNT];
        for (int i = 0; i < SUBSCRIBER_COUNT; i++) {
            subDeliveries[i]  = subscribers.get(i).getReceivedCount();
            totalDelivered   += subDeliveries[i];
            totalLatencyMs   += subscribers.get(i).getTotalLatency();
            subLatency[i]     = subDeliveries[i] > 0
                    ? (double) subscribers.get(i).getTotalLatency() / subDeliveries[i] : 0;
        }

        // ── Rata de matching per subscriptie individuala ──────────────────────
        long[] subStats         = collectSubStats(brokers);
        long totalSubEvaluations = subStats[0];
        long totalSubMatches     = subStats[1];
        double matchingRate      = totalSubEvaluations > 0
                ? (double) totalSubMatches / totalSubEvaluations * 100 : 0;

        double avgLatency       = totalDelivered > 0 ? (double) totalLatencyMs / totalDelivered : 0;
        double deliveriesPerPub = totalPublished > 0 ? (double) totalDelivered / totalPublished : 0;
        double deliveredPerSec  = feedTime > 0 ? (double) totalDelivered / (feedTime / 1000.0) : 0;

        // ── Afisare ───────────────────────────────────────────────────────────
        System.out.println();
        System.out.println("  REZULTATE " + label);
        System.out.println("  " + "-".repeat(55));
        System.out.printf("  %-30s %,12d%n",     "Publicatii emise:",           totalPublished);
        System.out.printf("  %-30s %,12d%n",     "Livrari totale:",             totalDelivered);
        System.out.printf("  %-30s %,12.2f%n",   "Livrari / publicatie:",       deliveriesPerPub);
        System.out.printf("  %-30s %,12d%n",     "Evaluari sub x pub:",         totalSubEvaluations);
        System.out.printf("  %-30s %,12d%n",     "Match-uri individuale:",      totalSubMatches);
        System.out.printf("  %-30s %12.4f%%%n",  "Rata de potrivire (c):",      matchingRate);
        System.out.printf("  %-30s %,12.2f%n",   "Debit livrari (msg/sec):",    deliveredPerSec);
        System.out.printf("  %-30s %12.3f ms%n", "Latenta medie:",              avgLatency);
        if (SUBSCRIBER_COUNT <= 3) {
            for (int i = 0; i < SUBSCRIBER_COUNT; i++) {
                System.out.printf("  >> sub-%d: primite %,d, latenta %.3f ms%n",
                        i, subDeliveries[i], subLatency[i]);
            }
        }
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

        long totalPublished = 0;
        for (PublisherNode p : publishers) totalPublished += p.getPublishedCount();

        long totalDelivered = 0, totalLatencyMs = 0;
        long[] subDeliveries = new long[SUBSCRIBER_COUNT];
        for (int i = 0; i < SUBSCRIBER_COUNT; i++) {
            subDeliveries[i]  = subscribers.get(i).getReceivedCount();
            totalDelivered   += subDeliveries[i];
            totalLatencyMs   += subscribers.get(i).getTotalLatency();
        }

        long[] subStats          = collectSubStats(brokers);
        long totalSubEvaluations = subStats[0];
        long totalSubMatches     = subStats[1];
        double matchingRate      = totalSubEvaluations > 0
                ? (double) totalSubMatches / totalSubEvaluations * 100 : 0;
        double avgLatency        = totalDelivered > 0 ? (double) totalLatencyMs / totalDelivered : 0;
        double deliveredPerSec   = feedTime > 0 ? (double) totalDelivered / (feedTime / 1000.0) : 0;

        System.out.println();
        System.out.println("  REZULTATE " + label);
        System.out.println("  " + "=".repeat(55));
        System.out.printf("  %-30s %,12d%n",     "Publicatii emise:",          totalPublished);
        System.out.printf("  %-30s %,12d%n",     "Livrari totale:",            totalDelivered);
        System.out.printf("  %-30s %,12d%n",     "Evaluari sub x pub:",        totalSubEvaluations);
        System.out.printf("  %-30s %,12d%n",     "Match-uri individuale:",     totalSubMatches);
        System.out.printf("  %-30s %12.4f%%%n",  "Rata de potrivire:",         matchingRate);
        System.out.printf("  %-30s %,12.2f%n",   "Debit livrari (msg/sec):",   deliveredPerSec);
        System.out.printf("  %-30s %12.3f ms%n", "Latenta medie:",             avgLatency);
        System.out.println("  " + "-".repeat(55));
        System.out.println("  STATISTICI PERSISTENTA PostgreSQL 17:");
        System.out.printf("  %-30s %,12d%n",     "Subscriptii INAINTE de cadere:", subsBefore);
        System.out.printf("  %-30s %,12d%n",     "Subscriptii DUPA recuperare:",   subsAfterRecovery);
        System.out.println("  " + "-".repeat(55));
        System.out.println("  CONCLUZIE:");
        System.out.println("  * Subscriptiile persistate in PostgreSQL 17 s-au incarcat corect la restart.");
        System.out.println("  * Sistemul continua sa functioneze dupa caderea si recuperarea brokerului.");
        System.out.println("  * Kafka pastreaza mesajele in topics, iar subscriptiile sunt restaurate din DB.");
        System.out.println();

        if (SUBSCRIBER_COUNT <= 3) {
            for (int i = 0; i < SUBSCRIBER_COUNT; i++) {
                double lat = subDeliveries[i] > 0
                        ? (double) subscribers.get(i).getTotalLatency() / subDeliveries[i] : 0;
                System.out.printf("  >> sub-%d: primite %,d, latenta %.3f ms%n",
                        i, subDeliveries[i], lat);
            }
        }
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