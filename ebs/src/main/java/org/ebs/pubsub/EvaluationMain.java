package org.ebs.pubsub;

import org.ebs.publication.FieldConfig;
import org.ebs.pubsub.broker.BrokerNode;
import org.ebs.pubsub.config.SystemConfig;
import org.ebs.pubsub.msg.KafkaBus;
import org.ebs.pubsub.msg.MessageBus;
import org.ebs.pubsub.publisher.PublisherNode;
import org.ebs.pubsub.subscriber.SubscriberNode;

import java.util.*;
import java.util.concurrent.*;

public class EvaluationMain {

    static final int BROKER_COUNT = SystemConfig.BROKER_COUNT;
    static final int SUBSCRIBER_COUNT = SystemConfig.SUBSCRIBER_COUNT;
    static final int PUBLISHER_COUNT = SystemConfig.PUBLISHER_COUNT;
    static final int SUBSCRIPTION_COUNT = SystemConfig.SUBSCRIPTION_COUNT;
    static final long FEED_DURATION_MS = SystemConfig.FEED_DURATION_MS;
    static final int PUBS_PER_SECOND = SystemConfig.PUBS_PER_SECOND;
    static final List<FieldConfig> FIELD_CONFIGS = SystemConfig.buildFieldConfigs();

    static class ScenarioResult {
        final String label;
        final long published;
        final long delivered;
        final double matchingRate;
        final double throughput;
        final double avgLatency;

        ScenarioResult(String label, long published, long delivered,
                       double matchingRate, double throughput, double avgLatency) {
            this.label = label;
            this.published = published;
            this.delivered = delivered;
            this.matchingRate = matchingRate;
            this.throughput = throughput;
            this.avgLatency = avgLatency;
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("=".repeat(80));
        System.out.println("  SISTEM PUB-SUB CU RUTARE AVANSATA PE BROKERI");
        System.out.println("  Arhitectura: Apache Kafka (livrare) + Java (filtrare, stocare, rutare)");
        System.out.println("=".repeat(80));
        System.out.printf("  Brokeri: %d | Publisheri: %d | Subscriberi: %d%n",
                BROKER_COUNT, PUBLISHER_COUNT, SUBSCRIBER_COUNT);
        System.out.printf("  Subscriptii totale: %,d | Rata publicare: %d/sec%n",
                SUBSCRIPTION_COUNT, PUBS_PER_SECOND);
        System.out.println("=".repeat(80));

        // Scenariile A si B sunt dezactivate
        // ScenarioResult rA = runAndPrint("A (100% eq, all fields)", SystemConfig.FIELD_FREQ,
        //         SystemConfig.eqFreqFull(), FEED_DURATION_MS);
        // ScenarioResult rB = runAndPrint("B (25% eq, all fields)", SystemConfig.FIELD_FREQ,
        //         SystemConfig.eqFreqQuarter(), FEED_DURATION_MS);

        System.out.println("\n" + "#".repeat(72));
        System.out.println("  TEST IZOLAT: DOAR CAMPUL 'value' IN SUBSCRIPTII");
        System.out.println("  (Masuram impactul pur al operatorului asupra matching-ului)");
        System.out.println("  Durata feed: 30 sec (suficient pentru calcul matching rate)");
        System.out.println("#".repeat(72));

        System.out.println("\n>>> C: eqFreq = 100% pe 'value', doar campul value <<<");
        ScenarioResult rC = runAndPrint("C (100% eq, value-only)", SystemConfig.FIELD_FREQ_VALUE_ONLY,
                SystemConfig.eqFreqFull(), 30_000);

        System.out.println("\n>>> D: eqFreq = 25% pe 'value', doar campul value <<<");
        ScenarioResult rD = runAndPrint("D (25% eq, value-only)", SystemConfig.FIELD_FREQ_VALUE_ONLY,
                SystemConfig.eqFreqQuarter(), 30_000);

        printReport(rC, rD);
    }

    static void printReport(ScenarioResult c, ScenarioResult d) {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("  RAPORT DE EVALUARE");
        System.out.println("=".repeat(80));
        System.out.println();
        System.out.println("  SISTEM:    Retea overlay cu 3 brokeri, 2 publisheri, 3 subscriberi");
        System.out.println("  ARHITECTURA:");
        System.out.println("    - Apache Kafka pentru livrarea mesajelor (MessageBus)");
        System.out.println("    - Filtrare continut, stocare subscriptii, rutare in Java pur");
        System.out.println("    - Rutare: subscriptii distribuite prin consistent hashing");
        System.out.println("    - Publicatiile traverseaza toti brokerii in lant (0->1->2->livrare)");
        System.out.println("    - Infrastructura Kafka in Docker (Confluent 7.5.0)");
        System.out.println("    - Serializare binara cu Protocol Buffers (protobuf)");
        System.out.println();
        System.out.println("  REZULTATE (valori reale din ultima executie):");
        System.out.println("  ----------------------------------------------------------------------");
        System.out.printf("  %-30s %18s %18s%n", "Metric", "C (eq=100%)", "D (eq=25%)");
        System.out.println("  ----------------------------------------------------------------------");
        System.out.printf("  %-30s %18d %18d%n",   "Publicatii emise",   c.published,    d.published);
        System.out.printf("  %-30s %18d %18d%n",   "Livrari totale",     c.delivered,    d.delivered);
        System.out.printf("  %-30s %17.2f%% %17.2f%%%n", "Rata matching", c.matchingRate, d.matchingRate);
        System.out.printf("  %-30s %18.2f %18.2f%n", "Debit (msg/sec)",  c.throughput,   d.throughput);
        System.out.printf("  %-30s %15.3f ms %15.3f ms%n", "Latenta medie", c.avgLatency, d.avgLatency);
        System.out.println("  ----------------------------------------------------------------------");

        System.out.println();
        System.out.println("  ANALIZA:");
        System.out.println();
        System.out.printf("  (a) Publicatii livrate cu succes (interval 30 sec):%n");
        System.out.printf("      Scenariu C: %,d publicatii emise -> %,d livrari%n",
                c.published, c.delivered);
        System.out.printf("      Scenariu D: %,d publicatii emise -> %,d livrari%n",
                d.published, d.delivered);
        System.out.println();
        System.out.printf("  (b) Latenta medie de livrare:%n");
        System.out.printf("      Scenariu C (100%% egalitate pe 'value'): %.3f ms%n", c.avgLatency);
        System.out.printf("      Scenariu D (25%%  egalitate pe 'value'): %.3f ms%n", d.avgLatency);
        System.out.println("      --");
        System.out.println("      Latentele sunt de ordinul zecilor de ms, rezonabil pentru");
        System.out.println("      o infrastructura Kafka locala in Docker. Factorii principali");
        System.out.println("      sunt serializarea protobuf si overhead-ul de retea Kafka.");
        System.out.println();
        System.out.printf("  (c) Rata de potrivire (matching):%n");
        System.out.printf("      C - 100%% operator de egalitate pe campul 'value':%n");
        System.out.printf("        Rata matching = %.2f%%%n", c.matchingRate);
        System.out.println("        Valoarea exacta (Double) trebuie sa coincida intr-un spatiu");
        System.out.println("        de ~99.000 valori posibile -> potriviri rare.");
        System.out.println();
        System.out.printf("      D - 25%% operator de egalitate / 75%% alti operatori pe 'value':%n");
        System.out.printf("        Rata matching = %.2f%%%n", d.matchingRate);
        System.out.println("        Operatorii !=, <, <=, >, >= acopera intervale largi");
        System.out.println("        -> aproape orice valoare publicata este potrivita.");
        System.out.println();
        System.out.println("  CONCLUZII:");
        System.out.println("  * Operatorul de egalitate (100%) reduce drastic matching-ul comparativ");
        System.out.println("    cu distributia mixta (25%), demonstrand importanta selectarii operatorilor");
        System.out.println("  * Sistemul functioneaza corect cu distributia subscriptiilor pe brokeri");
        System.out.println("    si rutarea in lant a publicatiilor prin toti brokerii");
        System.out.println("  * Arhitectura separa clar Kafka (doar livrare) de logica de business");
        System.out.println("  * Protocol Buffers ofera serializare binara eficienta pentru mesaje eterogene");
        System.out.println();
    }

    static ScenarioResult runAndPrint(String label, Map<String, Double> fieldFreq,
                                      Map<String, Double> eqFreq, long durationMs) throws Exception {
        MessageBus bus = new KafkaBus(SystemConfig.KAFKA_BOOTSTRAP_SERVERS, BROKER_COUNT);

        List<BrokerNode> brokers = new ArrayList<>();
        ExecutorService brokerPool = Executors.newFixedThreadPool(BROKER_COUNT);
        for (int i = 0; i < BROKER_COUNT; i++) {
            BrokerNode broker = new BrokerNode(i, BROKER_COUNT, bus);
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

        int perSub = SUBSCRIPTION_COUNT / SUBSCRIBER_COUNT;
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
        for (SubscriberNode s : subscribers) s.stop();
        for (BrokerNode b : brokers) b.stop();
        subPool.shutdownNow();
        brokerPool.shutdownNow();
        bus.stop();

        Thread.sleep(500);

        long totalPublished = 0;
        for (PublisherNode p : publishers) totalPublished += p.getPublishedCount();
        long totalDelivered = 0;
        long totalLatency = 0;
        long[] subDeliveries = new long[SUBSCRIBER_COUNT];
        double[] subLatency = new double[SUBSCRIBER_COUNT];
        for (int i = 0; i < SUBSCRIBER_COUNT; i++) {
            subDeliveries[i] = subscribers.get(i).getReceivedCount();
            totalDelivered += subDeliveries[i];
            totalLatency += subscribers.get(i).getTotalLatency();
            subLatency[i] = subDeliveries[i] > 0
                    ? (double) subscribers.get(i).getTotalLatency() / subDeliveries[i] : 0;
        }

        double avgLatency = totalDelivered > 0 ? (double) totalLatency / totalDelivered : 0;
        double deliveriesPerPub = totalPublished > 0
                ? (double) totalDelivered / totalPublished : 0;
        double matchingRate = totalPublished > 0 && SUBSCRIBER_COUNT > 0
                ? (double) totalDelivered / (totalPublished * SUBSCRIBER_COUNT) * 100 : 0;
        double deliveredPerSec = feedTime > 0
                ? (double) totalDelivered / (feedTime / 1000.0) : 0;

        System.out.println();
        System.out.println("  " + label);
        System.out.println("  " + "-".repeat(55));
        System.out.printf("  %-30s %,12d%n",     "Publicatii emise:",       totalPublished);
        System.out.printf("  %-30s %,12d%n",     "Livrari totale:",         totalDelivered);
        System.out.printf("  %-30s %,12.2f%n",   "Livrari / publicatie:",   deliveriesPerPub);
        System.out.printf("  %-30s %12.2f%%%n",  "Rata de potrivire:",      matchingRate);
        System.out.printf("  %-30s %,12.2f%n",   "Debit livrari (msg/sec):", deliveredPerSec);
        System.out.printf("  %-30s %12.3f ms%n", "Latenta medie:",          avgLatency);
        if (SUBSCRIBER_COUNT <= 3) {
            for (int i = 0; i < SUBSCRIBER_COUNT; i++) {
                System.out.printf("  >> sub-%d: primite %,d, latenta %.3f ms%n",
                        i, subDeliveries[i], subLatency[i]);
            }
        }
        System.out.println();

        return new ScenarioResult(label, totalPublished, totalDelivered,
                matchingRate, deliveredPerSec, avgLatency);
    }
}