package org.ebs.pubsub.subscriber;

import org.ebs.publication.FieldConfig;
import org.ebs.pubsub.msg.MessageBus;
import org.ebs.pubsub.model.DeliveryReport;
import org.ebs.pubsub.model.SubRegistration;
import org.ebs.pubsub.util.Hashing;
import org.ebs.subscription.Subscription;
import org.ebs.subscription.SubscriptionGenerator;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class SubscriberNode implements Runnable {

    private final String subscriberId;
    private final SubscriptionGenerator generator;
    private final MessageBus bus;
    private final int totalBrokers;
    private final BlockingQueue<DeliveryReport> deliveryQueue = new LinkedBlockingQueue<>();
    private final AtomicInteger receivedCount = new AtomicInteger(0);
    private final AtomicLong totalLatency = new AtomicLong(0);
    private volatile boolean running = true;

    public SubscriberNode(String subscriberId, List<FieldConfig> configs,
                          MessageBus bus, int totalBrokers) {
        this.subscriberId = subscriberId;
        this.generator = new SubscriptionGenerator(configs);
        this.bus = bus;
        this.totalBrokers = totalBrokers;
    }

    @Override
    public void run() {
        bus.onDeliverToSubscriber(subscriberId, this::handleDelivery);

        while (running) {
            try {
                DeliveryReport report = deliveryQueue.poll(500, java.util.concurrent.TimeUnit.MILLISECONDS);
                if (report != null) {
                    receivedCount.incrementAndGet();
                    totalLatency.addAndGet(report.getLatencyMs());
                }
            } catch (InterruptedException e) { break; }
        }
    }

    private void handleDelivery(DeliveryReport report) {
        deliveryQueue.offer(report);
    }

    public void registerSubscriptions(int count, List<FieldConfig> fieldConfigs,
                                      Map<String, Double> fieldFreq,
                                      Map<String, Double> eqFreq) {
        List<Subscription> subs = generator.generate(count, fieldFreq, eqFreq);
        for (int i = 0; i < subs.size(); i++) {
            int targetBroker = Hashing.distribute(subscriberId, i, totalBrokers);
            SubRegistration reg = new SubRegistration(subscriberId, i, subs.get(i));
            bus.sendSubRegistrationToBroker(targetBroker, reg);
        }
    }

    public int getReceivedCount() { return receivedCount.get(); }
    public long getTotalLatency() { return totalLatency.get(); }
    public void stop() { running = false; }
}
