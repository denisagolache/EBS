package org.ebs.pubsub.broker;

import org.ebs.pubsub.msg.MessageBus;
import org.ebs.pubsub.model.*;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class BrokerNode implements Runnable {

    private final int brokerId;
    private final int totalBrokers;
    private final SubscriptionStore store = new SubscriptionStore();
    private final MessageBus bus;
    private final AtomicInteger deliveryCount = new AtomicInteger(0);
    private final AtomicLong totalLatency = new AtomicLong(0);
    private volatile boolean running = true;

    public BrokerNode(int brokerId, int totalBrokers, MessageBus bus) {
        this.brokerId = brokerId;
        this.totalBrokers = totalBrokers;
        this.bus = bus;
    }

    @Override
    public void run() {
        bus.onPublishToBroker(brokerId, this::handlePublication);
        bus.onForwardToBroker(brokerId, this::handleBrokerMessage);
        bus.onRegisterSubscription(brokerId, reg ->
                store.addSubscription(reg.getSubscriberId(), reg.getSubscription()));

        while (running) {
            try { Thread.sleep(200); } catch (InterruptedException e) { break; }
        }
    }

    private void handlePublication(PubMessage pubMsg) {
        BrokerMessage bm = new BrokerMessage(pubMsg, 0);
        bm.setTargetBrokerId(0);
        bus.forwardToBroker(bm);
    }

    private void handleBrokerMessage(BrokerMessage bm) {
        Set<String> myMatches = store.match(bm.getPubMessage().getPublication());
        bm.getMatchedSubscriberIds().addAll(myMatches);
        int nextIdx = bm.getCurrentBrokerIndex() + 1;
        if (nextIdx < totalBrokers) {
            bm.setCurrentBrokerIndex(nextIdx);
            bm.setTargetBrokerId(nextIdx);
            bus.forwardToBroker(bm);
        } else {
            deliver(bm);
        }
    }

    private void deliver(BrokerMessage bm) {
        for (String subscriberId : bm.getMatchedSubscriberIds()) {
            long now = System.currentTimeMillis();
            long latency = now - bm.getPubMessage().getTimestamp();
            DeliveryReport report = new DeliveryReport(
                    bm.getPubMessage().getPubId(), subscriberId, now, latency);
            bus.deliverToSubscriber(report);
            deliveryCount.incrementAndGet();
            totalLatency.addAndGet(latency);
        }
    }

    public int getDeliveryCount() { return deliveryCount.get(); }
    public long getTotalLatency() { return totalLatency.get(); }
    public int getStoredSubscriptionCount() { return store.getTotalSubscriptions(); }
    public void stop() { running = false; }
}
