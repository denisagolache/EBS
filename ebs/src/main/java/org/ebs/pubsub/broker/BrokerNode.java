package org.ebs.pubsub.broker;

import org.ebs.pubsub.msg.MessageBus;
import org.ebs.pubsub.model.*;

import java.sql.SQLException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class BrokerNode implements Runnable {

    private final int brokerId;
    private final int totalBrokers;
    private final SubscriptionStore store = new SubscriptionStore();
    private final MessageBus bus;
    private final SubscriptionDatabase subscriptionDb;
    private final AtomicInteger deliveryCount = new AtomicInteger(0);
    private final AtomicLong totalLatency = new AtomicLong(0);
    private volatile boolean running = true;

    public BrokerNode(int brokerId, int totalBrokers, MessageBus bus, SubscriptionDatabase subscriptionDb) {
        this.brokerId = brokerId;
        this.totalBrokers = totalBrokers;
        this.bus = bus;
        this.subscriptionDb = subscriptionDb;
    }

    @Override
    public void run() {
        try {
            var allSubs = subscriptionDb.loadAll(brokerId);
            for (var entry : allSubs.entrySet()) {
                for (var sub : entry.getValue()) {
                    store.addSubscription(entry.getKey(), sub);
                }
            }
            System.out.println("[Broker " + brokerId + "] Incarcate " + store.getTotalSubscriptions()
                    + " subscriptii din baza de date.");
        } catch (SQLException e) {
            System.err.println("[Broker " + brokerId + "] Eroare incarcare subscriptii: " + e.getMessage());
        }

        bus.onPublishToBroker(brokerId, this::handlePublication);
        bus.onForwardToBroker(brokerId, this::handleBrokerMessage);
        bus.onRegisterSubscription(brokerId, reg -> {
            try {
                subscriptionDb.saveSubscription(brokerId, reg.getSubscriberId(), reg.getSubIndex(), reg.getSubscription());
            } catch (SQLException e) {
                System.err.println("[Broker " + brokerId + "] Eroare salvare subscriptie in DB: " + e.getMessage());
            }
            store.addSubscription(reg.getSubscriberId(), reg.getSubscription());
        });

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

    public void simulateCrash() {
        System.out.println("[Broker " + brokerId + "] SIMULAREA CADERII NODULUI...");
        bus.stopBrokerConsumer(brokerId);
        store.clear();
        System.out.println("[Broker " + brokerId + "] Nod cazut. Consumer oprit, subscriptii sterse din memorie.");
    }

    public void recover() {
        System.out.println("[Broker " + brokerId + "] RECUPERARE NOD...");
        bus.startBrokerConsumer(brokerId);
        try {
            var allSubs = subscriptionDb.loadAll(brokerId);
            for (var entry : allSubs.entrySet()) {
                for (var sub : entry.getValue()) {
                    store.addSubscription(entry.getKey(), sub);
                }
            }
            System.out.println("[Broker " + brokerId + "] Recuperat. Incarcate "
                    + store.getTotalSubscriptions() + " subscriptii din baza de date.");
        } catch (SQLException e) {
            System.err.println("[Broker " + brokerId + "] Eroare recuperare subscriptii: " + e.getMessage());
        }
    }

    public int    getDeliveryCount()              { return deliveryCount.get(); }
    public long   getTotalLatency()               { return totalLatency.get(); }
    public int    getStoredSubscriptionCount()    { return store.getTotalSubscriptions(); }
    // Statistici pentru rata de matching per subscriptie individuala
    public long   getStoreEvaluations()           { return store.getTotalEvaluations(); }
    public long   getStoreMatches()               { return store.getTotalMatches(); }
    public double getPerSubscriptionMatchRate()   { return store.getPerSubscriptionMatchRate(); }
    public void   stop()                          { running = false; }
}