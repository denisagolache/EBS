package org.ebs.pubsub.publisher;

import org.ebs.publication.FieldConfig;
import org.ebs.publication.Publication;
import org.ebs.publication.PublicationGenerator;
import org.ebs.pubsub.msg.MessageBus;
import org.ebs.pubsub.model.PubMessage;
import org.ebs.pubsub.util.Hashing;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class PublisherNode implements Runnable {

    private final String publisherId;
    private final PublicationGenerator generator;
    private final MessageBus bus;
    private final int totalBrokers;
    private final int pubsPerSecond;
    private final AtomicLong pubCounter = new AtomicLong(0);
    private volatile boolean running = true;

    public PublisherNode(String publisherId, List<FieldConfig> configs,
                         MessageBus bus, int totalBrokers, int pubsPerSecond) {
        this.publisherId = publisherId;
        this.generator = new PublicationGenerator(configs);
        this.bus = bus;
        this.totalBrokers = totalBrokers;
        this.pubsPerSecond = pubsPerSecond;
    }

    @Override
    public void run() {
        long intervalMs = 1000 / pubsPerSecond;
        while (running) {
            Publication pub = generator.generate(1).get(0);
            long id = pubCounter.incrementAndGet();
            String pubId = publisherId + "-" + id;
            PubMessage msg = new PubMessage(pubId, pub);

            int entryBroker = Hashing.distribute(pubId, totalBrokers);
            bus.publishToBroker(entryBroker, msg);

            try { Thread.sleep(Math.max(1, intervalMs)); }
            catch (InterruptedException e) { break; }
        }
    }

    public long getPublishedCount() { return pubCounter.get(); }
    public void stop() { running = false; }
}
