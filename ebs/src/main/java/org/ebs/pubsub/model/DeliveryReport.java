package org.ebs.pubsub.model;

public class DeliveryReport {
    private final String pubId;
    private final String subscriberId;
    private final long deliveryTimestamp;
    private final long latencyMs;

    public DeliveryReport(String pubId, String subscriberId, long deliveryTimestamp, long latencyMs) {
        this.pubId = pubId;
        this.subscriberId = subscriberId;
        this.deliveryTimestamp = deliveryTimestamp;
        this.latencyMs = latencyMs;
    }

    public String getPubId() { return pubId; }
    public String getSubscriberId() { return subscriberId; }
    public long getDeliveryTimestamp() { return deliveryTimestamp; }
    public long getLatencyMs() { return latencyMs; }
}
