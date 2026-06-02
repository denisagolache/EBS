package org.ebs.pubsub.model;

import org.ebs.subscription.Subscription;

public class SubRegistration {
    private final String subscriberId;
    private final int subIndex;
    private final Subscription subscription;

    public SubRegistration(String subscriberId, int subIndex, Subscription subscription) {
        this.subscriberId = subscriberId;
        this.subIndex = subIndex;
        this.subscription = subscription;
    }

    public String getSubscriberId() { return subscriberId; }
    public int getSubIndex() { return subIndex; }
    public Subscription getSubscription() { return subscription; }
}
