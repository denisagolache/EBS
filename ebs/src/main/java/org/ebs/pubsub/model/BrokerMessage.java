package org.ebs.pubsub.model;

import java.util.HashSet;
import java.util.Set;

public class BrokerMessage {
    private final PubMessage pubMessage;
    private final Set<String> matchedSubscriberIds;
    private int currentBrokerIndex;
    private int targetBrokerId;

    public BrokerMessage(PubMessage pubMessage, int entryBrokerIndex) {
        this.pubMessage = pubMessage;
        this.matchedSubscriberIds = new HashSet<>();
        this.currentBrokerIndex = entryBrokerIndex;
    }

    public PubMessage getPubMessage() { return pubMessage; }
    public Set<String> getMatchedSubscriberIds() { return matchedSubscriberIds; }
    public int getCurrentBrokerIndex() { return currentBrokerIndex; }
    public int getTargetBrokerId() { return targetBrokerId; }

    public void setCurrentBrokerIndex(int index) { this.currentBrokerIndex = index; }
    public void setTargetBrokerId(int id) { this.targetBrokerId = id; }
}
