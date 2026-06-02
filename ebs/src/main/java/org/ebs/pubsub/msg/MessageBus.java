package org.ebs.pubsub.msg;

import org.ebs.pubsub.model.BrokerMessage;
import org.ebs.pubsub.model.DeliveryReport;
import org.ebs.pubsub.model.PubMessage;
import org.ebs.pubsub.model.SubRegistration;

import java.util.function.Consumer;

public interface MessageBus {
    void start();
    void stop();

    void publishToBroker(int brokerId, PubMessage msg);
    void forwardToBroker(BrokerMessage msg);
    void deliverToSubscriber(DeliveryReport report);
    void registerSubscription(SubRegistration reg);
    void sendSubRegistrationToBroker(int brokerId, SubRegistration reg);

    void onPublishToBroker(int brokerId, Consumer<PubMessage> handler);
    void onForwardToBroker(int brokerId, Consumer<BrokerMessage> handler);
    void onDeliverToSubscriber(String subscriberId, Consumer<DeliveryReport> handler);
    void onRegisterSubscription(int brokerId, Consumer<SubRegistration> handler);
}
