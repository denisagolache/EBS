package org.ebs.pubsub.msg;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.ebs.proto.EbsProto;
import org.ebs.publication.Publication;
import org.ebs.publication.PublicationField;
import org.ebs.pubsub.model.BrokerMessage;
import org.ebs.pubsub.model.DeliveryReport;
import org.ebs.pubsub.model.PubMessage;
import org.ebs.pubsub.model.SubRegistration;
import org.ebs.subscription.Subscription;
import org.ebs.subscription.SubscriptionField;

import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * MessageBus peste Apache Kafka.
 * Kafka este folosit doar pentru livrarea mesajelor.
 * Serializarea mesajelor se face cu Protocol Buffers (proto3).
 * Filtrarea bazata pe continut, stocarea subscriptiilor si rutarea raman in Java.
 */
public class KafkaBus implements MessageBus {

    private final String bootstrapServers;
    private final int totalBrokers;
    private final String consumerGroupRunId;
    private final String topicPubs;
    private final String topicBrokerFwd;
    private final String topicSubReg;
    private final String topicDeliveryPrefix;

    private KafkaProducer<String, String> producer;
    private final List<KafkaConsumer<String, String>> consumers = new CopyOnWriteArrayList<>();
    private final Map<Integer, KafkaConsumer<String, String>> brokerConsumers = new ConcurrentHashMap<>();
    private final Map<Integer, Future<?>> brokerConsumerFutures = new ConcurrentHashMap<>();
    private final Set<Integer> activeBrokers = ConcurrentHashMap.newKeySet();
    private final ExecutorService consumerPool;
    private volatile boolean running;

    private final List<Consumer<PubMessage>>[] pubHandlers;
    private final List<Consumer<BrokerMessage>>[] brokerFwdHandlers;
    private final List<Consumer<SubRegistration>>[] subRegHandlers;
    private final ConcurrentHashMap<String, List<Consumer<DeliveryReport>>> deliveryHandlers
            = new ConcurrentHashMap<>();

    @SuppressWarnings("unchecked")
    public KafkaBus(String bootstrapServers, int totalBrokers) {
        this.bootstrapServers = bootstrapServers;
        this.totalBrokers = totalBrokers;
        this.consumerGroupRunId = UUID.randomUUID().toString().substring(0, 8);
        String topicRunId = UUID.randomUUID().toString().substring(0, 8);
        this.topicPubs           = "ebs-pubs-"     + topicRunId;
        this.topicBrokerFwd      = "ebs-broker-fwd-" + topicRunId;
        this.topicSubReg         = "ebs-sub-reg-"  + topicRunId;
        this.topicDeliveryPrefix = "ebs-delivery-" + topicRunId + "-";
        this.consumerPool = Executors.newCachedThreadPool();
        this.pubHandlers       = new List[totalBrokers];
        this.brokerFwdHandlers = new List[totalBrokers];
        this.subRegHandlers    = new List[totalBrokers];
        for (int i = 0; i < totalBrokers; i++) {
            pubHandlers[i]       = new CopyOnWriteArrayList<>();
            brokerFwdHandlers[i] = new CopyOnWriteArrayList<>();
            subRegHandlers[i]    = new CopyOnWriteArrayList<>();
        }
    }

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    @Override
    public void start() {
        running = true;
        createTopics();

        producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,      bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.ACKS_CONFIG,                   "all",
                ProducerConfig.RETRIES_CONFIG,                3,
                ProducerConfig.LINGER_MS_CONFIG,              5
        ));

        for (int i = 0; i < totalBrokers; i++) {
            activeBrokers.add(i);
            startBrokerConsumerInternal(i);
        }
    }

    private void createTopics() {
        Exception last = null;
        for (int attempt = 1; attempt <= 20; attempt++) {
            try (AdminClient admin = AdminClient.create(Map.of(
                    "bootstrap.servers", bootstrapServers,
                    "request.timeout.ms", 5000
            ))) {
                Set<String> existing = admin.listTopics().names().get(8, TimeUnit.SECONDS);
                List<NewTopic> newTopics = new ArrayList<>();
                addIfMissing(existing, newTopics, topicPubs,      1);
                addIfMissing(existing, newTopics, topicBrokerFwd, totalBrokers);
                addIfMissing(existing, newTopics, topicSubReg,    1);
                for (int i = 0; i < totalBrokers; i++) {
                    addIfMissing(existing, newTopics, topicDeliveryPrefix + "sub-" + i, 1);
                }
                if (!newTopics.isEmpty()) {
                    admin.createTopics(newTopics).all().get(8, TimeUnit.SECONDS);
                }
                return;
            } catch (Exception e) {
                last = e;
                try {
                    long delay = (long) Math.pow(2, Math.min(attempt, 6)) * 250;
                    Thread.sleep(delay);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        if (last != null) {
            System.err.println("[KafkaBus] Avertisment: nu s-au putut crea topic-uri: " + last);
        }
    }

    private void addIfMissing(Set<String> existing, List<NewTopic> list,
                              String topic, int partitions) {
        if (!existing.contains(topic)) {
            list.add(new NewTopic(topic, Optional.of(partitions), Optional.of((short) 1)));
        }
    }

    private void startBrokerConsumerInternal(int brokerId) {
        if (brokerConsumers.containsKey(brokerId)) {
            return;
        }
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,        bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,                 "broker-" + brokerId + "-" + consumerGroupRunId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,        "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,       "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,  "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,       "10000");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of(
                topicPubs,
                topicBrokerFwd,
                topicSubReg,
                topicDeliveryPrefix + "sub-" + brokerId
        ));
        consumers.add(consumer);
        brokerConsumers.put(brokerId, consumer);

        Future<?> future = consumerPool.submit(() -> {
            try {
                while (running && activeBrokers.contains(brokerId)) {
                    try {
                        var records = consumer.poll(Duration.ofMillis(200));
                        for (ConsumerRecord<String, String> rec : records) {
                            dispatch(rec.topic(), rec.key(), rec.value(), brokerId);
                        }
                    } catch (org.apache.kafka.common.errors.WakeupException e) {
                        if (!running || !activeBrokers.contains(brokerId)) break;
                    } catch (Exception e) {
                        if (running && activeBrokers.contains(brokerId)) {
                            System.err.println("[KafkaBus] Eroare consumer broker-"
                                    + brokerId + ": " + e.getMessage());
                        }
                    }
                }
            } finally {
                try {
                    consumer.close(Duration.ofSeconds(2));
                } catch (Exception ignored) {}
                consumers.remove(consumer);
                brokerConsumers.remove(brokerId);
                System.out.println("[KafkaBus] Consumer broker-" + brokerId + " inchis.");
            }
        });
        brokerConsumerFutures.put(brokerId, future);
    }

    // ── Dispatch ──────────────────────────────────────────────────────────────

    private void dispatch(String topic, String key, String value, int brokerId) {
        try {
            if (topicPubs.equals(topic)) {
                if (!matchesBrokerKey(key, brokerId)) return;
                PubMessage msg = ProtoCodec.decodePub(value);
                pubHandlers[brokerId].forEach(h -> h.accept(msg));
                return;
            }

            if (topicBrokerFwd.equals(topic)) {
                BrokerMessage msg = ProtoCodec.decodeBroker(value);
                if (msg.getTargetBrokerId() != brokerId) return;
                brokerFwdHandlers[brokerId].forEach(h -> h.accept(msg));
                return;
            }

            if (topicSubReg.equals(topic)) {
                if (!matchesBrokerKey(key, brokerId)) return;
                SubRegistration msg = ProtoCodec.decodeSub(value);
                subRegHandlers[brokerId].forEach(h -> h.accept(msg));
                return;
            }

            if (topic.startsWith(topicDeliveryPrefix)) {
                DeliveryReport msg  = ProtoCodec.decodeDelivery(value);
                String subId        = topic.substring(topicDeliveryPrefix.length());
                List<Consumer<DeliveryReport>> handlers = deliveryHandlers.get(subId);
                if (handlers != null) handlers.forEach(h -> h.accept(msg));
            }
        } catch (Exception e) {
            System.err.println("[KafkaBus] Eroare decodare proto: " + e.getMessage());
        }
    }

    private boolean matchesBrokerKey(String key, int brokerId) {
        return key != null && key.equals(String.valueOf(brokerId));
    }

    // ── MessageBus API ────────────────────────────────────────────────────────

    @Override
    public void stop() {
        running = false;
        activeBrokers.clear();
        consumers.forEach(c -> { try { c.wakeup(); } catch (Exception ignored) {} });
        consumerPool.shutdown();
        try {
            consumerPool.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        brokerConsumers.clear();
        brokerConsumerFutures.clear();
        consumers.clear();
        if (producer != null) producer.close(Duration.ofSeconds(5));
    }

    @Override
    public void publishToBroker(int brokerId, PubMessage msg) {
        producer.send(new ProducerRecord<>(topicPubs,
                String.valueOf(brokerId), ProtoCodec.encode(msg)));
    }

    @Override
    public void forwardToBroker(BrokerMessage msg) {
        producer.send(new ProducerRecord<>(topicBrokerFwd,
                String.valueOf(msg.getTargetBrokerId()), ProtoCodec.encode(msg)));
    }

    @Override
    public void deliverToSubscriber(DeliveryReport report) {
        String topic = topicDeliveryPrefix + report.getSubscriberId();
        producer.send(new ProducerRecord<>(topic, ProtoCodec.encode(report)));
    }

    @Override
    public void registerSubscription(SubRegistration reg) {
        sendSubRegistrationToBroker(0, reg);
    }

    @Override
    public void sendSubRegistrationToBroker(int brokerId, SubRegistration reg) {
        producer.send(new ProducerRecord<>(topicSubReg,
                String.valueOf(brokerId), ProtoCodec.encode(reg)));
    }

    @Override
    public void onPublishToBroker(int brokerId, Consumer<PubMessage> handler) {
        pubHandlers[brokerId].add(handler);
    }

    @Override
    public void onForwardToBroker(int brokerId, Consumer<BrokerMessage> handler) {
        brokerFwdHandlers[brokerId].add(handler);
    }

    @Override
    public void onDeliverToSubscriber(String subscriberId, Consumer<DeliveryReport> handler) {
        deliveryHandlers
                .computeIfAbsent(subscriberId, k -> new CopyOnWriteArrayList<>())
                .add(handler);
    }

    @Override
    public void onRegisterSubscription(int brokerId, Consumer<SubRegistration> handler) {
        subRegHandlers[brokerId].add(handler);
    }

    @Override
    public void stopBrokerConsumer(int brokerId) {
        activeBrokers.remove(brokerId);
        KafkaConsumer<String, String> consumer = brokerConsumers.get(brokerId);
        if (consumer != null) {
            consumer.wakeup();
        }
        System.out.println("[KafkaBus] Broker " + brokerId + " consumer oprit.");
    }

    @Override
    public void startBrokerConsumer(int brokerId) {
        activeBrokers.add(brokerId);
        startBrokerConsumerInternal(brokerId);
        System.out.println("[KafkaBus] Broker " + brokerId + " consumer repornit.");
    }

    @Override
    public boolean isBrokerConsumerRunning(int brokerId) {
        return activeBrokers.contains(brokerId) && brokerConsumers.containsKey(brokerId);
    }

    // ── Proto Codec ───────────────────────────────────────────────────────────

    /**
     * Serializeaza/deserializeaza modelele Java <-> Protocol Buffers.
     * Mesajele proto sunt encodate Base64 pentru transport prin Kafka StringSerializer.
     *
     * Tag-uri tip pentru ProtoTypedValue:
     *   "s"  = String
     *   "d"  = Double
     *   "ld" = LocalDate (ISO-8601)
     */
    static final class ProtoCodec {

        private static final String TAG_STRING = "s";
        private static final String TAG_DOUBLE = "d";
        private static final String TAG_DATE   = "ld";

        private ProtoCodec() {}

        // ── Encode ────────────────────────────────────────────────────────────

        static String encode(PubMessage m) {
            EbsProto.ProtoPubMessage proto = EbsProto.ProtoPubMessage.newBuilder()
                    .setPubId(m.getPubId())
                    .setTimestamp(m.getTimestamp())
                    .setPublication(encodePublication(m.getPublication()))
                    .build();
            return toBase64(proto.toByteArray());
        }

        static String encode(BrokerMessage m) {
            EbsProto.ProtoBrokerMessage.Builder builder = EbsProto.ProtoBrokerMessage.newBuilder()
                    .setCurrentBrokerIndex(m.getCurrentBrokerIndex())
                    .setTargetBrokerId(m.getTargetBrokerId())
                    .setPubMessage(decodePubProto(encode(m.getPubMessage())));
            m.getMatchedSubscriberIds().forEach(builder::addMatchedSubscriberIds);
            return toBase64(builder.build().toByteArray());
        }

        static String encode(SubRegistration m) {
            EbsProto.ProtoSubRegistration proto = EbsProto.ProtoSubRegistration.newBuilder()
                    .setSubscriberId(m.getSubscriberId())
                    .setSubIndex(m.getSubIndex())
                    .setSubscription(encodeSubscription(m.getSubscription()))
                    .build();
            return toBase64(proto.toByteArray());
        }

        static String encode(DeliveryReport m) {
            EbsProto.ProtoDeliveryReport proto = EbsProto.ProtoDeliveryReport.newBuilder()
                    .setPubId(m.getPubId())
                    .setSubscriberId(m.getSubscriberId())
                    .setDeliveryTimestamp(m.getDeliveryTimestamp())
                    .setLatencyMs(m.getLatencyMs())
                    .build();
            return toBase64(proto.toByteArray());
        }

        // ── Decode ────────────────────────────────────────────────────────────

        static PubMessage decodePub(String base64) {
            try {
                EbsProto.ProtoPubMessage proto =
                        EbsProto.ProtoPubMessage.parseFrom(fromBase64(base64));
                return new PubMessage(
                        proto.getPubId(),
                        decodePublication(proto.getPublication()),
                        proto.getTimestamp()
                );
            } catch (InvalidProtocolBufferException e) {
                throw new IllegalArgumentException("Eroare decodare ProtoPubMessage", e);
            }
        }

        static BrokerMessage decodeBroker(String base64) {
            try {
                EbsProto.ProtoBrokerMessage proto =
                        EbsProto.ProtoBrokerMessage.parseFrom(fromBase64(base64));
                PubMessage pub = decodePub(toBase64(proto.getPubMessage().toByteArray()));
                BrokerMessage msg = new BrokerMessage(pub, proto.getCurrentBrokerIndex());
                msg.setTargetBrokerId(proto.getTargetBrokerId());
                proto.getMatchedSubscriberIdsList()
                        .forEach(msg.getMatchedSubscriberIds()::add);
                return msg;
            } catch (InvalidProtocolBufferException e) {
                throw new IllegalArgumentException("Eroare decodare ProtoBrokerMessage", e);
            }
        }

        static SubRegistration decodeSub(String base64) {
            try {
                EbsProto.ProtoSubRegistration proto =
                        EbsProto.ProtoSubRegistration.parseFrom(fromBase64(base64));
                return new SubRegistration(
                        proto.getSubscriberId(),
                        proto.getSubIndex(),
                        decodeSubscription(proto.getSubscription())
                );
            } catch (InvalidProtocolBufferException e) {
                throw new IllegalArgumentException("Eroare decodare ProtoSubRegistration", e);
            }
        }

        static DeliveryReport decodeDelivery(String base64) {
            try {
                EbsProto.ProtoDeliveryReport proto =
                        EbsProto.ProtoDeliveryReport.parseFrom(fromBase64(base64));
                return new DeliveryReport(
                        proto.getPubId(),
                        proto.getSubscriberId(),
                        proto.getDeliveryTimestamp(),
                        proto.getLatencyMs()
                );
            } catch (InvalidProtocolBufferException e) {
                throw new IllegalArgumentException("Eroare decodare ProtoDeliveryReport", e);
            }
        }

        // ── Publication helpers ───────────────────────────────────────────────

        private static EbsProto.ProtoPublication encodePublication(Publication pub) {
            EbsProto.ProtoPublication.Builder builder = EbsProto.ProtoPublication.newBuilder();
            for (PublicationField field : pub.getFields()) {
                builder.addFields(EbsProto.ProtoPublicationField.newBuilder()
                        .setFieldName(field.getFieldName())
                        .setValue(encodeTypedValue(field.getValue()))
                        .build());
            }
            return builder.build();
        }

        private static Publication decodePublication(EbsProto.ProtoPublication proto) {
            Publication pub = new Publication();
            for (EbsProto.ProtoPublicationField f : proto.getFieldsList()) {
                pub.addField(new PublicationField(
                        f.getFieldName(),
                        decodeTypedValue(f.getValue())
                ));
            }
            return pub;
        }

        // ── Subscription helpers ──────────────────────────────────────────────

        private static EbsProto.ProtoSubscription encodeSubscription(Subscription sub) {
            EbsProto.ProtoSubscription.Builder builder = EbsProto.ProtoSubscription.newBuilder();
            for (SubscriptionField field : sub.getFields()) {
                builder.addFields(EbsProto.ProtoSubscriptionField.newBuilder()
                        .setFieldName(field.getFieldName())
                        .setOperator(field.getOperator())
                        .setValue(encodeTypedValue(field.getValue()))
                        .build());
            }
            return builder.build();
        }

        private static Subscription decodeSubscription(EbsProto.ProtoSubscription proto) {
            Subscription sub = new Subscription();
            for (EbsProto.ProtoSubscriptionField f : proto.getFieldsList()) {
                sub.addField(new SubscriptionField(
                        f.getFieldName(),
                        f.getOperator(),
                        decodeTypedValue(f.getValue())
                ));
            }
            return sub;
        }

        // ── TypedValue helpers ────────────────────────────────────────────────

        private static EbsProto.ProtoTypedValue encodeTypedValue(Object value) {
            EbsProto.ProtoTypedValue.Builder b = EbsProto.ProtoTypedValue.newBuilder();
            if (value instanceof String s) {
                b.setTypeTag(TAG_STRING).setRawValue(s);
            } else if (value instanceof Double d) {
                b.setTypeTag(TAG_DOUBLE).setRawValue(Double.toString(d));
            } else if (value instanceof LocalDate ld) {
                b.setTypeTag(TAG_DATE).setRawValue(ld.toString());
            } else {
                throw new IllegalArgumentException(
                        "Tip valoare nesuportat pentru serializare proto: "
                                + (value == null ? "null" : value.getClass().getName()));
            }
            return b.build();
        }

        private static Object decodeTypedValue(EbsProto.ProtoTypedValue proto) {
            return switch (proto.getTypeTag()) {
                case TAG_STRING -> proto.getRawValue();
                case TAG_DOUBLE -> Double.parseDouble(proto.getRawValue());
                case TAG_DATE   -> LocalDate.parse(proto.getRawValue());
                default -> throw new IllegalArgumentException(
                        "Tag tip proto necunoscut: " + proto.getTypeTag());
            };
        }

        // ── Base64 helpers ────────────────────────────────────────────────────

        private static String toBase64(byte[] bytes) {
            return Base64.getEncoder().encodeToString(bytes);
        }

        private static byte[] fromBase64(String s) {
            return Base64.getDecoder().decode(s);
        }

        // ── Helper intern: reutilizare parsare ProtoPubMessage ────────────────

        private static EbsProto.ProtoPubMessage decodePubProto(String base64) {
            try {
                return EbsProto.ProtoPubMessage.parseFrom(fromBase64(base64));
            } catch (InvalidProtocolBufferException e) {
                throw new IllegalArgumentException("Eroare decodare ProtoPubMessage intern", e);
            }
        }
    }
}