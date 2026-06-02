package org.ebs.pubsub.msg;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.UUID;

/**
 * MessageBus peste Apache Kafka.
 * Kafka este folosit doar pentru livrarea mesajelor.
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
    private final ExecutorService consumerPool;
    private volatile boolean running;

    private final List<Consumer<PubMessage>>[] pubHandlers;
    private final List<Consumer<BrokerMessage>>[] brokerFwdHandlers;
    private final List<Consumer<SubRegistration>>[] subRegHandlers;
    private final ConcurrentHashMap<String, List<Consumer<DeliveryReport>>> deliveryHandlers = new ConcurrentHashMap<>();

    @SuppressWarnings("unchecked")
    public KafkaBus(String bootstrapServers, int totalBrokers) {
        this.bootstrapServers = bootstrapServers;
        this.totalBrokers = totalBrokers;
        this.consumerGroupRunId = UUID.randomUUID().toString().substring(0, 8);
        String topicRunId = UUID.randomUUID().toString().substring(0, 8);
        this.topicPubs = "ebs-pubs-" + topicRunId;
        this.topicBrokerFwd = "ebs-broker-fwd-" + topicRunId;
        this.topicSubReg = "ebs-sub-reg-" + topicRunId;
        this.topicDeliveryPrefix = "ebs-delivery-" + topicRunId + "-";
        this.consumerPool = Executors.newCachedThreadPool();
        this.pubHandlers = new List[totalBrokers];
        this.brokerFwdHandlers = new List[totalBrokers];
        this.subRegHandlers = new List[totalBrokers];
        for (int i = 0; i < totalBrokers; i++) {
            pubHandlers[i] = new CopyOnWriteArrayList<>();
            brokerFwdHandlers[i] = new CopyOnWriteArrayList<>();
            subRegHandlers[i] = new CopyOnWriteArrayList<>();
        }
    }

    @Override
    public void start() {
        running = true;
        createTopics();

        producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.RETRIES_CONFIG, 3,
                ProducerConfig.LINGER_MS_CONFIG, 5
        ));

        for (int i = 0; i < totalBrokers; i++) {
            startBrokerConsumer(i);
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
                addTopicIfMissing(existing, newTopics, topicPubs, 1);
                addTopicIfMissing(existing, newTopics, topicBrokerFwd, totalBrokers);
                addTopicIfMissing(existing, newTopics, topicSubReg, 1);
                for (int i = 0; i < totalBrokers; i++) {
                    addTopicIfMissing(existing, newTopics, topicDeliveryPrefix + "sub-" + i, 1);
                }
                if (!newTopics.isEmpty()) {
                    admin.createTopics(newTopics).all().get(8, TimeUnit.SECONDS);
                }
                return;
            } catch (Exception e) {
                last = e;
                if (attempt >= 20) break;
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

    private void addTopicIfMissing(Set<String> existing, List<NewTopic> newTopics, String topic, int partitions) {
        if (!existing.contains(topic)) {
            newTopics.add(new NewTopic(topic, Optional.of(partitions), Optional.of((short) 1)));
        }
    }

    private void startBrokerConsumer(int brokerId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "broker-" + brokerId + "-" + consumerGroupRunId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of(
                topicPubs,
                topicBrokerFwd,
                topicSubReg,
                topicDeliveryPrefix + "sub-" + brokerId
        ));
        consumers.add(consumer);

        final int bid = brokerId;
        consumerPool.submit(() -> {
            while (running) {
                try {
                    var records = consumer.poll(Duration.ofMillis(200));
                    for (ConsumerRecord<String, String> rec : records) {
                        dispatch(rec.topic(), rec.key(), rec.value(), bid);
                    }
                } catch (Exception e) {
                    if (running) {
                        System.err.println("[KafkaBus] Eroare consumer broker-" + bid + ": " + e.getMessage());
                    }
                }
            }
        });
    }

    private void dispatch(String topic, String key, String value, int brokerId) {
        try {
            if (topicPubs.equals(topic)) {
                if (!matchesBrokerKey(key, brokerId)) {
                    return;
                }
                PubMessage msg = PubCodec.decodePub(value);
                for (Consumer<PubMessage> h : pubHandlers[brokerId]) {
                    h.accept(msg);
                }
                return;
            }

            if (topicBrokerFwd.equals(topic)) {
                BrokerMessage msg = PubCodec.decodeBroker(value);
                if (msg.getTargetBrokerId() != brokerId) {
                    return;
                }
                for (Consumer<BrokerMessage> h : brokerFwdHandlers[brokerId]) {
                    h.accept(msg);
                }
                return;
            }

            if (topicSubReg.equals(topic)) {
                if (!matchesBrokerKey(key, brokerId)) {
                    return;
                }
                SubRegistration msg = PubCodec.decodeSub(value);
                for (Consumer<SubRegistration> h : subRegHandlers[brokerId]) {
                    h.accept(msg);
                }
                return;
            }

            if (topic.startsWith(topicDeliveryPrefix)) {
                DeliveryReport msg = PubCodec.decodeDelivery(value);
                String subId = topic.substring(topicDeliveryPrefix.length());
                List<Consumer<DeliveryReport>> handlers = deliveryHandlers.get(subId);
                if (handlers != null) {
                    for (Consumer<DeliveryReport> h : handlers) {
                        h.accept(msg);
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("[KafkaBus] Eroare decodare: " + e.getMessage());
        }
    }

    private boolean matchesBrokerKey(String key, int brokerId) {
        return key != null && key.equals(String.valueOf(brokerId));
    }

    @Override
    public void stop() {
        running = false;
        for (KafkaConsumer<?, ?> c : consumers) {
            try {
                c.wakeup();
            } catch (Exception ignored) {
            }
            try {
                c.close();
            } catch (Exception ignored) {
            }
        }
        if (producer != null) {
            producer.close(Duration.ofSeconds(5));
        }
        consumerPool.shutdownNow();
    }

    @Override
    public void publishToBroker(int brokerId, PubMessage msg) {
        producer.send(new ProducerRecord<>(topicPubs, String.valueOf(brokerId), PubCodec.encode(msg)));
    }

    @Override
    public void forwardToBroker(BrokerMessage msg) {
        producer.send(new ProducerRecord<>(
                topicBrokerFwd,
                String.valueOf(msg.getTargetBrokerId()),
                PubCodec.encode(msg)
        ));
    }

    @Override
    public void deliverToSubscriber(DeliveryReport report) {
        String topic = topicDeliveryPrefix + report.getSubscriberId();
        producer.send(new ProducerRecord<>(topic, PubCodec.encode(report)));
    }

    @Override
    public void registerSubscription(SubRegistration reg) {
        sendSubRegistrationToBroker(0, reg);
    }

    @Override
    public void sendSubRegistrationToBroker(int brokerId, SubRegistration reg) {
        producer.send(new ProducerRecord<>(topicSubReg, String.valueOf(brokerId), PubCodec.encode(reg)));
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
        deliveryHandlers.computeIfAbsent(subscriberId, k -> new CopyOnWriteArrayList<>()).add(handler);
    }

    @Override
    public void onRegisterSubscription(int brokerId, Consumer<SubRegistration> handler) {
        subRegHandlers[brokerId].add(handler);
    }

    static class PubCodec {
        private static final ObjectMapper MAPPER = new ObjectMapper();

        private static final String FIELD_STRING = "s";
        private static final String FIELD_DOUBLE = "d";
        private static final String FIELD_DATE = "ld";

        static String encode(PubMessage m) {
            ObjectNode root = MAPPER.createObjectNode();
            root.put("pubId", m.getPubId());
            root.put("timestamp", m.getTimestamp());
            root.set("publication", encodePublication(m.getPublication()));
            return write(root);
        }

        static PubMessage decodePub(String s) {
            JsonNode root = read(s);
            String pubId = text(root, "pubId");
            long timestamp = longValue(root, "timestamp");
            Publication publication = decodePublication(root.get("publication"));
            return new PubMessage(pubId, publication, timestamp);
        }

        static String encode(BrokerMessage m) {
            ObjectNode root = MAPPER.createObjectNode();
            root.put("curr", m.getCurrentBrokerIndex());
            root.put("target", m.getTargetBrokerId());
            ArrayNode matched = MAPPER.createArrayNode();
            for (String subId : m.getMatchedSubscriberIds()) {
                matched.add(subId);
            }
            root.set("matched", matched);
            root.set("pub", read(encode(m.getPubMessage())));
            return write(root);
        }

        static BrokerMessage decodeBroker(String s) {
            JsonNode root = read(s);
            PubMessage pub = decodePub(write(root.get("pub")));
            BrokerMessage message = new BrokerMessage(pub, intValue(root, "curr"));
            message.setTargetBrokerId(intValue(root, "target"));

            JsonNode matched = root.get("matched");
            if (matched != null && matched.isArray()) {
                for (JsonNode node : matched) {
                    message.getMatchedSubscriberIds().add(node.asText());
                }
            }
            return message;
        }

        static String encode(SubRegistration m) {
            ObjectNode root = MAPPER.createObjectNode();
            root.put("subId", m.getSubscriberId());
            root.put("idx", m.getSubIndex());
            root.set("subscription", encodeSubscription(m.getSubscription()));
            return write(root);
        }

        static SubRegistration decodeSub(String s) {
            JsonNode root = read(s);
            String subscriberId = text(root, "subId");
            int subIndex = intValue(root, "idx");
            Subscription subscription = decodeSubscription(root.get("subscription"));
            return new SubRegistration(subscriberId, subIndex, subscription);
        }

        static String encode(DeliveryReport m) {
            ObjectNode root = MAPPER.createObjectNode();
            root.put("pubId", m.getPubId());
            root.put("subId", m.getSubscriberId());
            root.put("ts", m.getDeliveryTimestamp());
            root.put("latMs", m.getLatencyMs());
            return write(root);
        }

        static DeliveryReport decodeDelivery(String s) {
            JsonNode root = read(s);
            return new DeliveryReport(
                    text(root, "pubId"),
                    text(root, "subId"),
                    longValue(root, "ts"),
                    longValue(root, "latMs")
            );
        }

        private static ObjectNode encodePublication(Publication publication) {
            ObjectNode root = MAPPER.createObjectNode();
            ArrayNode fields = MAPPER.createArrayNode();
            for (PublicationField field : publication.getFields()) {
                ObjectNode f = MAPPER.createObjectNode();
                f.put("name", field.getFieldName());
                f.set("value", encodeTypedValue(field.getValue()));
                fields.add(f);
            }
            root.set("fields", fields);
            return root;
        }

        private static Publication decodePublication(JsonNode node) {
            Publication publication = new Publication();
            if (node == null || !node.has("fields") || !node.get("fields").isArray()) {
                return publication;
            }
            for (JsonNode fieldNode : node.get("fields")) {
                String name = text(fieldNode, "name");
                Object value = decodeTypedValue(fieldNode.get("value"));
                publication.addField(new PublicationField(name, value));
            }
            return publication;
        }

        private static ObjectNode encodeSubscription(Subscription subscription) {
            ObjectNode root = MAPPER.createObjectNode();
            ArrayNode fields = MAPPER.createArrayNode();
            for (SubscriptionField field : subscription.getFields()) {
                ObjectNode f = MAPPER.createObjectNode();
                f.put("name", field.getFieldName());
                f.put("op", field.getOperator());
                f.set("value", encodeTypedValue(field.getValue()));
                fields.add(f);
            }
            root.set("fields", fields);
            return root;
        }

        private static Subscription decodeSubscription(JsonNode node) {
            Subscription subscription = new Subscription();
            if (node == null || !node.has("fields") || !node.get("fields").isArray()) {
                return subscription;
            }
            for (JsonNode fieldNode : node.get("fields")) {
                String name = text(fieldNode, "name");
                String op = text(fieldNode, "op");
                Object value = decodeTypedValue(fieldNode.get("value"));
                subscription.addField(new SubscriptionField(name, op, value));
            }
            return subscription;
        }

        private static ObjectNode encodeTypedValue(Object value) {
            ObjectNode node = MAPPER.createObjectNode();
            if (value instanceof String s) {
                node.put("_t", FIELD_STRING);
                node.put("_v", s);
            } else if (value instanceof Double d) {
                node.put("_t", FIELD_DOUBLE);
                node.put("_v", d);
            } else if (value instanceof LocalDate d) {
                node.put("_t", FIELD_DATE);
                node.put("_v", d.toString());
            } else {
                throw new IllegalArgumentException("Tip valoare nesuportat: " + value);
            }
            return node;
        }

        private static Object decodeTypedValue(JsonNode node) {
            if (node == null || !node.has("_t") || !node.has("_v")) {
                throw new IllegalArgumentException("Valoare serializata invalida");
            }
            String t = text(node, "_t");
            JsonNode v = node.get("_v");
            return switch (t) {
                case FIELD_STRING -> v.asText();
                case FIELD_DOUBLE -> v.asDouble();
                case FIELD_DATE -> LocalDate.parse(v.asText());
                default -> throw new IllegalArgumentException("Tip serializat necunoscut: " + t);
            };
        }

        private static JsonNode read(String s) {
            try {
                return MAPPER.readTree(s);
            } catch (JsonProcessingException e) {
                throw new IllegalArgumentException("JSON invalid", e);
            }
        }

        private static String write(JsonNode node) {
            try {
                return MAPPER.writeValueAsString(node);
            } catch (JsonProcessingException e) {
                throw new IllegalArgumentException("Nu se poate serializa JSON", e);
            }
        }

        private static String text(JsonNode node, String field) {
            JsonNode value = node.get(field);
            if (value == null) {
                throw new IllegalArgumentException("Camp lipsa: " + field);
            }
            return value.asText();
        }

        private static int intValue(JsonNode node, String field) {
            JsonNode value = node.get(field);
            if (value == null) {
                throw new IllegalArgumentException("Camp lipsa: " + field);
            }
            return value.asInt();
        }

        private static long longValue(JsonNode node, String field) {
            JsonNode value = node.get(field);
            if (value == null) {
                throw new IllegalArgumentException("Camp lipsa: " + field);
            }
            return value.asLong();
        }
    }
}
