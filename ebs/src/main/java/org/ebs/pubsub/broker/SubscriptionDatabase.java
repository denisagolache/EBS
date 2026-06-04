package org.ebs.pubsub.broker;

import org.ebs.proto.EbsProto;
import org.ebs.subscription.Subscription;
import org.ebs.subscription.SubscriptionField;

import java.sql.*;
import java.time.LocalDate;
import java.util.*;

public class SubscriptionDatabase implements AutoCloseable {

    private final Connection connection;

    public SubscriptionDatabase(String jdbcUrl, String user, String password) throws SQLException {
        this.connection = DriverManager.getConnection(jdbcUrl, user, password);
        initSchema();
    }

    private void initSchema() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS subscriptions (" +
                    "  broker_id INT NOT NULL," +
                    "  subscriber_id VARCHAR(255) NOT NULL," +
                    "  sub_index INT NOT NULL," +
                    "  subscription_data TEXT NOT NULL," +
                    "  PRIMARY KEY (broker_id, subscriber_id, sub_index)" +
                    ")");
        }
    }

    public void saveSubscription(int brokerId, String subscriberId, int subIndex, Subscription sub) throws SQLException {
        String data = serializeSubscription(sub);
        try (PreparedStatement ps = connection.prepareStatement(
                "INSERT INTO subscriptions (broker_id, subscriber_id, sub_index, subscription_data) VALUES (?, ?, ?, ?) " +
                "ON CONFLICT (broker_id, subscriber_id, sub_index) DO UPDATE SET subscription_data = EXCLUDED.subscription_data")) {
            ps.setInt(1, brokerId);
            ps.setString(2, subscriberId);
            ps.setInt(3, subIndex);
            ps.setString(4, data);
            ps.executeUpdate();
        }
    }

    public Map<String, List<Subscription>> loadAll(int brokerId) throws SQLException {
        Map<String, List<Subscription>> result = new LinkedHashMap<>();
        try (PreparedStatement ps = connection.prepareStatement(
                "SELECT subscriber_id, subscription_data FROM subscriptions WHERE broker_id = ? ORDER BY subscriber_id, sub_index")) {
            ps.setInt(1, brokerId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String subscriberId = rs.getString("subscriber_id");
                    String data = rs.getString("subscription_data");
                    Subscription sub = deserializeSubscription(data);
                    result.computeIfAbsent(subscriberId, k -> new ArrayList<>()).add(sub);
                }
            }
        }
        return result;
    }

    public void deleteAll(int brokerId) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(
                "DELETE FROM subscriptions WHERE broker_id = ?")) {
            ps.setInt(1, brokerId);
            ps.executeUpdate();
        }
    }

    public void clearAll() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("DELETE FROM subscriptions");
        }
    }

    @Override
    public void close() throws Exception {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    private String serializeSubscription(Subscription sub) {
        EbsProto.ProtoSubscription.Builder builder = EbsProto.ProtoSubscription.newBuilder();
        for (SubscriptionField f : sub.getFields()) {
            EbsProto.ProtoTypedValue.Builder vb = EbsProto.ProtoTypedValue.newBuilder();
            if (f.getValue() instanceof String s) {
                vb.setTypeTag("s").setRawValue(s);
            } else if (f.getValue() instanceof Double d) {
                vb.setTypeTag("d").setRawValue(Double.toString(d));
            } else if (f.getValue() instanceof LocalDate ld) {
                vb.setTypeTag("ld").setRawValue(ld.toString());
            }
            builder.addFields(EbsProto.ProtoSubscriptionField.newBuilder()
                    .setFieldName(f.getFieldName())
                    .setOperator(f.getOperator())
                    .setValue(vb.build()));
        }
        return Base64.getEncoder().encodeToString(builder.build().toByteArray());
    }

    private Subscription deserializeSubscription(String data) {
        try {
            EbsProto.ProtoSubscription proto = EbsProto.ProtoSubscription.parseFrom(
                    Base64.getDecoder().decode(data));
            Subscription sub = new Subscription();
            for (EbsProto.ProtoSubscriptionField f : proto.getFieldsList()) {
                Object value = switch (f.getValue().getTypeTag()) {
                    case "s" -> f.getValue().getRawValue();
                    case "d" -> Double.parseDouble(f.getValue().getRawValue());
                    case "ld" -> LocalDate.parse(f.getValue().getRawValue());
                    default -> f.getValue().getRawValue();
                };
                sub.addField(new SubscriptionField(f.getFieldName(), f.getOperator(), value));
            }
            return sub;
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            throw new RuntimeException("Failed to deserialize subscription", e);
        }
    }
}
