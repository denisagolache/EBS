package org.ebs.pubsub.config;

import org.ebs.publication.FieldConfig;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SystemConfig {

    public static final String KAFKA_BOOTSTRAP_SERVERS =
            System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");

    public static final int PUBLISHER_COUNT = 2;
    public static final int BROKER_COUNT = 3;
    public static final int SUBSCRIBER_COUNT = 3;
    public static final int SUBSCRIPTION_COUNT = 10000;
    public static final long FEED_DURATION_MS = 180_000;
    public static final int PUBS_PER_SECOND = 200;

    public static List<FieldConfig> buildFieldConfigs() {
        List<FieldConfig> configs = new ArrayList<>();
        configs.add(new FieldConfig("company",
                List.of("Google", "Microsoft", "Apple", "Amazon", "Meta", "Tesla")));
        configs.add(new FieldConfig("value",      10.0, 1000.0));
        configs.add(new FieldConfig("drop",        0.0,   50.0));
        configs.add(new FieldConfig("variation",  -5.0,    5.0));
        configs.add(new FieldConfig("date",
                LocalDate.of(2020, 1, 1),
                LocalDate.of(2024, 12, 31)));
        return configs;
    }

    public static final Map<String, Double> FIELD_FREQ = Map.of(
            "company",   0.90,
            "value",     0.80,
            "drop",      0.60,
            "variation", 0.70,
            "date",      0.50
    );

    /** Doar campul 'value' pentru test izolat de matching */
    public static final Map<String, Double> FIELD_FREQ_VALUE_ONLY = Map.of(
            "value", 1.0
    );

    public static Map<String, Double> eqFreqFull() {
        return Map.of("value", 1.0);
    }

    public static Map<String, Double> eqFreqQuarter() {
        return Map.of("value", 0.25);
    }
}
