package org.ebs.publication;

import java.time.LocalDate;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class FieldConfig {

    public enum FieldType { ENUM, DOUBLE, DATE }

    private final String    name;
    private final FieldType type;

    private final List<String> enumValues;

    private final double minDouble;
    private final double maxDouble;

    private final LocalDate minDate;
    private final LocalDate maxDate;

    public FieldConfig(String name, List<String> enumValues) {
        this.name       = name;
        this.type       = FieldType.ENUM;
        this.enumValues = enumValues;
        this.minDouble  = 0;
        this.maxDouble  = 0;
        this.minDate    = null;
        this.maxDate    = null;
    }

    public FieldConfig(String name, double minDouble, double maxDouble) {
        this.name       = name;
        this.type       = FieldType.DOUBLE;
        this.enumValues = null;
        this.minDouble  = minDouble;
        this.maxDouble  = maxDouble;
        this.minDate    = null;
        this.maxDate    = null;
    }

    public FieldConfig(String name, LocalDate minDate, LocalDate maxDate) {
        this.name       = name;
        this.type       = FieldType.DATE;
        this.enumValues = null;
        this.minDouble  = 0;
        this.maxDouble  = 0;
        this.minDate    = minDate;
        this.maxDate    = maxDate;
    }

    public String    getName() { return name; }
    public FieldType getType() { return type; }

    public Object generateValue() {
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        return switch (type) {
            case ENUM -> enumValues.get(rng.nextInt(enumValues.size()));
            case DOUBLE -> {
                double raw = rng.nextDouble(minDouble, maxDouble);
                yield Math.round(raw * 100.0) / 100.0;
            }
            case DATE -> {
                long minDay = minDate.toEpochDay();
                long maxDay = maxDate.toEpochDay();
                yield LocalDate.ofEpochDay(rng.nextLong(minDay, maxDay + 1));
            }
        };
    }
}