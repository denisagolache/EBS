package org.ebs.subscription;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * Un camp al unei subscriptii: (nume, operator, valoare).
 * Valoarea poate fi String, Double sau LocalDate.
 */
public class SubscriptionField {

    private static final DateTimeFormatter DATE_FMT =
            DateTimeFormatter.ofPattern("dd.MM.yyyy");

    private final String fieldName;
    private final String operator;
    private final Object value;

    public SubscriptionField(String fieldName, String operator, Object value) {
        this.fieldName = fieldName;
        this.operator  = operator;
        this.value     = value;
    }

    public String getFieldName() { return fieldName; }
    public String getOperator()  { return operator; }
    public Object getValue()     { return value; }

    @Override
    public String toString() {
        if (value instanceof String)
            return "(" + fieldName + ", " + operator + ", \"" + value + "\")";
        if (value instanceof LocalDate)
            return "(" + fieldName + ", " + operator + ", "
                    + ((LocalDate) value).format(DATE_FMT) + ")";
        return "(" + fieldName + ", " + operator + ", " + value + ")";
    }
}