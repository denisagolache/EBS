package org.ebs.publication;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class PublicationField {

    private static final DateTimeFormatter DATE_FMT =
            DateTimeFormatter.ofPattern("dd.MM.yyyy");

    private final String fieldName;
    private final Object value;

    public PublicationField(String fieldName, Object value) {
        this.fieldName = fieldName;
        this.value     = value;
    }

    public String getFieldName() { return fieldName; }
    public Object getValue()     { return value; }

    @Override
    public String toString() {
        if (value instanceof String)
            return "(" + fieldName + ", \"" + value + "\")";
        if (value instanceof LocalDate)
            return "(" + fieldName + ", " + ((LocalDate) value).format(DATE_FMT) + ")";
        return "(" + fieldName + ", " + value + ")";
    }
}