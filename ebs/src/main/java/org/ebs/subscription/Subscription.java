package org.ebs.subscription;

import java.util.ArrayList;
import java.util.List;

public class Subscription {

    private final List<SubscriptionField> fields = new ArrayList<>();

    public void addField(SubscriptionField field) {
        fields.add(field);
    }

    public List<SubscriptionField> getFields() {
        return fields;
    }

    @Override
    public String toString() {
        if (fields.isEmpty()) return "{}";
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < fields.size(); i++) {
            sb.append(fields.get(i));
            if (i < fields.size() - 1) sb.append("; ");
        }
        sb.append("}");
        return sb.toString();
    }
}