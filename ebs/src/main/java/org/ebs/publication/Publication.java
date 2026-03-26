package org.ebs.publication;

import java.util.ArrayList;
import java.util.List;

public class Publication {

    private final List<PublicationField> fields = new ArrayList<>();

    public void addField(PublicationField field) {
        fields.add(field);
    }

    public List<PublicationField> getFields() {
        return fields;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < fields.size(); i++) {
            sb.append(fields.get(i));
            if (i < fields.size() - 1) sb.append("; ");
        }
        sb.append("}");
        return sb.toString();
    }
}