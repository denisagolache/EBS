package org.ebs.pubsub.broker;

import org.ebs.publication.Publication;
import org.ebs.publication.PublicationField;
import org.ebs.subscription.Subscription;
import org.ebs.subscription.SubscriptionField;

import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class SubscriptionStore {

    private final ConcurrentHashMap<String, List<Subscription>> subsBySubscriber = new ConcurrentHashMap<>();

    public void addSubscription(String subscriberId, Subscription sub) {
        subsBySubscriber.computeIfAbsent(subscriberId, k -> new CopyOnWriteArrayList<>()).add(sub);
    }

    public int getTotalSubscriptions() {
        return subsBySubscriber.values().stream().mapToInt(List::size).sum();
    }

    public Set<String> match(Publication pub) {
        Set<String> matched = new HashSet<>();
        for (Map.Entry<String, List<Subscription>> entry : subsBySubscriber.entrySet()) {
            for (Subscription sub : entry.getValue()) {
                if (matches(sub, pub)) {
                    matched.add(entry.getKey());
                    break;
                }
            }
        }
        return matched;
    }

    private boolean matches(Subscription sub, Publication pub) {
        for (SubscriptionField sf : sub.getFields()) {
            PublicationField pf = findField(pub, sf.getFieldName());
            if (pf == null) return false;
            if (!evaluate(sf.getOperator(), pf.getValue(), sf.getValue())) {
                return false;
            }
        }
        return true;
    }

    private PublicationField findField(Publication pub, String name) {
        for (PublicationField f : pub.getFields()) {
            if (f.getFieldName().equals(name)) return f;
        }
        return null;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private boolean evaluate(String op, Object pubVal, Object subVal) {
        if (pubVal == null || subVal == null) return false;
        int cmp;
        if (pubVal instanceof String s1 && subVal instanceof String s2) {
            cmp = s1.compareTo(s2);
        } else if (pubVal instanceof Double d1 && subVal instanceof Double d2) {
            cmp = Double.compare(d1, d2);
        } else if (pubVal instanceof LocalDate d1 && subVal instanceof LocalDate d2) {
            cmp = d1.compareTo(d2);
        } else if (pubVal instanceof Number n1 && subVal instanceof Number n2) {
            cmp = Double.compare(n1.doubleValue(), n2.doubleValue());
        } else {
            cmp = pubVal.toString().compareTo(subVal.toString());
        }
        return switch (op) {
            case "="  -> cmp == 0;
            case "!=" -> cmp != 0;
            case "<"  -> cmp < 0;
            case "<=" -> cmp <= 0;
            case ">"  -> cmp > 0;
            case ">=" -> cmp >= 0;
            default   -> false;
        };
    }
}
