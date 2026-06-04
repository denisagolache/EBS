package org.ebs.pubsub.broker;

import org.ebs.publication.Publication;
import org.ebs.publication.PublicationField;
import org.ebs.subscription.Subscription;
import org.ebs.subscription.SubscriptionField;

import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

public class SubscriptionStore {

    private final ConcurrentHashMap<String, List<Subscription>> subsBySubscriber = new ConcurrentHashMap<>();

    // Total subscriptii evaluate (pentru rata per subscriptie)
    private final AtomicLong totalEvaluations = new AtomicLong(0);
    // Total subscriptii care au dat match
    private final AtomicLong totalMatches     = new AtomicLong(0);

    public void addSubscription(String subscriberId, Subscription sub) {
        subsBySubscriber.computeIfAbsent(subscriberId, k -> new CopyOnWriteArrayList<>()).add(sub);
    }

    public void clear() {
        subsBySubscriber.clear();
        totalEvaluations.set(0);
        totalMatches.set(0);
    }

    public int getTotalSubscriptions() {
        return subsBySubscriber.values().stream().mapToInt(List::size).sum();
    }

    /**
     * Returneaza subscriberId-urile care au cel putin o subscriptie potrivita.
     * Totodata contorizeaza evaluarile si match-urile individuale per subscriptie.
     */
    public Set<String> match(Publication pub) {
        Set<String> matched = new HashSet<>();
        for (Map.Entry<String, List<Subscription>> entry : subsBySubscriber.entrySet()) {
            for (Subscription sub : entry.getValue()) {
                totalEvaluations.incrementAndGet();
                if (matches(sub, pub)) {
                    totalMatches.incrementAndGet();
                    matched.add(entry.getKey()); // nu break: continuam sa contorizam
                }
            }
        }
        return matched;
    }

    /** Rata de matching per subscriptie individuala: matches / evaluations */
    public double getPerSubscriptionMatchRate() {
        long evals = totalEvaluations.get();
        return evals == 0 ? 0.0 : (double) totalMatches.get() / evals;
    }

    public long getTotalEvaluations() { return totalEvaluations.get(); }
    public long getTotalMatches()     { return totalMatches.get(); }

    private boolean matches(Subscription sub, Publication pub) {
        for (SubscriptionField sf : sub.getFields()) {
            PublicationField pf = findField(pub, sf.getFieldName());
            if (pf == null) return false;
            if (!evaluate(sf.getOperator(), pf.getValue(), sf.getValue())) return false;
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