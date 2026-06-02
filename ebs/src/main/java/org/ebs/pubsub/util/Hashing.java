package org.ebs.pubsub.util;

public class Hashing {

    public static int distribute(String key, int total) {
        int hash = key.hashCode();
        return ((hash % total) + total) % total;
    }

    public static int distribute(String subscriberId, int subIndex, int total) {
        return distribute(subscriberId + "_" + subIndex, total);
    }
}
