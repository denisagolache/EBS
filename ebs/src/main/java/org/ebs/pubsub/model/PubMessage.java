package org.ebs.pubsub.model;

import org.ebs.publication.Publication;

public class PubMessage {
    private final String pubId;
    private final Publication publication;
    private final long timestamp;

    public PubMessage(String pubId, Publication publication) {
        this(pubId, publication, System.currentTimeMillis());
    }

    public PubMessage(String pubId, Publication publication, long timestamp) {
        this.pubId = pubId;
        this.publication = publication;
        this.timestamp = timestamp;
    }

    public String getPubId() { return pubId; }
    public Publication getPublication() { return publication; }
    public long getTimestamp() { return timestamp; }
}
