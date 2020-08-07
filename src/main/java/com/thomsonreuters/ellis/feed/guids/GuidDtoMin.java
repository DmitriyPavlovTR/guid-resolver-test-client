package com.thomsonreuters.ellis.feed.guids;

import java.time.Instant;

public class GuidDtoMin {
    private String context;
    private String parentContext;
    private String correlationId;
    private String guid;
    private String collection;
    /**
     * Is true if collection field was updated as a result of current resolve request
     */
    private boolean collectionUpdated;

    public String getGuid(){
        return guid;
    }

    public String getContext() {
        return context;
    }
}