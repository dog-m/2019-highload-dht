package ru.mail.polis.service.dogm.processors.entity;

import one.nio.http.Request;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.service.dogm.ReplicasFraction;
import ru.mail.polis.service.dogm.processors.ParsedRequest;

/**
 *
 */
public class EntityRequest extends ParsedRequest {
    protected final String id;

    /**
     * @param id
     * @param fromCluster
     * @param rawRequest
     * @param fraction
     */
    public EntityRequest(@NotNull final String id,
                         final boolean fromCluster,
                         @NotNull final Request rawRequest,
                         @NotNull final ReplicasFraction fraction) {
        super(fromCluster, rawRequest, fraction);
        this.id = id;
    }
}
