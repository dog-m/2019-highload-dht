package ru.mail.polis.service.dogm.processors.entity;

import one.nio.http.Request;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.service.dogm.ReplicasFraction;
import ru.mail.polis.service.dogm.processors.ParsedRequest;

/**
 * Parsed request on /v0/entity URL.
 */
public class EntityRequest extends ParsedRequest {
    protected final String id;

    /**
     * Creates new parsed request out of useful info.
     * @param id          identifier
     * @param fromCluster is request coming from cluster coordinator?
     * @param rawRequest  original one-nio request
     * @param fraction    number of replicas, see {@link ReplicasFraction}
     */
    public EntityRequest(@NotNull final String id,
                         final boolean fromCluster,
                         @NotNull final Request rawRequest,
                         @NotNull final ReplicasFraction fraction) {
        super(fromCluster, rawRequest, fraction);
        this.id = id;
    }
}
