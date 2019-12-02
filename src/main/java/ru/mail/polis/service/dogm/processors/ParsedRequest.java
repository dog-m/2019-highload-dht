package ru.mail.polis.service.dogm.processors;

import one.nio.http.Request;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.service.dogm.ReplicasFraction;

/**
 * Storage for all useful parsed data from a request.
 */
public class ParsedRequest {
    public final boolean fromCluster;
    public final Request raw;
    public final ReplicasFraction fraction;

    /**
     * Creates new parsed request out of useful info.
     * @param fromCluster is request coming from cluster coordinator?
     * @param rawRequest  original one-nio request
     * @param fraction    number of replicas, see {@link ReplicasFraction}
     */
    protected ParsedRequest(final boolean fromCluster,
                            @NotNull final Request rawRequest,
                            @NotNull final ReplicasFraction fraction) {
        this.fromCluster = fromCluster;
        this.raw = rawRequest;
        this.fraction = fraction;
    }
}
