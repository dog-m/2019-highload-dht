package ru.mail.polis.service.dogm.processors;

import one.nio.http.Request;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.service.dogm.ReplicasFraction;

public abstract class ParsedRequest {
    public final boolean fromCluster;
    public final Request raw;
    public final ReplicasFraction fraction;

    public ParsedRequest(final boolean fromCluster,
                         @NotNull final Request rawRequest,
                         @NotNull final ReplicasFraction fraction) {
        this.fromCluster = fromCluster;
        this.raw = rawRequest;
        this.fraction = fraction;
    }
}
