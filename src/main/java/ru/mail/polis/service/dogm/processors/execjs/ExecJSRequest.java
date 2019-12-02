package ru.mail.polis.service.dogm.processors.execjs;

import one.nio.http.Request;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.service.dogm.ReplicasFraction;
import ru.mail.polis.service.dogm.processors.ParsedRequest;

/**
 * Parsed request on /v0/execjs URL.
 */
public class ExecJSRequest extends ParsedRequest {
    protected final String js;

    /**
     * Creates new parsed request out of useful info.
     * @param js          JavaScript source code
     * @param fromCluster is request coming from cluster coordinator?
     * @param rawRequest  original one-nio request
     * @param fraction    number of replicas, see {@link ReplicasFraction}
     */
    public ExecJSRequest(@NotNull final String js,
                         final boolean fromCluster,
                         @NotNull final Request rawRequest,
                         @NotNull final ReplicasFraction fraction) {
        super(fromCluster, rawRequest, fraction);
        this.js = js;
    }
}
