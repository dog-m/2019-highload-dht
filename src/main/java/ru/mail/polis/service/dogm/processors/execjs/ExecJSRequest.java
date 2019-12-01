package ru.mail.polis.service.dogm.processors.execjs;

import one.nio.http.Request;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.service.dogm.ReplicasFraction;
import ru.mail.polis.service.dogm.processors.ParsedRequest;

/**
 *
 */
public class ExecJSRequest extends ParsedRequest {
    protected final String js;

    /**
     * @param js
     * @param fromCluster
     * @param rawRequest
     * @param fraction
     */
    public ExecJSRequest(@NotNull final String js,
                         final boolean fromCluster,
                         @NotNull final Request rawRequest,
                         @NotNull final ReplicasFraction fraction) {
        super(fromCluster, rawRequest, fraction);
        this.js = js;
    }
}
