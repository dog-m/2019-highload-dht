package ru.mail.polis.service.dogm.processors;

import one.nio.http.HttpException;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.pool.PoolException;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.dogm.RocksDAO;
import ru.mail.polis.service.dogm.Bridges;
import ru.mail.polis.service.dogm.ReplicasFraction;
import ru.mail.polis.service.dogm.Topology;

import java.io.IOException;
import java.util.logging.Logger;

import static java.nio.charset.StandardCharsets.UTF_8;

public abstract class SimpleRequestProcessor {
    protected final Logger log = Logger.getLogger(getClass().getName());
    protected final RocksDAO dao;
    protected final Topology topology;
    private final Bridges bridges;

    SimpleRequestProcessor(final RocksDAO dao, final Topology topology, final Bridges bridges) {
        this.dao = dao;
        this.topology = topology;
        this.bridges = bridges;
    }

    public abstract Response processEntityRequest(@NotNull final String id,
                                                  @NotNull final ReplicasFraction fraction,
                                                  @NotNull final Request request);

    public abstract Response processEntityDirectly(@NotNull final String id,
                                                   @NotNull final Request request);

    protected Response processEntityRequestOnClusterEmptyResult(@NotNull final String id,
                                                                @NotNull final ReplicasFraction fraction,
                                                                @NotNull final Request request,
                                                                final String codeString,
                                                                final int codeInteger) {
        int successfulResponses = 0;
        for (final var node : topology.nodesFor(id, fraction.from)) {
            try {
                final Response response =
                        topology.isMe(node)
                                ? processEntityDirectly(id, request)
                                : processEntityRemotely(node, request);
                if (response.getStatus() == codeInteger) {
                    ++successfulResponses;
                }
            } catch (IOException e) {
                log.warning(Protocol.WARN_PROCESSOR);
            }
        }

        if (successfulResponses < fraction.ack) {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        } else {
            return new Response(codeString, Response.EMPTY);
        }
    }

    static Response getWrongProcessorResponse() {
        return new Response(Response.INTERNAL_ERROR, Protocol.FAIL_WRONG_PROCESSOR.getBytes(UTF_8));
    }

    Response processEntityRemotely(final String node, final Request request) throws IOException {
        try {
            return bridges.sendRequestTo(request, node);
        } catch (InterruptedException | PoolException | HttpException e) {
            log.warning(Protocol.WARN_PROXY);
            return new Response(Response.INTERNAL_ERROR, Protocol.WARN_PROXY.getBytes(UTF_8));
        }
    }
}
