package ru.mail.polis.service.dogm.processors;

import one.nio.http.Request;
import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.dogm.RocksDAO;
import ru.mail.polis.service.dogm.ReplicasFraction;
import ru.mail.polis.service.dogm.Topology;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import static java.nio.charset.StandardCharsets.UTF_8;

public abstract class SimpleRequestProcessor {
    protected final Logger log = Logger.getLogger(getClass().getName());
    protected final RocksDAO dao;
    protected final Topology topology;
    private final Bridges bridges;
    protected final long PROCESSING_TIMEOUT = Bridges.TIMEOUT.toMillis() + 1;

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
        AtomicInteger successfulResponses = new AtomicInteger(0);
        final var result = new CompletableFuture<Integer>();

        for (final var node : topology.nodesFor(id, fraction.from)) {
            (topology.isMe(node)
                    ? CompletableFuture.supplyAsync(() -> processEntityDirectly(id, request))
                    : processEntityRemotely(node, request))
            .thenAccept(
                    response -> {
                        if (response.getStatus() == codeInteger) {
                            final var count = successfulResponses.incrementAndGet();
                            if (count >= fraction.ack) {
                                result.complete(count);
                            }
                        }
                    })
            .exceptionally(e -> {
                log.warning(Protocol.WARN_PROCESSOR);
                return null;
            });
        }

        try {
            if (result.get(PROCESSING_TIMEOUT, TimeUnit.MILLISECONDS) < fraction.ack) {
                return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
            } else {
                return new Response(codeString, Response.EMPTY);
            }
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            return new Response(Response.GATEWAY_TIMEOUT, e.getMessage().getBytes(UTF_8));
        }
    }

    static Response getWrongProcessorResponse() {
        return new Response(Response.INTERNAL_ERROR, Protocol.FAIL_WRONG_PROCESSOR.getBytes(UTF_8));
    }

    CompletableFuture<Response> processEntityRemotely(final String node, final Request request) {
        return bridges.sendRequestTo(request, node).thenApply(
                response -> new Response(String.valueOf(response.statusCode()), response.body())
        );
    }
}
