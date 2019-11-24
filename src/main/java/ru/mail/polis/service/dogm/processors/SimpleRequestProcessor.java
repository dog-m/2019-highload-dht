package ru.mail.polis.service.dogm.processors;

import one.nio.http.Request;
import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.dogm.RocksDAO;
import ru.mail.polis.service.dogm.ReplicasFraction;
import ru.mail.polis.service.dogm.Topology;

import java.io.IOException;
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
    protected static final long TIMEOUT_CLUSTER = Bridges.TIMEOUT_CONNECT.toMillis() * 2;

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
        final var successfulResponses = new AtomicInteger(0);
        final var maxNumberOfExceptions = new AtomicInteger(fraction.from - fraction.ack);
        final var result = new CompletableFuture<Integer>();

        for (final var node : topology.nodesFor(id, fraction.from)) {
            (topology.isMe(node)
                    ? CompletableFuture.supplyAsync(() -> processEntityDirectly(id, request))
                    : processEntityRemotely(node, request))
            .thenAccept(
                    response -> {
                        if (response.getStatus() == codeInteger) {
                            final var succeeded = successfulResponses.incrementAndGet();
                            if (succeeded >= fraction.ack) {
                                result.complete(succeeded);
                            }
                        }
                    })
            .exceptionally(e -> futureErrorHandler(e, maxNumberOfExceptions, result));
        }

        try {
            if (result.get(TIMEOUT_CLUSTER, TimeUnit.MILLISECONDS) < fraction.ack) {
                return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
            } else {
                return new Response(codeString, Response.EMPTY);
            }
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            final var message = e.getMessage() == null ? e.getClass().getName() : e.getMessage();
            return new Response(Response.GATEWAY_TIMEOUT, message.getBytes(UTF_8));
        }
    }

    protected Void futureErrorHandler(final Throwable e,
                                      @NotNull final AtomicInteger maxNumberOfExceptions,
                                      @NotNull final CompletableFuture<Integer> result) {
        log.warning(String.format("%s:\n%s", Protocol.WARN_PROCESSOR, e.getMessage()));
        if (maxNumberOfExceptions.decrementAndGet() < 0) {
            result.completeExceptionally(new IOException("Too many exceptions"));
        }
        return null;
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
