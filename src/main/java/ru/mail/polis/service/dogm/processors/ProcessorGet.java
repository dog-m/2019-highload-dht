package ru.mail.polis.service.dogm.processors;

import one.nio.http.Request;
import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.dogm.ByteBufferUtils;
import ru.mail.polis.dao.dogm.DataWithTimestamp;
import ru.mail.polis.dao.dogm.RocksDAO;
import ru.mail.polis.service.dogm.ReplicasFraction;
import ru.mail.polis.service.dogm.Topology;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Request processor for GET method.
 */
public class ProcessorGet extends SimpleRequestProcessor {
    /**
     * Create a new GET-processor.
     * @param dao DAO implementation
     * @param topology cluster topology
     * @param bridges connection to other nodes in cluster
     */
    public ProcessorGet(final RocksDAO dao, final Topology topology, final Bridges bridges) {
        super(dao, topology, bridges);
    }

    @Override
    public Response processEntityRequest(@NotNull final String id,
                                         @NotNull final ReplicasFraction fraction,
                                         @NotNull final Request request) {
        final var successfulResponses = new AtomicInteger(0);
        final var responses = new ArrayList<DataWithTimestamp>(fraction.from);
        final var maxNumberOfExceptions = new AtomicInteger(fraction.from - fraction.ack);
        final var result = new CompletableFuture<Integer>();

        final var nodes = topology.nodesFor(id, fraction.from);
        for (int i = 0; i < nodes.size(); i++) {
            responses.add(null); // will be replaced in future if succeed

            final var node = nodes.get(i);
            final var index = i;
            (topology.isMe(node)
                    ? CompletableFuture.supplyAsync(() -> processEntityDirectly(id, request))
                    : processEntityRemotely(node, request))
            .thenAccept(
                    response -> {
                        final DataWithTimestamp data = extractDataFromGetResponse(response);
                        if (data != null) {
                            responses.set(index, data); // replace NULL with data
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
                return resolveSuitableGetResponse(responses);
            }
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            final var message = e.getMessage() == null ? e.getClass().getName() : e.getMessage();
            return new Response(Response.GATEWAY_TIMEOUT, message.getBytes(UTF_8));
        }
    }

    @Override
    public Response processEntityDirectly(@NotNull final String id,
                                          @NotNull final Request request) {
        final var key = ByteBuffer.wrap(id.getBytes(UTF_8));
        final var val = dao.getWithTimestamp(key);

        if (val.isPresent()) {
            return new Response(Response.OK, val.toBytes());
        } else if (val.isRemoved()) {
            return new Response(Response.NOT_FOUND, val.toBytes());
        } else {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
    }

    private Response resolveSuitableGetResponse(@NotNull final List<DataWithTimestamp> responses) {
        DataWithTimestamp max = DataWithTimestamp.fromAbsent();
        for (final var candidate : responses) {
            // ignore NULLs
            if (candidate != null) {
                if (candidate.isRemoved()) {
                    max = candidate;
                    break;
                } else if (candidate.timestamp > max.timestamp && !candidate.isAbsent()) {
                    max = candidate;
                }
            }
        }

        try {
            if (max.isPresent()) {
                return new Response(Response.OK, ByteBufferUtils.getByteArray(max.getData()));
            } else {
                return new Response(Response.NOT_FOUND, Response.EMPTY);
            }
        } catch (IOException e) {
            return new Response(Response.INTERNAL_ERROR, e.getMessage().getBytes(UTF_8));
        }
    }

    private DataWithTimestamp extractDataFromGetResponse(@NotNull final Response response) {
        switch (response.getStatus()) {
            case 200:
                return DataWithTimestamp.fromBytes(response.getBody());

            case 404:
                if (response.getBody().length == 0) {
                    return DataWithTimestamp.fromAbsent();
                } else {
                    return DataWithTimestamp.fromBytes(response.getBody());
                }

            default:
                return null;
        }
    }
}
