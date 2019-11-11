package ru.mail.polis.service.dogm.processors;

import one.nio.http.Request;
import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.dogm.ByteBufferUtils;
import ru.mail.polis.dao.dogm.DataWithTimestamp;
import ru.mail.polis.dao.dogm.RocksDAO;
import ru.mail.polis.service.dogm.Bridges;
import ru.mail.polis.service.dogm.ReplicasFraction;
import ru.mail.polis.service.dogm.Topology;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.logging.Logger;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.logging.Logger.getLogger;

/**
 * Request processor for GET method.
 */
public class ProcessorGet extends SimpleRequestProcessor {
    private final Logger log = getLogger("ProcessorGet");

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
                                         @NotNull final Request request,
                                         final boolean proxied) {
        applyProxyHeader(request, proxied);

        final var nodes = topology.nodesFor(id, fraction.from);
        final var successfulResponses = new ArrayList<DataWithTimestamp>(nodes.size());
        for (final var node : nodes) {
            try {
                final Response response = topology.isMe(node)
                                            ? get(id)
                                            : proxy(node, request);
                final DataWithTimestamp data = extractDataFromGetResponse(response);
                if (data != null) {
                    successfulResponses.add(data);
                }
            } catch (IOException e) {
                log.warning(Protocol.WARN_PROCESSOR);
            }
        }

        if (successfulResponses.size() < fraction.ack) {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        } else {
            return resolveSuitableGetResponse(successfulResponses, proxied);
        }
    }

    private Response resolveSuitableGetResponse(@NotNull final List<DataWithTimestamp> responses,
                                                final boolean proxied) {
        DataWithTimestamp max = DataWithTimestamp.fromAbsent();
        for (final var candidate : responses) {
            if (candidate.isRemoved()) {
                max = candidate; // keep removed
            }
            else if (candidate.timestamp > max.timestamp && !candidate.isAbsent()) {
                max = candidate;
            }
        }

        if (max.isPresent()) {
            try {
                return proxied
                        ? new Response(Response.OK, max.toBytes())
                        : new Response(Response.OK, ByteBufferUtils.getByteArray(max.getData()));
            } catch (IOException e) {
                return new Response(Response.INTERNAL_ERROR, e.getMessage().getBytes(UTF_8));
            }
        } else if (max.isRemoved()) {
            return new Response(Response.NOT_FOUND, max.toBytes());
        } else {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
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

    private Response get(final String id) throws IOException {
        try {
            final var key = ByteBuffer.wrap(id.getBytes(UTF_8));
            final var val = dao.getWithTimestamp(key);

            if (val.isRemoved()) {
                return new Response(Response.NOT_FOUND, val.toBytes());
            } else {
                return new Response(Response.OK, val.toBytes());
            }
        } catch (NoSuchElementException ex) {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
    }
}
