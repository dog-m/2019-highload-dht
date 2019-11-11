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
        final var successfulResponses = new ArrayList<DataWithTimestamp>(fraction.from);
        for (final var node : topology.nodesFor(id, fraction.from)) {
            try {
                final Response response =
                        topology.isMe(node)
                                ? processEntityDirectly(id, request)
                                : processEntityRemotely(node, request);
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
            return resolveSuitableGetResponse(successfulResponses);
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
            if (candidate.isRemoved()) {
                max = candidate;
                break;
            }
            else if (candidate.timestamp > max.timestamp && !candidate.isAbsent()) {
                max = candidate;
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
