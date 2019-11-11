package ru.mail.polis.service.dogm.processors;

import one.nio.http.Request;
import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.dogm.RockException;
import ru.mail.polis.dao.dogm.RocksDAO;
import ru.mail.polis.service.dogm.Bridges;
import ru.mail.polis.service.dogm.ReplicasFraction;
import ru.mail.polis.service.dogm.Topology;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.logging.Logger;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.logging.Logger.getLogger;

/**
 * Request processor for PUT method.
 */
public class ProcessorPut extends SimpleRequestProcessor {
    private final Logger log = getLogger("ProcessorPut");

    /**
     * Create a new PUT-processor.
     * @param dao DAO implementation
     * @param topology cluster topology
     * @param bridges connection to other nodes in cluster
     */
    public ProcessorPut(final RocksDAO dao, final Topology topology, final Bridges bridges) {
        super(dao, topology, bridges);
    }

    @Override
    public Response processEntityRequest(@NotNull final String id,
                                         @NotNull final ReplicasFraction fraction,
                                         @NotNull final Request request) {
        int successfulResponses = 0;
        for (final var node : topology.nodesFor(id, fraction.from)) {
            try {
                final Response response =
                        topology.isMe(node)
                                ? processEntityDirectly(id, request)
                                : processEntityRemotely(node, request);
                if (response.getStatus() == 201) {
                    ++successfulResponses;
                }
            } catch (IOException e) {
                log.warning(Protocol.WARN_PROCESSOR);
            }
        }

        if (successfulResponses < fraction.ack) {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        } else {
            return new Response(Response.CREATED, Response.EMPTY);
        }
    }

    @Override
    public Response processEntityDirectly(@NotNull final String id,
                                          @NotNull final Request request) {
        try {
            final var key = ByteBuffer.wrap(id.getBytes(UTF_8));
            final var val = ByteBuffer.wrap(request.getBody());
            dao.upsertWithTimestamp(key, val);
            return new Response(Response.CREATED, Response.EMPTY);
        } catch (RockException e) {
            return new Response(Response.INTERNAL_ERROR, e.getMessage().getBytes(UTF_8));
        }
    }
}
