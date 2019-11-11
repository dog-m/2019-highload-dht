package ru.mail.polis.service.dogm.processors;

import one.nio.http.Request;
import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.dogm.RockException;
import ru.mail.polis.dao.dogm.RocksDAO;
import ru.mail.polis.service.dogm.Bridges;
import ru.mail.polis.service.dogm.ReplicasFraction;
import ru.mail.polis.service.dogm.Topology;

import java.nio.ByteBuffer;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Request processor for PUT method.
 */
public class ProcessorPut extends SimpleRequestProcessor {
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
        final var codeString = Response.CREATED;
        final var codeInteger = 201;
        return processEntityRequestOnClusterEmptyResult(
                id, fraction, request, codeString, codeInteger);
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
