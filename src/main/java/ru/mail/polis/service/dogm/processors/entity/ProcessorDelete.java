package ru.mail.polis.service.dogm.processors.entity;

import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.mail.polis.dao.dogm.RocksDAO;
import ru.mail.polis.service.dogm.Topology;
import ru.mail.polis.service.dogm.processors.Bridges;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Request processor for DELETE method.
 */
public class ProcessorDelete extends EntityProcessor<Void> {
    /**
     * Create a new DELETE-processor.
     * @param dao DAO implementation
     * @param topology cluster topology
     * @param bridges connection to other nodes in cluster
     */
    public ProcessorDelete(final RocksDAO dao, final Topology topology, final Bridges bridges) {
        super(dao, topology, bridges);
    }

    @Nullable
    @Override
    protected Void getDataFromResponse(@NotNull final Response response) {
        return null;
    }

    @Override
    protected boolean isValid(@Nullable final Void data,
                              @NotNull final Response response) {
        return response.getStatus() == 202;
    }

    @Override
    protected Response resolveClusterResponse(@NotNull final EntityRequest request,
                                              @NotNull final List<Void> responses) {
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }

    @Override
    public Response processDirectly(@NotNull final EntityRequest request) {
        try {
            final var key = ByteBuffer.wrap(request.id.getBytes(UTF_8));
            dao.removeWithTimestamp(key);
            return new Response(Response.ACCEPTED, Response.EMPTY);
        } catch (IOException e) {
            return new Response(Response.INTERNAL_ERROR, e.getMessage().getBytes(UTF_8));
        }
    }
}
