package ru.mail.polis.service.dogm.processors.entity;

import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.mail.polis.dao.dogm.ByteBufferUtils;
import ru.mail.polis.dao.dogm.DataWithTimestamp;
import ru.mail.polis.dao.dogm.RocksDAO;
import ru.mail.polis.service.dogm.Topology;
import ru.mail.polis.service.dogm.processors.Bridges;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Request processor for GET method.
 */
public class ProcessorGet extends EntityProcessor<DataWithTimestamp> {
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
    public Response processDirectly(@NotNull final EntityRequest request) {
        final var key = ByteBuffer.wrap(request.id.getBytes(UTF_8));
        final var val = dao.getWithTimestamp(key);

        if (val.isPresent()) {
            return new Response(Response.OK, val.toBytes());
        } else if (val.isRemoved()) {
            return new Response(Response.NOT_FOUND, val.toBytes());
        } else {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
    }

    @Override
    protected Response resolveClusterResponse(@NotNull final EntityRequest request,
                                              @NotNull final List<DataWithTimestamp> responses) {
        var max = DataWithTimestamp.fromAbsent();
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

    @Override
    protected boolean isValid(@Nullable final DataWithTimestamp data,
                              @NotNull final Response response) {
        return data != null;
    }

    @Override
    @Nullable
    protected DataWithTimestamp getDataFromResponse(@NotNull final Response response) {
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
