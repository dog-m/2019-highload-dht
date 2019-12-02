package ru.mail.polis.service.dogm.processors.entity;

import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import ru.mail.polis.service.dogm.processors.SharedInfo;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Request processor for PUT method.
 */
public class EntityProcessorPut extends EntityProcessor<Void> {
    /**
     * Create a new PUT-processor.
     * @param sharedInfo Common information between processors
     */
    public EntityProcessorPut(@NotNull final SharedInfo sharedInfo) {
        super(sharedInfo);
    }

    @Nullable
    @Override
    protected Void getDataFromResponse(@NotNull final Response response) {
        return null;
    }

    @Override
    protected boolean isValid(@Nullable final Void data,
                              @NotNull final Response response) {
        return response.getStatus() == 201;
    }

    @Override
    @NotNull
    protected Response resolveClusterResponse(@NotNull final EntityRequest request,
                                              @NotNull final List<Void> responses) {
        return new Response(Response.CREATED, Response.EMPTY);
    }

    @Override
    @NotNull
    public Response processDirectly(@NotNull final EntityRequest request) {
        try {
            final var key = ByteBuffer.wrap(request.id.getBytes(UTF_8));
            final var val = ByteBuffer.wrap(request.raw.getBody());
            info.dao.upsertWithTimestamp(key, val);
            return new Response(Response.CREATED, Response.EMPTY);
        } catch (IOException e) {
            return new Response(Response.INTERNAL_ERROR, e.getMessage().getBytes(UTF_8));
        }
    }
}
