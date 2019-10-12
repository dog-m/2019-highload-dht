package ru.mail.polis.service.dog_m;

import one.nio.http.*;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.service.Service;
import org.jetbrains.annotations.NotNull;
import com.google.common.base.Charsets;
import one.nio.http.HttpServerConfig;
import one.nio.server.AcceptorConfig;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

/**
 * Simple REST/HTTP service
 */
public class ServiceImpl extends HttpServer implements Service {
    private final DAO dao;

    public ServiceImpl(final int port, @NotNull final DAO dao) throws IOException {
        super(getConfig(port));
        this.dao = dao;
    }

    private static HttpServerConfig getConfig(final int port) {
        if (port <= 1024 || port >= 66535) {
            throw new IllegalArgumentException("Invalid port");
        }

        AcceptorConfig acceptor = new AcceptorConfig();
        acceptor.port = port;
        HttpServerConfig config = new HttpServerConfig();
        config.acceptors = new AcceptorConfig[]{ acceptor };
        return config;
    }

    @Path("/v0/entity")
    public Response entity(@Param("id") final String id, @NotNull final Request request) {
        try {
            if (id == null || id.isEmpty()) {
                return new Response(Response.BAD_REQUEST, Response.EMPTY);
            }

            final var key = ByteBuffer.wrap(id.getBytes(Charsets.UTF_8));
            switch (request.getMethod()) {
                case Request.METHOD_GET:
                    return get(key);

                case Request.METHOD_PUT:
                    return put(key, request);

                case Request.METHOD_DELETE:
                    return delete(key);

                default:
                    return new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY);
            }
            
        } catch (Exception ex) {
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }
    }

    @Override
    public void handleDefault(final Request request, final HttpSession session) throws IOException {
        session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
    }

    @Path("/v0/status")
    public Response status() {
        return new Response(Response.OK, Response.EMPTY);
    }

    private Response get(final ByteBuffer key) throws IOException {
        try {
            final var value = dao.get(key).duplicate();
            return new Response(Response.OK, value.array());
        } catch (NoSuchElementException ex) {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
    }

    private Response put(final ByteBuffer key, final Request request) throws IOException {
        final var value = ByteBuffer.wrap(request.getBody());
        dao.upsert(key, value);
        return new Response(Response.CREATED, Response.EMPTY);
    }

    private Response delete(final ByteBuffer key) throws IOException {
        dao.remove(key);
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }
}
