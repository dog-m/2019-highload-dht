package ru.mail.polis.service.dogm;

import one.nio.http.HttpServer;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.HttpSession;
import one.nio.http.HttpServerConfig;
import one.nio.net.Socket;
import one.nio.server.AcceptorConfig;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.ByteBufferUtils;
import ru.mail.polis.service.Service;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import java.util.concurrent.Executor;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Simple REST/HTTP service.
 */
public class ServiceImpl extends HttpServer implements Service {
    private final DAO dao;
    private Executor myWorkers;
    private Logger log = Logger.getLogger("HttpServer");
    private static final String EXTRA_FAILURE = "Something went wrong";

    /**
     * Constructor of simple REST/HTTP service.
     */
    public ServiceImpl(final int port,
                       @NotNull final DAO dao,
                       final Executor workers) throws IOException {
        super(getConfig(port));
        this.dao = dao;
        this.myWorkers = workers;
    }

    /**
     * Default configuration for simple REST/HTTP service.
     */
    private static HttpServerConfig getConfig(final int port) {
        if (port <= 1024 || port >= 66535) {
            throw new IllegalArgumentException("Invalid port");
        }

        final var acceptor = new AcceptorConfig();
        final var config = new HttpServerConfig();
        acceptor.port = port;
        config.acceptors = new AcceptorConfig[]{ acceptor };
        return config;
    }

    /**
     * Main handler for requests to addresses like http://localhost:8080/v0/entity?id=key1.
     */
    @Path("/v0/entity")
    public void entity(@Param("id") final String id,
                       @NotNull final Request request,
                       @NotNull final HttpSession session) throws IOException {
        if (id == null || id.isEmpty()) {
            session.sendError(Response.BAD_REQUEST, "No id");
            return;
        }

        try {
            switch (request.getMethod()) {
                case Request.METHOD_GET:
                    executeAsync(session, () -> get(id));
                    return;

                case Request.METHOD_PUT:
                    executeAsync(session, () -> put(id, request.getBody()));
                    return;

                case Request.METHOD_DELETE:
                    executeAsync(session, () -> delete(id));
                    return;

                default:
                    session.sendError(Response.METHOD_NOT_ALLOWED, "Wrong method");
            }
        } catch (Exception e) {
            session.sendError(Response.INTERNAL_ERROR, e.getMessage());
        }
    }

    @Override
    public void handleDefault(final Request request, final HttpSession session) throws IOException {
        session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
    }

    /**
     * Just 'system' status handle.
     */
    @Path("/v0/status")
    public Response status() {
        return new Response(Response.OK, Response.EMPTY);
    }

    private Response get(final String id) throws IOException {
        try {
            final var key = ByteBuffer.wrap(id.getBytes(UTF_8));
            final var value = ByteBufferUtils.getByteArray(dao.get(key));
            return new Response(Response.OK, value);
        } catch (NoSuchElementException ex) {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
    }

    private Response put(final String id, final byte[] value) throws IOException {
        final var key = ByteBuffer.wrap(id.getBytes(UTF_8));
        final var val = ByteBuffer.wrap(value);
        dao.upsert(key, val);
        return new Response(Response.CREATED, Response.EMPTY);
    }

    private Response delete(final String id) throws IOException {
        final var key = ByteBuffer.wrap(id.getBytes(UTF_8));
        dao.remove(key);
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }

    void sendError(final HttpSession session, final String code, final String data) {
        try {
            session.sendError(code, data);
        } catch (IOException e) {
            log.log(Level.SEVERE, EXTRA_FAILURE, e);
        }
    }

    /**
     * Main handler for requests to addresses like http://localhost:8080/v0/entities?start=key1[&end=key99].
     */
    @Path("/v0/entities")
    public void entities(@Param("start") final String start,
                         @Param("end") String end,
                         @NotNull final Request request,
                         final HttpSession session) {
        if (start == null || start.isEmpty()) {
            sendError(session, Response.BAD_REQUEST, "No start");
            return;
        }

        if (request.getMethod() != Request.METHOD_GET) {
            sendError(session, Response.METHOD_NOT_ALLOWED, "Wrong method");
            return;
        }

        if (end != null && end.isEmpty()) {
            end = null;
        }

        final String finalEnd = end;
        myWorkers.execute(() -> {
            try {
                final var from = ByteBuffer.wrap(start.getBytes(UTF_8));
                final var to = finalEnd == null ? null : ByteBuffer.wrap(finalEnd.getBytes(UTF_8));
                final var records = dao.range(from, to);

                final var storageSession = (StorageSession) session;
                storageSession.stream(records);
            } catch (IOException e) {
                try {
                    session.sendError(Response.INTERNAL_ERROR, e.getMessage());
                } catch (IOException ex) {
                    log.log(Level.SEVERE, EXTRA_FAILURE, ex);
                }
            }
        });
    }

    @Override
    public HttpSession createSession(final Socket socket) {
        return new StorageSession(socket, this);
    }

    private void executeAsync(
            @NotNull final HttpSession session,
            @NotNull final Action action) {
        myWorkers.execute(() -> {
            try {
                session.sendResponse(action.act());
            } catch (Exception e) {
                try {
                    session.sendError(Response.INTERNAL_ERROR, e.getMessage());
                } catch (IOException ex) {
                    log.log(Level.SEVERE, EXTRA_FAILURE, ex);
                }
            }
        });
    }

    @FunctionalInterface
    interface Action {
        Response act() throws IOException;
    }
}
