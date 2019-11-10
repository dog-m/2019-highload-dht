package ru.mail.polis.service.dogm;

import one.nio.http.HttpException;
import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;

import one.nio.net.Socket;
import one.nio.pool.PoolException;
import one.nio.server.AcceptorConfig;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.dogm.ByteBufferUtils;
import ru.mail.polis.dao.dogm.DataWithTimestamp;
import ru.mail.polis.dao.dogm.RocksDAO;
import ru.mail.polis.service.Service;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Simple REST/HTTP service.
 */
public class ServiceImpl extends HttpServer implements Service {
    private final RocksDAO dao;
    private final Executor myWorkers;
    private final Logger log = Logger.getLogger("HttpServer");
    private final Topology topology;
    private final int clusterSize;
    private final Bridges bridges;

    /**
     * Constructor of simple REST/HTTP service.
     */
    public ServiceImpl(final int port,
                       @NotNull final DAO dao,
                       final Executor workers,
                       @NotNull final Set<String> topology) throws IOException {
        super(getConfig(port));
        this.dao = (RocksDAO) dao;
        this.myWorkers = workers;
        this.topology = new BasicTopology(topology, port);
        this.clusterSize = topology.size();
        this.bridges = new Bridges(this.topology);
    }

    /**
     * Default configuration for simple REST/HTTP service.
     */
    private static HttpServerConfig getConfig(final int port) {
        if (port <= 1024 || port >= 66535) {
            throw new IllegalArgumentException(Protocol.FAIL_INVALID_PORT);
        }

        final var acceptor = new AcceptorConfig();
        final var config = new HttpServerConfig();
        acceptor.port = port;
        config.acceptors = new AcceptorConfig[]{ acceptor };
        return config;
    }

    /**
     * Main handler for requests to addresses like http://localhost:8080/v0/entity?id=key1[&replicas=ack/from].
     */
    @Path("/v0/entity")
    public void entity(@Param("id") final String id,
                       @Param("replicas") final String replicas,
                       @NotNull final Request request,
                       @NotNull final HttpSession session) throws IOException {
        if (id == null || id.isEmpty()) {
            session.sendError(Response.BAD_REQUEST, Protocol.FAIL_MISSING_ID);
            return;
        }

        final var fraction = request.getHeader(Protocol.HEADER_PROXIED) == null
                    ? ReplicasFraction.parse(replicas, clusterSize)
                    : ReplicasFraction.one();
        if (fraction.ack < 1 || fraction.ack > fraction.from || fraction.from > clusterSize) {
            session.sendError(Response.BAD_REQUEST, Protocol.FAIL_REPLICAS);
            return;
        }

        switch (request.getMethod()) {
            case Request.METHOD_GET:
            case Request.METHOD_PUT:
            case Request.METHOD_DELETE:
                executeAsync(session, () -> processEntityRequest(id, fraction, request));
                break;

            default:
                session.sendError(Response.METHOD_NOT_ALLOWED, "Wrong method");
                break;
        }
    }

    private Response processEntityRequest(@NotNull final String id,
                                          @NotNull final ReplicasFraction fraction,
                                          @NotNull final Request request) {
        request.addHeader(Protocol.HEADER_PROXIED);
        final var nodes = topology.nodesFor(id, fraction.from);
        final var responses = new ArrayList<Response>(nodes.size());
        for (final var node : nodes) {
            try {
                Response response = null;
                DataWithTimestamp data = null;

                if (topology.isMe(node)) {
                    switch (request.getMethod()) {
                        case Request.METHOD_GET:
                            response = get(id);
                            if (responseToDataWithTimestamp(response).isRemoved())
                                return new Response(Response.NOT_FOUND, Response.EMPTY);
                            break;

                        case Request.METHOD_PUT:
                            response = put(id, request.getBody());
                            if (response.getStatus() != 201) {
                                continue;
                            }
                            break;

                        case Request.METHOD_DELETE:
                            response = delete(id);
                            if (response.getStatus() != 202) {
                                continue;
                            }
                            break;
                    }
                } else {
                    response = proxy(node, request);
                }

                responses.add(response);
            } catch (IOException e) {
                log.severe(e.getMessage());
            }
        }

        if (responses.size() < fraction.ack) {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        } else {
            if (request.getMethod() == Request.METHOD_GET) {
                return getSuitableResponse(responses);
            } else {
                return responses.get(0);
            }
        }
    }

    private Response getSuitableResponse(@NotNull final List<Response> responses) {
        long maxTimestamp = -1;
        Response max = new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        for (final var candidate : responses) {
            final var candidateTimestamp = responseToDataWithTimestamp(candidate).timestamp;
            if (maxTimestamp < candidateTimestamp) {
                maxTimestamp = candidateTimestamp;
                max = candidate;
            }
        }
        return max;
    }

    private long tryParseLong(final String x) {
        if (x == null) {
            return -1;
        }

        try {
            return Long.parseLong(x);
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    private DataWithTimestamp responseToDataWithTimestamp(final Response res) {
        if (res == null)
            return DataWithTimestamp.fromAbsent();

        final var state = (byte)tryParseLong(res.getHeader(Protocol.HEADER_STATE));
        final var timestamp = tryParseLong(res.getHeader(Protocol.HEADER_TIMESTAMP));
        final var data = res.getBody();

        if (timestamp < 0) {
            return DataWithTimestamp.fromAbsent();
        } else {
            return new DataWithTimestamp(timestamp, ByteBuffer.wrap(data), DataWithTimestamp.State.fromValue(state));
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

    private Response proxy(final String node, final Request request) throws IOException {
        try {
            return bridges.sendRequestTo(request, node);
        } catch (InterruptedException | PoolException | HttpException e) {
            log.log(Level.SEVERE, Protocol.FAIL_PROXY, e);
            return new Response(Response.INTERNAL_ERROR, Protocol.FAIL_PROXY.getBytes(UTF_8));
        }
    }

    private Response dataWithTimestampToResponse(@NotNull final String code, @NotNull final DataWithTimestamp value) {
        Response res = new Response(code);
        res.addHeader(String.format(Protocol.HEADER_STATE_FORMAT, value.state.value));
        res.addHeader(String.format(Protocol.HEADER_TIMESTAMP_FORMAT, value.timestamp));
        if (value.isPresent()) {
            try {
                res.setBody(ByteBufferUtils.getByteArray(value.getData()));
            } catch (IOException e) {
                return new Response(Response.INTERNAL_ERROR, e.getMessage().getBytes(UTF_8));
            }
        }
        return res;
    }

    private Response get(final String id) throws IOException {
        try {
            final var key = ByteBuffer.wrap(id.getBytes(UTF_8));
            final var data = dao.getWithTimestamp(key);
            return dataWithTimestampToResponse(data.isPresent() ? Response.OK : Response.NOT_FOUND, data);
        } catch (NoSuchElementException ex) {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        }
    }

    private Response put(final String id, final byte[] value) throws IOException {
        final var key = ByteBuffer.wrap(id.getBytes(UTF_8));
        final var val = ByteBuffer.wrap(value);
        final var data = dao.upsertWithTimestamp(key, val);
        return dataWithTimestampToResponse(Response.CREATED, data);
    }

    private Response delete(final String id) throws IOException {
        final var key = ByteBuffer.wrap(id.getBytes(UTF_8));
        final var data = dao.removeWithTimestamp(key);
        return dataWithTimestampToResponse(Response.ACCEPTED, data);
    }

    private void sendError(final HttpSession session, final String code, final String data) {
        try {
            session.sendError(code, data);
        } catch (IOException e) {
            log.log(Level.SEVERE, Protocol.FAIL_ERROR_SEND, e);
        }
    }

    /**
     * Main handler for requests to addresses like http://localhost:8080/v0/entities?start=key1[&end=key99].
     */
    @Path("/v0/entities")
    public void entities(@Param("start") final String start,
                         @Param("end") final String end,
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

        myWorkers.execute(() -> {
            try {
                final var from = ByteBuffer.wrap(start.getBytes(UTF_8));
                final var to =
                        end == null || end.isEmpty()
                            ? null
                            : ByteBuffer.wrap(end.getBytes(UTF_8));
                final var records = dao.range(from, to);

                final var storageSession = (StorageSession) session;
                storageSession.stream(records);
            } catch (IOException e) {
                sendError(session, Response.INTERNAL_ERROR, e.getMessage());
            }
        });
    }

    @Override
    public HttpSession createSession(final Socket socket) {
        return new StorageSession(socket, this);
    }

    private void executeAsync(@NotNull final HttpSession session, @NotNull final Action action) {
        myWorkers.execute(() -> {
            try {
                session.sendResponse(action.act());
            } catch (IOException e) {
                sendError(session, Response.INTERNAL_ERROR, e.getMessage());
            }
        });
    }

    @FunctionalInterface
    interface Action {
        Response act() throws IOException;
    }
}
