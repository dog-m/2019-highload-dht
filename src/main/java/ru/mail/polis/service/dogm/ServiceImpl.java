package ru.mail.polis.service.dogm;

import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.Socket;
import one.nio.server.AcceptorConfig;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.dogm.RocksDAO;
import ru.mail.polis.service.Service;
import ru.mail.polis.service.dogm.processors.Bridges;
import ru.mail.polis.service.dogm.processors.ProcessorDelete;
import ru.mail.polis.service.dogm.processors.ProcessorGet;
import ru.mail.polis.service.dogm.processors.ProcessorPut;
import ru.mail.polis.service.dogm.processors.Protocol;
import ru.mail.polis.service.dogm.processors.SimpleRequestProcessor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
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
    private final int clusterSize;
    private final Map<Integer, SimpleRequestProcessor> processors;

    /**
     * Create a new server (node in cluster).
     * @param port server port
     * @param dao data storage
     * @param workers worker pool to work asynchronously
     * @param topologyDescription cluster topology
     */
    public ServiceImpl(final int port,
                       @NotNull final DAO dao,
                       final Executor workers,
                       @NotNull final Set<String> topologyDescription) throws IOException {
        super(getConfig(port));
        this.dao = (RocksDAO) dao;
        this.myWorkers = workers;
        this.clusterSize = topologyDescription.size();
        final Topology topology = new BasicTopology(topologyDescription, port);
        final Bridges bridges = new Bridges(topology);

        this.processors = new HashMap<>();
        this.processors.put(Request.METHOD_GET, new ProcessorGet(this.dao, topology, bridges));
        this.processors.put(Request.METHOD_PUT, new ProcessorPut(this.dao, topology, bridges));
        this.processors.put(Request.METHOD_DELETE, new ProcessorDelete(this.dao, topology, bridges));
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

        final boolean fromCluster = request.getHeader(Protocol.HEADER_FROM_CLUSTER) != null;
        final var fraction = fromCluster
                                ? ReplicasFraction.one()
                                : ReplicasFraction.parse(replicas, clusterSize);
        if (fraction.ack < 1 || fraction.ack > fraction.from || fraction.from > clusterSize) {
            session.sendError(Response.BAD_REQUEST, Protocol.FAIL_REPLICAS);
            return;
        }

        final var method = request.getMethod();
        switch (method) {
            case Request.METHOD_GET:
            case Request.METHOD_PUT:
            case Request.METHOD_DELETE:
                executeAsync(session, () -> processEntityRequest(id, fraction, request, fromCluster));
                break;

            default:
                session.sendError(Response.METHOD_NOT_ALLOWED, Protocol.FAIL_METHOD);
                break;
        }
    }

    private Response processEntityRequest(@NotNull final String id,
                                          @NotNull final ReplicasFraction fraction,
                                          @NotNull final Request request,
                                          final boolean fromCluster) {
        final var method = request.getMethod();
        if (fromCluster) {
            return processors.get(method).processEntityDirectly(id, request);
        } else {
            request.addHeader(Protocol.HEADER_FROM_CLUSTER);
            return processors.get(method).processEntityRequest(id, fraction, request);
        }
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
            sendError(session, Response.METHOD_NOT_ALLOWED, Protocol.FAIL_METHOD);
            return;
        }

        myWorkers.execute(() -> {
            try {
                final var from = ByteBuffer.wrap(start.getBytes(UTF_8));
                final var to = end == null || end.isEmpty()
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
