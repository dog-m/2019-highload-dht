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
import ru.mail.polis.service.dogm.processors.Protocol;
import ru.mail.polis.service.dogm.processors.SharedInfo;
import ru.mail.polis.service.dogm.processors.entity.EntityProcessor;
import ru.mail.polis.service.dogm.processors.entity.EntityRequest;
import ru.mail.polis.service.dogm.processors.entity.EntityProcessorDelete;
import ru.mail.polis.service.dogm.processors.entity.EntityProcessorGet;
import ru.mail.polis.service.dogm.processors.entity.EntityProcessorPut;
import ru.mail.polis.service.dogm.processors.execjs.ExecJSProcessor;
import ru.mail.polis.service.dogm.processors.execjs.ExecJSRequest;
import ru.mail.polis.service.dogm.processors.execjs.ExecJSProcessorPost;

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
    private final Executor myWorkers;
    private final Logger log = Logger.getLogger(getClass().getName());
    private final SharedInfo sharedInfo;
    private final Map<Integer, EntityProcessor<?>> entityProcs;
    private final ExecJSProcessor jsProc;

    /**
     * Create a new server (node in cluster).
     * @param port server port
     * @param dao data storage
     * @param workers worker pool to work asynchronously
     * @param topologyDescription cluster topology
     */
    public ServiceImpl(final int port,
                       @NotNull final DAO dao,
                       @NotNull final Executor workers,
                       @NotNull final Set<String> topologyDescription) throws IOException {
        super(getConfig(port));
        this.myWorkers = workers;
        final Topology topology = new BasicTopology(topologyDescription, port);
        this.sharedInfo = new SharedInfo((RocksDAO) dao, topology, new Bridges(topology));

        this.entityProcs = new HashMap<>();
        this.entityProcs.put(Request.METHOD_GET, new EntityProcessorGet(sharedInfo));
        this.entityProcs.put(Request.METHOD_PUT, new EntityProcessorPut(sharedInfo));
        this.entityProcs.put(Request.METHOD_DELETE, new EntityProcessorDelete(sharedInfo));

        this.jsProc = new ExecJSProcessorPost(sharedInfo);
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
    public void handleDefault(@NotNull final Request request,
                              @NotNull final HttpSession session) throws IOException {
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
                                : ReplicasFraction.parse(replicas, sharedInfo.topology.size());
        if (fraction.ack < 1
                || fraction.ack > fraction.from
                || fraction.from > sharedInfo.topology.size()) {
            session.sendError(Response.BAD_REQUEST, Protocol.FAIL_REPLICAS);
            return;
        }

        final var compoundRequest = new EntityRequest(id, fromCluster, request, fraction);
        final var method = request.getMethod();
        switch (method) {
            case Request.METHOD_GET:
            case Request.METHOD_PUT:
            case Request.METHOD_DELETE:
                executeAsync(session, () -> processEntityRequest(compoundRequest));
                break;

            default:
                session.sendError(Response.METHOD_NOT_ALLOWED, Protocol.FAIL_METHOD);
                break;
        }
    }

    private Response processEntityRequest(@NotNull final EntityRequest request) {
        final var method = request.raw.getMethod();
        if (request.fromCluster) {
            return entityProcs.get(method).processDirectly(request);
        } else {
            request.raw.addHeader(Protocol.HEADER_FROM_CLUSTER);
            return entityProcs.get(method).processAsCluster(request);
        }
    }

    private void sendError(@NotNull final HttpSession session,
                           @NotNull final String code,
                           @NotNull final String data) {
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
                         @NotNull final HttpSession session) {
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
                final var records = sharedInfo.dao.range(from, to);

                final var storageSession = (StorageSession) session;
                storageSession.stream(records);
            } catch (IOException e) {
                sendError(session, Response.INTERNAL_ERROR, e.getMessage());
            }
        });
    }

    @Override
    public HttpSession createSession(@NotNull final Socket socket) {
        return new StorageSession(socket, this);
    }

    private void executeAsync(@NotNull final HttpSession session,
                              @NotNull final Action action) {
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

    /**
     * Main handler for POST requests to addresses like http://localhost:8080/v0/execjs.
     */
    @Path("/v0/execjs")
    public void execjs(@NotNull final Request request,
                       @NotNull final HttpSession session) {
        if (request.getMethod() != Request.METHOD_POST) {
            sendError(session, Response.METHOD_NOT_ALLOWED, "Use POST method instead");
            return;
        }

        final var source = request.getBody();
        if (source == null || source.length == 0) {
            sendError(session, Response.BAD_REQUEST, "No any JavaScript code");
            return;
        }

        final var js = new String(request.getBody(), UTF_8);
        final boolean fromCluster = request.getHeader(Protocol.HEADER_FROM_CLUSTER) != null;
        final var fraction = ReplicasFraction.all(sharedInfo.topology.size());

        final var compoundRequest = new ExecJSRequest(js, fromCluster, request, fraction);
        executeAsync(session, () -> processExecJSRequest(compoundRequest));
    }

    private Response processExecJSRequest(@NotNull final ExecJSRequest request) {
        if (request.fromCluster) {
            return jsProc.processDirectly(request);
        } else {
            request.raw.addHeader(Protocol.HEADER_FROM_CLUSTER);
            return jsProc.processAsCluster(request);
        }
    }
}
