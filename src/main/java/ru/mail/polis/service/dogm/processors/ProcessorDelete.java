package ru.mail.polis.service.dogm.processors;

import one.nio.http.Request;
import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
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
 * Request processor for DELETE method.
 */
public class ProcessorDelete extends SimpleRequestProcessor {
    private final Logger log = getLogger("ProcessorDelete");

    /**
     * Create a new DELETE-processor.
     * @param dao DAO implementation
     * @param topology cluster topology
     * @param bridges connection to other nodes in cluster
     */
    public ProcessorDelete(final RocksDAO dao, final Topology topology, final Bridges bridges) {
        super(dao, topology, bridges);
    }

    @Override
    public Response processEntityRequest(@NotNull final String id,
                                         @NotNull final ReplicasFraction fraction,
                                         @NotNull final Request request,
                                         final boolean proxied) {
        applyProxyHeader(request, proxied);

        final var nodes = topology.nodesFor(id, fraction.from);
        int successfulResponses = 0;
        for (final var node : nodes) {
            try {
                if (topology.isMe(node)) {
                    if (request.getMethod() == Request.METHOD_DELETE) {
                        if (delete(id).getStatus() != 202) {
                            continue;
                        }
                    } else {
                        return getWrongProcessorResponse();
                    }
                } else {
                    final var response = proxy(node, request);
                    if (response.getStatus() >= 500) {
                        continue;
                    }
                }

                ++successfulResponses;
            } catch (IOException e) {
                log.warning(Protocol.WARN_PROCESSOR);
            }
        }

        if (successfulResponses < fraction.ack) {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        } else {
            return new Response(Response.ACCEPTED, Response.EMPTY);
        }
    }

    private Response delete(final String id) throws IOException {
        final var key = ByteBuffer.wrap(id.getBytes(UTF_8));
        dao.removeWithTimestamp(key);
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }
}
