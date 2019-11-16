package ru.mail.polis.service.dogm.processors;

import one.nio.http.Request;
import ru.mail.polis.service.dogm.Topology;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;

/**
 * Bridging class for redirecting requests to other nodes in cluster.
 */
public class Bridges {
    private final Logger log = Logger.getLogger(getClass().getName());
    private final Map<String, HttpClient> clients = new HashMap<>();
    public static final Duration TIMEOUT = Duration.ofMillis(500);

    /**
     * Constructor for redirector dictionary (Bridges).
     */
    public Bridges(final Topology topology) {
        for (final String point : topology.all()) {
            if (!topology.isMe(point)) {
                clients.put(point, HttpClient.newBuilder().build());
            }
        }
    }

    public CompletableFuture<HttpResponse<byte[]>> sendRequestTo(final Request request,
                                                                 final String node) {
        final var uri = URI.create(node + request.getURI());

        log.info("bridge URI: " + uri.toString());

        var httpRequestBuilder = HttpRequest
                .newBuilder()
                .uri(uri)
                .setHeader(Protocol.HEADER_FROM_CLUSTER_PREFIX, "1")
                .timeout(TIMEOUT);

        switch (request.getMethod()) {
            case Request.METHOD_GET:
                httpRequestBuilder = httpRequestBuilder.GET();
                break;

            case Request.METHOD_PUT:
                final var body = HttpRequest.BodyPublishers.ofByteArray(request.getBody());
                httpRequestBuilder = httpRequestBuilder.PUT(body);
                break;

            case Request.METHOD_DELETE:
                httpRequestBuilder = httpRequestBuilder.DELETE();
                break;

            default:
                log.severe("Unknown method used. Reinterpreted as GET");
                httpRequestBuilder = httpRequestBuilder.GET();
                break;
        }

        return clients.get(node).sendAsync(httpRequestBuilder.build(), HttpResponse.BodyHandlers.ofByteArray());
    }
}
