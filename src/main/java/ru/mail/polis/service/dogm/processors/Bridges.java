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
    public static final Duration TIMEOUT_REQUEST = Duration.ofMillis(500);
    public static final Duration TIMEOUT_CONNECT = Duration.ofSeconds(5);

    /**
     * Constructor for redirector dictionary (Bridges).
     */
    public Bridges(final Topology topology) {
        for (final String point : topology.all()) {
            if (!topology.isMe(point)) {
                final var clientBuilder = HttpClient.newBuilder()
                        .connectTimeout(TIMEOUT_CONNECT);
                clients.put(point, clientBuilder.build());
            }
        }
    }

    /**
     * Redirect request to another node in the cluster.
     * @param request source request
     * @param node target node
     * @return future response from node
     */
    public CompletableFuture<HttpResponse<byte[]>> sendRequestTo(final Request request,
                                                                 final String node) {
        var httpRequestBuilder = HttpRequest
                .newBuilder()
                .uri(URI.create(node + request.getURI()))
                .setHeader(Protocol.HEADER_FROM_CLUSTER, "1")
                .timeout(TIMEOUT_REQUEST);

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
