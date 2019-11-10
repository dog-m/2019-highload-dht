package ru.mail.polis.service.dogm;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;

import one.nio.http.HttpException;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.ConnectionString;
import one.nio.http.HttpClient;
import one.nio.pool.PoolException;

/**
 * Bridging class for redirecting requests to other nodes in cluster.
 */
public class Bridges {
    private final Map<String, HttpClient> clients = new HashMap<>();

    /**
     * Constructor for redirector dictionary (Bridges).
     */
    public Bridges(final Topology topology) {
        for (final String point : topology.all()) {
            if (!topology.isMe(point)) {
                clients.put(point, new HttpClient(new ConnectionString(point + "?timeout=15")));
            }
        }
    }

    public Response sendRequestTo(final Request request, final String node)
            throws InterruptedException, IOException, HttpException, PoolException {
        return clients.get(node).invoke(request);
    }
}
