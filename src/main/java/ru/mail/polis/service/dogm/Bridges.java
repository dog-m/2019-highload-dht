package ru.mail.polis.service.dogm;

import java.util.Map;
import java.util.HashMap;

import one.nio.net.ConnectionString;
import one.nio.http.HttpClient;

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
                clients.put(point, new HttpClient(new ConnectionString(point + "/?timeout=100")));
            }
        }
    }

    public HttpClient getBridgeTo(final String node) {
        return clients.get(node);
    }
}
