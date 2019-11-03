package ru.mail.polis.service.dogm;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Basic topology class.
 */
public class BasicTopology implements Topology {

    private final List<String> nodes;
    private final String me;

    /**
     * Basic topology class.
     *
     * @param topology - set of nodes in cluster
     * @param myPort   - port of current node
     */
    public BasicTopology(final Set<String> topology, final int myPort) {
        this.nodes = new ArrayList<>(topology);
        String meNode = null;
        final String myPortSignature = ":" + myPort;
        for(final String node : topology) {
            if(node.contains(myPortSignature)) {
                meNode = node;
                break;
            }
        }
        this.me = meNode;
    }

    protected int identifierToIndex(final String id) {
        return hashToIndex(id.hashCode());
    }

    protected int identifierToIndex(final ByteBuffer id) {
        return hashToIndex(id.hashCode());
    }

    protected int hashToIndex(final int hash) {
        return (hash & Integer.MAX_VALUE) % nodes.size();
    }

    @Override
    public String primaryFor(final String id) {
        final int node = identifierToIndex(id);
        return nodes.get(node);
    }

    @Override
    public List<String> all() {
        return nodes;
    }

    @Override
    public boolean isMe(final String node) {
        return me.equals(node);
    }

    @Override
    public String getMe() {
        return me;
    }

    @Override
    public String[] replicas(final ByteBuffer id, final int count) {
        final var result = new String[count];
        for (int j = 0, i = identifierToIndex(id); j < count; j++, i++) {
            result[j] = nodes.get(i % nodes.size());
        }
        return result;
    }
}
