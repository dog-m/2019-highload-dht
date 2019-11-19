package ru.mail.polis.service.dogm;

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
    BasicTopology(final Set<String> topology, final int myPort) {
        this.nodes = new ArrayList<>(topology);
        String meNode = null;
        final String myPortSignature = ":" + myPort;
        for (final String node : topology) {
            if (node.contains(myPortSignature)) {
                meNode = node;
                break;
            }
        }
        this.me = meNode;
    }

    private int getIndexFor(final Object o) {
        final var hash = o.hashCode();
        return (hash & Integer.MAX_VALUE) % nodes.size();
    }

    @Override
    public String primaryFor(final String id) {
        return nodes.get(getIndexFor(id));
    }

    @Override
    public List<String> all() {
        return nodes;
    }

    @Override
    public Boolean isMe(final String node) {
        return me.equals(node);
    }

    @Override
    public List<String> nodesFor(final String id, final int count) {
        final List<String> result = new ArrayList<>(count);

        for (int c = 0, nodeIndex = getIndexFor(id); c < count; c++, nodeIndex++) {
            final String node = nodes.get(nodeIndex % nodes.size());
            result.add(node);
        }

        return result;
    }
}
