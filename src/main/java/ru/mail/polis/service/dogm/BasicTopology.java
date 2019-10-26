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

    @Override
    public String primaryFor(final String id) {
        final int hash = id.hashCode();
        final int node = (hash & Integer.MAX_VALUE) % nodes.size();
        return nodes.get(node);
    }

    @Override
    public List<String> all() {
        return nodes;
    }

    @Override
    public Boolean isMe(final String node) {
        return me.equals(node);
    }
}
