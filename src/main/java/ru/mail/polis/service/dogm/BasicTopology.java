package ru.mail.polis.service.dogm;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class BasicTopology implements Topology {

    private final List<String> nodes;
    private final String me;

    public BasicTopology(final Set<String> nodes, final int me) {
        this.nodes = new ArrayList<>(nodes);
        String meNode = null;
        final String portSignature = ":" + me;
        for(final String node : nodes)
            if(node.contains(portSignature)) {
                meNode = node;
                break;
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
