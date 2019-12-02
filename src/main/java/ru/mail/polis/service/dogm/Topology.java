package ru.mail.polis.service.dogm;

import java.util.List;

public interface Topology {
    String primaryFor(final String id);

    List<String> all();

    boolean isMe(final String node);

    List<String> nodesFor(String id, int count);

    int size();
}
