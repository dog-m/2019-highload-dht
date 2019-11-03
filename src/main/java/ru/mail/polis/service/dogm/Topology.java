package ru.mail.polis.service.dogm;

import java.nio.ByteBuffer;
import java.util.List;

public interface Topology {
    String primaryFor(final String id);

    List<String> all();

    boolean isMe(final String node);

    String[] replicas(final ByteBuffer id, final int count);

    String getMe();
}
