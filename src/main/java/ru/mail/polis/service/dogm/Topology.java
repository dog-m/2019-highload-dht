package ru.mail.polis.service.dogm;

import java.util.List;

public interface Topology {

    public String primaryFor(final String id);

    public List<String> all();

    public Boolean isMe(final String node);
}
