package ru.mail.polis.service.dogm.processors.execjs;

import one.nio.http.Request;
import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.dogm.RocksDAO;
import ru.mail.polis.service.dogm.ReplicasFraction;
import ru.mail.polis.service.dogm.Topology;
import ru.mail.polis.service.dogm.processors.Bridges;
import ru.mail.polis.service.dogm.processors.SimpleRequestProcessor;

public class ProcessorPost extends SimpleRequestProcessor {
    public ProcessorPost(RocksDAO dao, Topology topology, Bridges bridges) {
        super(dao, topology, bridges);
    }

    @Override
    public Response processAsCluster(@NotNull final String id,
                                     @NotNull final ReplicasFraction fraction,
                                     @NotNull final Request request) {
        return null;
    }

    @Override
    public Response processDirectly(@NotNull final String id,
                                    @NotNull final Request request) {
        return null;
    }
}
