package ru.mail.polis.service.dogm.processors.entity;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.dogm.RocksDAO;
import ru.mail.polis.service.dogm.Topology;
import ru.mail.polis.service.dogm.processors.Bridges;
import ru.mail.polis.service.dogm.processors.SimpleRequestProcessor;

import java.util.List;

public abstract class EntityProcessor<DataType> extends SimpleRequestProcessor<DataType, EntityRequest> {
    public EntityProcessor(final RocksDAO dao, final Topology topology, final Bridges bridges) {
        super(dao, topology, bridges);
    }

    @NotNull
    @Override
    protected List<String> getNodesByRequest(@NotNull final Topology topology,
                                             @NotNull final EntityRequest request) {
        return topology.nodesFor(request.id, request.fraction.from);
    }
}
