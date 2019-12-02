package ru.mail.polis.service.dogm.processors.entity;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.service.dogm.Topology;
import ru.mail.polis.service.dogm.processors.SharedInfo;
import ru.mail.polis.service.dogm.processors.SimpleRequestProcessor;

import java.util.List;

public abstract class EntityProcessor<DataType> extends SimpleRequestProcessor<DataType, EntityRequest> {
    public EntityProcessor(@NotNull final SharedInfo sharedInfo) {
        super(sharedInfo);
    }

    @NotNull
    @Override
    protected List<String> getNodesByRequest(@NotNull final Topology topology,
                                             @NotNull final EntityRequest request) {
        return topology.nodesFor(request.id, request.fraction.from);
    }
}
