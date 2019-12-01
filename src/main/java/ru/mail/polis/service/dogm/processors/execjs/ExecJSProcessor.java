package ru.mail.polis.service.dogm.processors.execjs;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.dogm.RocksDAO;
import ru.mail.polis.service.dogm.Topology;
import ru.mail.polis.service.dogm.processors.Bridges;
import ru.mail.polis.service.dogm.processors.SimpleRequestProcessor;

import java.util.List;

public abstract class ExecJSProcessor extends SimpleRequestProcessor<String, ExecJSRequest> {
    public ExecJSProcessor(final RocksDAO dao, final Topology topology, final Bridges bridges) {
        super(dao, topology, bridges);
    }

    @NotNull
    @Override
    protected List<String> getNodesByRequest(@NotNull final Topology topology,
                                             @NotNull final ExecJSRequest request) {
        return topology.all();
    }
}
