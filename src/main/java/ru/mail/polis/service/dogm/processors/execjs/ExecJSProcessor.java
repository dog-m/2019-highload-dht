package ru.mail.polis.service.dogm.processors.execjs;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.service.dogm.Topology;
import ru.mail.polis.service.dogm.processors.SharedInfo;
import ru.mail.polis.service.dogm.processors.SimpleRequestProcessor;

import java.util.List;

public abstract class ExecJSProcessor extends SimpleRequestProcessor<String, ExecJSRequest> {
    public ExecJSProcessor(@NotNull final SharedInfo sharedInfo) {
        super(sharedInfo);
    }

    @NotNull
    @Override
    protected List<String> getNodesByRequest(@NotNull final Topology topology,
                                             @NotNull final ExecJSRequest request) {
        return topology.all();
    }
}
