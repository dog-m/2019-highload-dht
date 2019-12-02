package ru.mail.polis.service.dogm.processors;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.dogm.RocksDAO;
import ru.mail.polis.service.dogm.Topology;

/**
 * Common information for request processors.
 */
public class SharedInfo {
    public final RocksDAO dao;
    public final Topology topology;
    public final Bridges bridges;

    /**
     * Create a new shared information holder.
     * @param dao      DAO implementation
     * @param topology cluster topology
     * @param bridges  connection to other nodes in cluster
     */
    public SharedInfo(@NotNull final RocksDAO dao,
                      @NotNull final Topology topology,
                      @NotNull final Bridges bridges) {
        this.dao = dao;
        this.topology = topology;
        this.bridges = bridges;
    }
}
