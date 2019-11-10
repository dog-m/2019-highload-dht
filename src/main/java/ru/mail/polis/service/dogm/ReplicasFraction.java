package ru.mail.polis.service.dogm;

import java.io.Console;

class ReplicasFraction {
    final int ack, from;

    private ReplicasFraction(final int ack, final int from) {
        this.ack = ack;
        this.from = from;
    }

    public static ReplicasFraction one() {
        return new ReplicasFraction(1, 1);
    }

    private static ReplicasFraction quorumSized(final int clusterSize) {
        return new ReplicasFraction(quorum(clusterSize), clusterSize);
    }

    private static int quorum(final int x) {
        return x / 2 + 1;
    }

    static ReplicasFraction parse(final String replicas, final int clusterSize) {
        if (replicas == null || replicas.isEmpty()) {
            return quorumSized(clusterSize);
        }

        final var cleanedReplicas = replicas.replace("=", "");
        final var pos = cleanedReplicas.indexOf("/");
        final var ack = cleanedReplicas.substring(0, pos);
        final var from = cleanedReplicas.substring(pos + 1);
        try {
            return new ReplicasFraction(Integer.parseInt(ack), Integer.parseInt(from));
        } catch (NumberFormatException e) {
            return quorumSized(clusterSize);
        }
    }
}
