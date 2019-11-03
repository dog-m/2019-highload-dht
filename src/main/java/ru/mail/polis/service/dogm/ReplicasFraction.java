package ru.mail.polis.service.dogm;

import com.google.common.base.Splitter;
import one.nio.http.HttpSession;
import one.nio.http.Response;

import java.io.IOException;
import java.util.List;

class ReplicasFraction {
    final int ack;
    final int from;

    ReplicasFraction(final int ack, final int from) {
        this.ack = ack;
        this.from = from;
    }

    private static ReplicasFraction parse(final String url) {
        final String source = url.replace("=", "");
        final List<String> values = Splitter.on('/').splitToList(source);
        if (values.size() != 2) {
            throw new IllegalArgumentException(url);
        }
        return new ReplicasFraction(Integer.parseInt(values.get(0)), Integer.parseInt(values.get(1)));
    }

    static ReplicasFraction tryParse(final String replicas,
                                     final HttpSession session,
                                     final ReplicasFraction fraction,
                                     final int size) throws IOException {
        ReplicasFraction newFraction = null;
        try {
            if (replicas == null) {
                newFraction = fraction;
            } else {
                newFraction = ReplicasFraction.parse(replicas);
            }
            if (newFraction.ack < 1 || newFraction.from < newFraction.ack || newFraction.from > size) {
                throw new IllegalArgumentException("big from");
            }
            return newFraction;
        } catch (IllegalArgumentException e) {
            session.sendError(Response.BAD_REQUEST, "ReplicasFractionParser is wrong");
        }
        return newFraction;
    }
}
