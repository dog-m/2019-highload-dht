package ru.mail.polis.dao.dogm;

import java.io.IOException;
import java.nio.ByteBuffer;

public final class DataWithTimestamp {
    public final State state;
    public final long timestamp;
    private final ByteBuffer data;

    public enum State {
        ABSENT((byte) 0),
        PRESENT((byte) 1),
        REMOVED((byte) -1);

        public final byte value;

        State(final byte value) {
            this.value = value;
        }

        public static State fromValue(final byte value) {
            if (value == REMOVED.value) {
                return REMOVED;
            } else if (value == PRESENT.value) {
                return PRESENT;
            } else {
                return ABSENT;
            }
        }
    }

    public boolean isAbsent() {
        return state == State.ABSENT;
    }

    public boolean isPresent() {
        return state == State.PRESENT;
    }

    public boolean isRemoved() {
        return state == State.REMOVED;
    }

    public DataWithTimestamp(final long timestamp, final ByteBuffer data, final State state) {
        this.timestamp = timestamp;
        this.state = state;
        this.data = data;
    }

    public static DataWithTimestamp fromPresent(final ByteBuffer data, final long timestamp) {
        return new DataWithTimestamp(timestamp, data, State.PRESENT);
    }

    public static DataWithTimestamp fromRemovedAt(final long timestamp) {
        return new DataWithTimestamp(timestamp, null, State.REMOVED);
    }

    public static DataWithTimestamp fromAbsent() {
        return new DataWithTimestamp(-1, null, State.ABSENT);
    }

    public ByteBuffer getData() throws IOException {
        if (!isPresent()) {
            throw new IOException("There are no data");
        }
        return data;
    }

    /**
     * Restore object from an array of bytes.
     */
    public static DataWithTimestamp fromBytes(final byte[] bytes) {
        if (bytes == null) {
            return fromAbsent();
        }

        final var buffer = ByteBuffer.wrap(bytes);
        final var recordState = State.fromValue(buffer.get());
        return new DataWithTimestamp(buffer.getLong(), buffer, recordState);
    }

    /**
     * Save object to bytes.
     */
    public byte[] toBytes() {
        int size = 0;
        size += 1; // state
        size += Long.BYTES; // timestamp
        size += isPresent() // actual data
                    ? data.remaining()
                    : 0;

        final var buffer = ByteBuffer.allocate(size);
        buffer.put(state.value);
        buffer.putLong(timestamp);
        if (isPresent()) {
            buffer.put(data.duplicate());
        }

        return buffer.array();
    }
}
