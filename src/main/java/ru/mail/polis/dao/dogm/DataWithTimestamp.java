package ru.mail.polis.dao.dogm;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;

public final class DataWithTimestamp {
    public final State state;
    public final long timestamp;
    private final ByteBuffer data;

    enum State {
        ABSENT((byte) 0),
        PRESENT((byte) 1),
        REMOVED((byte) -1);

        final byte value;

        State(final byte value) {
            this.value = value;
        }

        static State fromValue(final byte value) {
            switch (value) {
              case REMOVED.value:
                  return REMOVED;
                  break;

              case PRESENT.value:
                  return PRESENT;
                  break;

              default:
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

    private DataWithTimestamp(final long timestamp, final ByteBuffer data, final State state) {
        this.timestamp = timestamp;
        this.state = state;
        this.data = data;
    }

    static DataWithTimestamp fromPresent(final ByteBuffer data, final long timestamp) {
        return new DataWithTimestamp(timestamp, data, State.PRESENT);
    }

    static DataWithTimestamp fromRemovedAt(final long timestamp) {
        return new DataWithTimestamp(timestamp, null, State.REMOVED);
    }

    public static DataWithTimestamp fromAbsent() {
        return new DataWithTimestamp(-1, null, State.ABSENT);
    }

    private ByteBuffer getData() throws IOException {
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
