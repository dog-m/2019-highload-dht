package ru.mail.polis.dao.dogm;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;

public final class DataWithTimestamp {
    private final State state;
    private final long stamp;
    private final ByteBuffer present;

    enum State {
        PRESENT((byte) 1),
        REMOVED((byte) -1),
        ABSENT((byte) 0);

        final byte value;

        State(final byte value) {
            this.value = value;
        }

        static State fromValue(final byte value) {
            if (value == REMOVED.value) {
                return REMOVED;
            }
            if (value == PRESENT.value) {
                return PRESENT;
            }
            return ABSENT;
        }
    }

    private DataWithTimestamp(final long stamp, final ByteBuffer present, final State type) {
        this.stamp = stamp;
        this.state = type;
        this.present = present;
    }

    static DataWithTimestamp fromPresent(final ByteBuffer present, final long timestamp) {
        return new DataWithTimestamp(timestamp, present, State.PRESENT);
    }

    static DataWithTimestamp timestamp(final long timestamp) {
        return new DataWithTimestamp(timestamp, null, State.REMOVED);
    }

    private long getStamp() {
        return stamp;
    }

    public boolean isPresent() {
        return state == State.PRESENT;
    }

    public boolean isAbsent() {
        return state == State.ABSENT;
    }

    public boolean isRemoved() {
        return state == State.REMOVED;
    }

    /**
     * Method allow get Value as byte arrays.
     */
    public byte[] getPresentAsBytes() throws IOException {
        final var duplicate = getPresent().duplicate();
        final var result = new byte[duplicate.remaining()];
        duplicate.get(result);
        return result;
    }

    private ByteBuffer getPresent() throws IOException {
        if (!isPresent()) {
            throw new IOException("value is not present");
        }
        return present;
    }

    /**
     * Get most recent timestamp from responses or absent one.
     */
    public static DataWithTimestamp merge(final List<DataWithTimestamp> responses) {
        if (responses.size() == 1) {
            return responses.get(0);
        } else {
            return responses.stream()
                    .filter(timestamp -> !timestamp.isAbsent())
                    .max(Comparator.comparingLong(DataWithTimestamp::getStamp))
                    .orElseGet(DataWithTimestamp::getAbsent);
        }
    }

    public static DataWithTimestamp getAbsent() {
        return new DataWithTimestamp(-1, null, State.ABSENT);
    }

    /**
     * Conversion from an array of bytes.
     */
    public static DataWithTimestamp fromBytes(final byte[] bytes) {
        if (bytes == null) {
            return getAbsent();
        }
        final var buffer = ByteBuffer.wrap(bytes);
        final var recordType = State.fromValue(buffer.get());
        return new DataWithTimestamp(buffer.getLong(), buffer, recordType);
    }

    /**
     * Conversion to bytes.
     */
    public byte[] toBytes() {
        int length = 0;
        if (isPresent()) {
            length = present.remaining();
        }
        final var byteBuffer = ByteBuffer.allocate(1 + Long.BYTES + length);
        byteBuffer.put(state.value);
        byteBuffer.putLong(getStamp());
        if (isPresent()) {
            byteBuffer.put(present.duplicate());
        }
        return byteBuffer.array();
    }
}
