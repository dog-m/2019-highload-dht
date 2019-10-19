package ru.mail.polis.dao;

import org.jetbrains.annotations.NotNull;
import org.rocksdb.RocksIterator;
import ru.mail.polis.Record;

import java.nio.ByteBuffer;
import java.util.Iterator;

public class RocksRecordIterator implements Iterator<Record>, AutoCloseable {

    private final RocksIterator iterator;

    RocksRecordIterator(@NotNull final RocksIterator iterator) {
        this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
        return iterator.isValid();
    }

    @Override
    public Record next() throws IllegalStateException {
        if (!hasNext()) {
            throw new IllegalStateException("Iterator is exhausted");
        }
        final ByteBuffer key = ByteBufferUtils.shiftByteArray(iterator.key());
        final ByteBuffer value = ByteBuffer.wrap(iterator.value());
        final Record record = Record.of(key, value);
        iterator.next();
        return record;
    }

    @Override
    public void close() {
        iterator.close();
    }
}
