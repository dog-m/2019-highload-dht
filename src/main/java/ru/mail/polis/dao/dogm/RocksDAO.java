package ru.mail.polis.dao.dogm;

import org.rocksdb.Options;
import org.rocksdb.RocksDBException;
import org.rocksdb.BuiltinComparator;
import org.rocksdb.RocksDB;

import org.jetbrains.annotations.NotNull;

import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.io.File;
import java.io.IOException;

/**
 * Custom DAO implementation via RocksDB.
 */
public final class RocksDAO implements DAO {
    private final RocksDB db;

    /**
     * Constructor of custom RocksDB DAO implementation.
     */
    public RocksDAO(@NotNull final File data) throws IOException {
        RocksDB.loadLibrary();
        try {
            final var options = new Options()
                    .setCreateIfMissing(true)
                    .setComparator(BuiltinComparator.BYTEWISE_COMPARATOR);
            this.db = RocksDB.open(options, data.getAbsolutePath());
        } catch (RocksDBException e) {
            throw new RockException("Cannot create RocksDB instance", e);
        }
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) {
        final var iterator = db.newIterator();
        iterator.seek(ByteBufferUtils.restoreByteArray(from));
        return new RocksRecordIterator(iterator);
    }

    @NotNull
    @Override
    public ByteBuffer get(@NotNull final ByteBuffer key) throws RockException {
        try {
            final var result = db.get(ByteBufferUtils.restoreByteArray(key));
            if (result == null) {
                throw new NoSuchElementExceptionLite("Cant find element with key " + key.toString());
            }
            return ByteBuffer.wrap(result);
        } catch (RocksDBException e) {
            throw new RockException("Error while get", e);
        }
    }

    /**
     * Hacky way to retrieve data with timestamp on it from plain old RocksDB.
     */
    @NotNull
    public DataWithTimestamp getWithTimestamp(@NotNull final ByteBuffer key) throws RockException {
        try {
            return DataWithTimestamp.fromBytes(db.get(ByteBufferUtils.restoreByteArray(key)));
        } catch (RocksDBException e) {
            throw new RockException("Error while getWithTimestamp", e);
        }
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws RockException {
        try {
            db.put(ByteBufferUtils.restoreByteArray(key), ByteBufferUtils.getByteArray(value));
        } catch (RocksDBException e) {
            throw new RockException("Error while upsert", e);
        }
    }

    /**
     * Hacky way to store data with timestamp on it in plain old RocksDB.
     */
    public void upsertWithTimestamp(@NotNull final ByteBuffer key,
                                    @NotNull final ByteBuffer value) throws RockException {
        try {
            db.put(ByteBufferUtils.restoreByteArray(key),
                   DataWithTimestamp.fromPresent(value, System.currentTimeMillis()).toBytes());
        } catch (RocksDBException e) {
            throw new RockException("Error while upsertWithTimestamp", e);
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws RockException {
        try {
            db.delete(ByteBufferUtils.restoreByteArray(key));
        } catch (RocksDBException e) {
            throw new RockException("Error while remove", e);
        }
    }

    /**
     * Hacky way to mark data with timestamp deleted in plain old RocksDB.
     */
    public void removeWithTimestamp(@NotNull final ByteBuffer key) throws IOException {
        try {
            db.put(ByteBufferUtils.restoreByteArray(key),
                   DataWithTimestamp.fromRemovedAt(System.currentTimeMillis()).toBytes());
        } catch (RocksDBException e) {
            throw new RockException("Error while removeWithTimestamp", e);
        }
    }

    @Override
    public void compact() throws RockException {
        try {
            db.compactRange();
        } catch (RocksDBException e) {
            throw new RockException("Error while compact", e);
        }
    }

    @Override
    public void close() throws RockException {
        try {
            db.syncWal();
            db.closeE();
        } catch (RocksDBException e) {
            throw new RockException("Error while close", e);
        }
    }
}
