package ru.mail.polis.service.dogm;

import one.nio.http.HttpServer;
import one.nio.http.HttpSession;
import one.nio.http.Response;
import one.nio.net.Socket;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.Record;
import ru.mail.polis.dao.dogm.ByteBufferUtils;
import ru.mail.polis.dao.dogm.DataWithTimestamp;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import static java.nio.charset.StandardCharsets.UTF_8;
import static one.nio.http.Response.OK;

public class StorageSession extends HttpSession {
    private static final byte[] CRLF = "\r\n".getBytes(UTF_8);
    private static final byte[] LF = "\n".getBytes(UTF_8);
    private static final byte[] EMPTY = "0\r\n\r\n".getBytes(UTF_8);

    private Iterator<Record> records;

    StorageSession(@NotNull final Socket socket,
                   @NotNull final HttpServer server) {
        super(socket, server);
    }

    /**
     * Method to send large amount of data to client via chunked-transfer-encoding.
     */
    void stream(final Iterator<Record> records) throws IOException {
        this.records = records;

        final Response response = new Response(OK);
        response.addHeader("Transfer-Encoding: chunked");
        writeResponse(response, false);

        next();
    }

    @Override
    protected void processWrite() throws Exception {
        super.processWrite();

        next();
    }

    private void next() throws IOException {
        while (records.hasNext() && queueHead == null) {
            final Record record = records.next();

            final byte[] key = ByteBufferUtils.getByteArray(record.getKey());
            final var data = DataWithTimestamp.fromBytes(ByteBufferUtils.getByteArray(record.getValue()));
            final byte[] value = ByteBufferUtils.getByteArray(data.getData());

            final int size = key.length + value.length + LF.length;
            final byte[] hexSize = Integer.toHexString(size).getBytes(UTF_8);
            final int len = size + hexSize.length + 2 * CRLF.length;

            final byte[] chunk = new byte[len];
            final ByteBuffer chunkBuffer = ByteBuffer.wrap(chunk);

            chunkBuffer.put(hexSize);
            chunkBuffer.put(CRLF);
            chunkBuffer.put(key);
            chunkBuffer.put(LF);
            chunkBuffer.put(value);
            chunkBuffer.put(CRLF);

            write(chunk, 0, len);
        }

        if (!records.hasNext()) {
            write(EMPTY, 0, EMPTY.length);

            server.incRequestsProcessed();
            handling = pipeline.pollFirst();
            if (handling != null) {
                if (handling == FIN) {
                    scheduleClose();
                } else {
                    server.handleRequest(handling, this);
                }
            }
        }
    }
}
