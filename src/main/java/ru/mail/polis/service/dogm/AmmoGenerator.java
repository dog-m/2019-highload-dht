package ru.mail.polis.service.dogm;

import org.jetbrains.annotations.NotNull;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;

import static java.nio.charset.StandardCharsets.US_ASCII;

final class AmmoGenerator {
    private static final Logger log = Logger.getLogger(AmmoGenerator.class.getName());
    private static final int VALUE_LENGTH = 256;
    private static final String CRLF = "\r\n";

    private static final String STR_MAINCLASS = "ru.mail.polis.service.dogm.AmmoGenerator";
    private static final String STR_CLASSPATH = "-cp build/classes/java/main " + STR_MAINCLASS;
    private static final String STR_METHODS = "put|put-o|get|get-r|mix";
    private static final String STR_PARAMS = "<" + STR_METHODS + "> <requests>";
    private static final String STR_USAGE = "Usage:\n\tjava " + STR_CLASSPATH + " " + STR_PARAMS;

    private final String mode;
    private final byte[] tag;
    private final int count;
    private final PrintStream out;
    private static final long GENERATOR_SEED = "2019-highload-dht".hashCode();
    private final Random keyGenerator = new Random(GENERATOR_SEED);

    private AmmoGenerator(final String mode, final int count) throws FileNotFoundException {
        if (!STR_METHODS.contains(mode)) {
            throw new UnsupportedOperationException("Unsupported mode: " + mode);
        }

        this.mode = mode;
        this.tag = (" " + mode + "\n").getBytes(US_ASCII);
        this.count = count;
        this.out = new PrintStream(new BufferedOutputStream(new FileOutputStream(mode + ".ammo")), true);
    }

    @NotNull
    private String randomKey() {
        return "k" + keyGenerator.nextLong();
    }

    @NotNull
    private static byte[] randomValue() {
        final byte[] result = new byte[VALUE_LENGTH];
        ThreadLocalRandom.current().nextBytes(result);
        return result;
    }

    private void put(final String key, final byte[] value) throws IOException {
        final ByteArrayOutputStream request = new ByteArrayOutputStream();
        try (Writer writer = new OutputStreamWriter(request, US_ASCII)) {
            writer.write("PUT /v0/entity?id=" + key + " HTTP/1.1\r\n");
            writer.write("Content-Length: " + value.length + CRLF);
            writer.write(CRLF);
        }
        request.write(value);
        out.write(Integer.toString(request.size()).getBytes(US_ASCII));
        out.write(tag);
        request.writeTo(out);
        out.write(CRLF.getBytes(US_ASCII));
    }

    private void get(final String key) throws IOException {
        final ByteArrayOutputStream request = new ByteArrayOutputStream();
        try (Writer writer = new OutputStreamWriter(request, US_ASCII)) {
            writer.write("GET /v0/entity?id=" + key + " HTTP/1.1\r\n");
            writer.write(CRLF);
        }
        out.write(Integer.toString(request.size()).getBytes(US_ASCII));
        out.write(tag);
        request.writeTo(out);
        out.write(CRLF.getBytes(US_ASCII));
    }

    private void putUnique() throws IOException {
        for (int i = 0; i < count; i++) {
            put(randomKey(), randomValue());
        }
    }

    private void getUnique() throws IOException {
        for (int i = 0; i < count; i++) {
            get(randomKey());
        }
    }

    private List<String> randomKeys(final int amount) {
        final var keys = new ArrayList<String>(amount);
        for (int i = 0; i < amount; i++) {
            keys.add(randomKey());
        }
        return keys;
    }

    private void mixGetPut() throws IOException {
        final var keys = randomKeys(count / 2);
        final var existingKeys = new ArrayList<String>(keys.size());
        for (int i = 0; i < count; i++) {
            if (ThreadLocalRandom.current().nextBoolean()) {
                final var key = keys.get(i % keys.size());
                put(key, randomValue());
                existingKeys.add(key);
            } else {
                final int index = ThreadLocalRandom.current().nextInt(0, existingKeys.size());
                get(existingKeys.get(index));
            }
        }
    }

    private void putWithOverwrite() throws IOException {
        final var keys = randomKeys(count);
        final int repeatCount = count / 10;
        for (int i = 0; i < repeatCount; i++) {
            final var index = ThreadLocalRandom.current().nextInt(count);
            final var key = keys.get(ThreadLocalRandom.current().nextInt(count));
            keys.set(index, key);
        }

        for (final String key : keys) {
            put(key, randomValue());
        }
    }

    private static int clamp(final int x, final int low, final int high) {
        return Math.max(low, Math.min(x, high));
    }

    private void getWithRecent() throws IOException {
        final var keys = randomKeys(count);
        for (int i = 0; i < count; i++) {
            final int index = (int) Math.round(count * (ThreadLocalRandom.current().nextGaussian() * 0.1 + 0.9));
            get(keys.get(clamp(index, 0, count - 1)));
        }
    }

    private void print() throws IOException {
        switch (mode) {
            case "put":
                putUnique();
                break;

            case "put-o":
                putWithOverwrite();
                break;

            case "get":
                getUnique();
                break;

            case "get-r":
                getWithRecent();
                break;

            case "mix":
                mixGetPut();
                break;

            default:
                throw new UnsupportedOperationException("Unsupported mode: " + mode);
        }
    }

    public static void main(final String[] args) throws IOException {
        if (args.length != 2) {
            log.info(STR_USAGE);
            System.exit(-1);
        }

        final var generator = new AmmoGenerator(args[0], Integer.parseInt(args[1]));
        generator.print();
    }
}
