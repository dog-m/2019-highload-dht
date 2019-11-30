package ru.mail.polis.service;

import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.*;
import ru.mail.polis.Files;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.DAOFactory;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ???
 *
 * @author Onischuck Mikhail
 */
class ServerSideProcessingTest extends ClusterTestBase {
    private static byte[] jsSource;
    private static final Duration TIMEOUT = Duration.ofMinutes(1);
    private File data0;
    private File data1;
    private DAO dao0;
    private DAO dao1;
    private Service storage0;
    private Service storage1;

    @BeforeAll
    static void prepare() throws Exception {
        loadJS();
    }

    @BeforeEach
    void beforeEach() throws Exception {
        final var port0 = randomPort();
        final var port1 = randomPort();
        endpoints = new LinkedHashSet<>(Arrays.asList(endpoint(port0), endpoint(port1)));
        data0 = Files.createTempDirectory();
        dao0 = DAOFactory.create(data0);
        storage0 = ServiceFactory.create(port0, dao0, endpoints);
        storage0.start();
        data1 = Files.createTempDirectory();
        dao1 = DAOFactory.create(data1);
        storage1 = ServiceFactory.create(port1, dao1, endpoints);
        start(1, storage1);
    }

    @AfterEach
    void afterEach() throws IOException {
        stop(0, storage0);
        dao0.close();
        Files.recursiveDelete(data0);
        stop(1, storage1);
        dao1.close();
        Files.recursiveDelete(data1);
        endpoints = Collections.emptySet();
    }

    @RepeatedTest(50)
    void jsMaxKeyLength() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            // Send some data in each node of the cluster
            final var keys = randomKeys(5);
            for (int i = 0, keysLength = keys.length; i < keysLength; i++) {
                final var node = i % 2;
                final var key = keys[i];
                final var value = randomValue();
                assertEquals(201, upsert(node, key, value, 1, 1).getStatus());
            }

            // Evaluate correct answer
            final var maxKeyLength = Arrays.stream(keys)
                    .map(String::length).max(Integer::compareTo)
                    .orElse(-1);

            // Check first node
            final var result0 = post(0, jsSource);
            assertEquals(200, result0.getStatus());
            assertEquals(maxKeyLength, Integer.parseInt(result0.getBodyUtf8()));

            // Check second node
            final var result1 = post(1, jsSource);
            assertEquals(200, result1.getStatus());
            assertEquals(maxKeyLength, Integer.parseInt(result1.getBodyUtf8()));
        });
    }

    @NotNull
    protected static String[] randomKeys(final int count) {
        final var hex = "0123456789abcdef";

        final var keys = new String[count];
        for (int i = 0; i < count; i++) {
            final var key = new char[ThreadLocalRandom.current().nextInt(5, 100)];
            for (int j = 0; j < key.length; j++) {
                final var index = ThreadLocalRandom.current().nextInt(0, hex.length());
                key[j] = hex.charAt(index);
            }

            keys[i] = new String(key);
        }
        return keys;
    }

    private String path() {
        return "/v0/execjs";
    }

    Response post(final int node,
                  @NotNull final byte[] data) throws Exception {
        return client(node).post(path(), data);
    }

    private static void loadJS() throws IOException {
        final var classLoader = ServerSideProcessingTest.class.getClassLoader();
        final var stream = classLoader.getResourceAsStream("js/longest_key.js");
        assert stream != null;

        jsSource = stream.readAllBytes();
        assert jsSource.length != 0;
    }
}
