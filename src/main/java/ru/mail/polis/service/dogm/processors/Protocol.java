package ru.mail.polis.service.dogm.processors;

public final class Protocol {
    public static final String FAIL_INVALID_PORT = "Invalid port";
    public static final String FAIL_METHOD = "Wrong method";
    public static final String FAIL_ERROR_SEND = "Cannot send an error message";
    public static final String FAIL_MISSING_ID = "No id";
    public static final String FAIL_REPLICAS = "Invalid replicas fraction";
    public static final String FAIL_WRONG_PROCESSOR = "Wrong processor";

    public static final String WARN_PROXY = "Proxy failure";
    public static final String WARN_PROCESSOR = "In-processor failure";

    public static final String HEADER_PROXIED = "X-Proxied: 1";

    private Protocol() {
        // not instantiable
    }
}
