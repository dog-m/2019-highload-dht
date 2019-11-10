package ru.mail.polis.service.dogm;

final class Protocol {
    static final String FAIL_INVALID_PORT = "Invalid port";
    static final String FAIL_METHOD = "Wrong method";
    static final String FAIL_ERROR_SEND = "Cannot send an error message";
    static final String FAIL_PROXY = "Proxy failure";
    static final String FAIL_MISSING_ID = "No id";
    static final String FAIL_REPLICAS = "Invalid replicas fraction";

    static final String HEADER_PROXIED = "X-Proxied: 1";

    private Protocol() {
        // not instantiable
    }
}
