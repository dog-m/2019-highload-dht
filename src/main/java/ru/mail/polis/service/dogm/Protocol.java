package ru.mail.polis.service.dogm;

abstract class Protocol {
    static final String FAIL_INVALID_PORT = "Invalid port";
    static final String FAIL_ERROR_SEND = "Cannot send an error message";
    static final String FAIL_PROXY = "Proxy failure";
    static final String FAIL_MISSING_ID = "No id";
    static final String FAIL_REPLICAS = "Invalid replicas fraction";

    static final String HEADER_PROXIED = "X-Proxied: true";
    static final String HEADER_TIMESTAMP = "X-Timestamp: ";
    static final String HEADER_TIMESTAMP_FORMAT = HEADER_TIMESTAMP + "%d";
    static final String HEADER_STATE = "X-State: ";
    static final String HEADER_STATE_FORMAT = HEADER_STATE + "%d";
}
