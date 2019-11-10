package ru.mail.polis.dao.dogm;

import java.io.IOException;

public class RockException extends IOException {

    private static final long serialVersionUID = 2000L;

    public RockException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
