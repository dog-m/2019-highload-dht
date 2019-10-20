package ru.mail.polis.dao;

import java.util.NoSuchElementException;

/**
 * Lightweight exception without stack-trace collection.
 */
public class NoSuchElementExceptionLite extends NoSuchElementException {

    private static final long serialVersionUID = 1000L;

    public NoSuchElementExceptionLite(final String s) {
        super(s);
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }
}
