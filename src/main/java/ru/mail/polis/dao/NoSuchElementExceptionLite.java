package ru.mail.polis.dao;

import java.util.NoSuchElementException;

/**
 * Lightweight exception without stack-trace collection
 */
public class NoSuchElementExceptionLite extends NoSuchElementException {

    public NoSuchElementExceptionLite(final String s) {
        super(s);
    }

    @Override
    public Throwable fillInStackTrace() {
        synchronized (this) {
            return this;
        }
    }
}
