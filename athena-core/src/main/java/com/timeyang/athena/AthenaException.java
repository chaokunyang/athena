package com.timeyang.athena;

/**
 * {@code AthenaException} is the superclass of those exceptions that caused by athena
 * @author https://github.com/chaokunyang
 */
public class AthenaException extends RuntimeException {

    private String message;
    private Throwable throwable;

    public AthenaException(String message) {
        this(message, null);
    }

    public AthenaException(String message, Throwable throwable) {
        this.message = message;
        this.throwable = throwable;
    }

    @Override
    public String getMessage() {
        return message;
    }

    public Throwable getThrowable() {
        return throwable;
    }
}
