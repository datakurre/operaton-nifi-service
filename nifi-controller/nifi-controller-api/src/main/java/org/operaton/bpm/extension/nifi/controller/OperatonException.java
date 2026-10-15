package org.operaton.bpm.extension.nifi.controller;

/**
 * Failed call to the Operaton REST API.
 */
public class OperatonException extends Exception {

    private final int statusCode;

    /**
     * @param statusCode HTTP status code, or 0 when no response was received
     */
    public OperatonException(String message, int statusCode, Throwable cause) {
        super(message, cause);
        this.statusCode = statusCode;
    }

    public OperatonException(String message, int statusCode) {
        this(message, statusCode, null);
    }

    /**
     * @return HTTP status code, or 0 when no response was received
     */
    public int getStatusCode() {
        return statusCode;
    }
}
