package se.lth.cs.docria.exceptions;

public class DataInconsistencyException extends RuntimeException {
    public DataInconsistencyException() {
    }

    public DataInconsistencyException(String message) {
        super(message);
    }

    public DataInconsistencyException(String message, Throwable cause) {
        super(message, cause);
    }

    public DataInconsistencyException(Throwable cause) {
        super(cause);
    }

    public DataInconsistencyException(String message, Throwable cause, boolean enableSuppression,
                                      boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
