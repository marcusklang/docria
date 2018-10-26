package se.lth.cs.docria.exceptions;

public class SpanException extends RuntimeException {
    public SpanException() {
    }

    public SpanException(String message) {
        super(message);
    }

    public SpanException(String message, Throwable cause) {
        super(message, cause);
    }

    public SpanException(Throwable cause) {
        super(cause);
    }

    public SpanException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
