package eu.neclab.ngsildbroker.commons.exceptions;

public class LdContextException extends Exception {

    public LdContextException() {
        super("LDContext is not available.");
    }

    public LdContextException(String message) {
        super(message);
    }

    public LdContextException(String message, Throwable cause) {
        super(message, cause);
    }

    public LdContextException(Throwable cause) {
        super(cause);
    }
}
