package eu.neclab.ngsildbroker.commons.exceptions;

public class LdContextException extends Exception {

    /**
	 * 
	 */
	private static final long serialVersionUID = 756693170539690404L;

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
