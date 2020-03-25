package eu.neclab.ngsildbroker.commons.tools;

public enum HTTPMethod {
	/** GET method. */
	GET,
	/** POST method. */
	POST,
	/** PUT method. */
	PUT,
	/** DELETE method. */
	DELETE,
	/** HEAD method. */
	HEAD,
	/** OPTIONS method. */
	OPTIONS;

	/**
	 * Get the method out of its name.
	 * 
	 * @param methodString
	 *            the name of the method
	 * @return an instance of this enum representing the method
	 */
	public static HTTPMethod getMethod(String methodString) {
		for (HTTPMethod method : HTTPMethod.values()) {
			if (method.name().equals(methodString)) {
				return method;
			}
		}
		throw new AssertionError("Unknown method");
	}
}
