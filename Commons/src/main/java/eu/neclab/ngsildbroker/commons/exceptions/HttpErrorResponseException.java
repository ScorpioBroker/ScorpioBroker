
package eu.neclab.ngsildbroker.commons.exceptions;

import org.apache.http.client.HttpResponseException;


public class HttpErrorResponseException extends HttpResponseException {
	private static final long serialVersionUID = -5656867439394559485L;

	/**
	 * Instantiate the exception.
	 * 
	 * @param statusCode
	 *            the status code that lead to this error
	 * @param statusReason
	 *            the reason in the status message
	 */
	public HttpErrorResponseException(int statusCode, String statusReason) {
		super(statusCode, statusReason);
	}
}
