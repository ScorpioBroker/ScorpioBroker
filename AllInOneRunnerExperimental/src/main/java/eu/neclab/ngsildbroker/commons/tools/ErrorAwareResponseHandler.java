package eu.neclab.ngsildbroker.commons.tools;

import java.io.IOException;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.util.EntityUtils;

import eu.neclab.ngsildbroker.commons.exceptions.HttpErrorResponseException;


public class ErrorAwareResponseHandler extends BasicResponseHandler {

	private static final int MIN_NON_SUCCESSFUL_STATUS = 300;

	@Override
	public String handleResponse(final HttpResponse response)
			throws IOException {
		StatusLine statusLine = response.getStatusLine();
		HttpEntity entity = response.getEntity();
		String body = entity == null ? null : EntityUtils.toString(entity);
		EntityUtils.consume(entity);
		if (statusLine.getStatusCode() >= MIN_NON_SUCCESSFUL_STATUS) {
			throw new HttpErrorResponseException(statusLine.getStatusCode(),
					statusLine.getReasonPhrase());
		}
		return body;
	}
}
