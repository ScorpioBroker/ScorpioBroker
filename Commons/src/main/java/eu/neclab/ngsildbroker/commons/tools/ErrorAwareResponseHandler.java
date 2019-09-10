/*** Begin copyright statement *************************************************
 * LeafEngine (eu.nec.leaf.commons)
 * 
 * File: ErrorAwareResponseHandler.java
 * 
 * NEC Display Solutions Europe GmbH  PROPRIETARY INFORMATION
 * 
 * This software is supplied under the terms of a license agreement
 * or nondisclosure agreement with NEC Display Solutions Europe GmbH
 * and may not be copied or disclosed except in accordance with the
 * terms of that agreement. The software and its source code contain
 * valuable  trade secrets and confidential information which have to
 * be maintained in confidence.
 * 
 * Any unauthorized publication, transfer to third parties or
 * duplication of the object or source code - either totally or in
 * part - is prohibited.
 * 
 * Copyright (c) 2010-2013 NEC Display Solutions Europe GmbH
 * All Rights Reserved.
 * 
 * Authors:
 *  the leafengine team at NEC Laboratories Europe Ltd.
 *  e-mail: leaf@neclab.eu
 * 
 * NEC Display Solutions Europe GmbH DISCLAIMS ALL WARRANTIES,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE AND THE WARRANTY AGAINST LATENT
 * DEFECTS, WITH RESPECT TO THE PROGRAM AND THE
 * ACCOMPANYING DOCUMENTATION.
 * 
 * No Liability For Consequential Damages IN NO EVENT SHALL NEC
 * Display Solutions Europe GmbH, NEC Corporation OR ANY OF ITS
 * SUBSIDIARIES BE LIABLE FOR ANY DAMAGES WHATSOEVER (INCLUDING,
 * WITHOUT LIMITATION, DAMAGES FOR LOSS OF BUSINESS PROFITS,
 * BUSINESS INTERRUPTION, LOSS OF INFORMATION, OR OTHER
 * PECUNIARY LOSS AND INDIRECT, CONSEQUENTIAL, INCIDENTAL,
 * ECONOMIC OR PUNITIVE DAMAGES) ARISING OUT OF THE USE OF
 * OR INABILITY TO USE THIS PROGRAM, EVEN IF NEC Display
 * Solutions Europe GmbH HAS BEEN ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGES.
 * 
 * THIS HEADER MAY NOT BE EXTRACTED OR MODIFIED IN ANY WAY.
 ******************************************************************************/
package eu.neclab.ngsildbroker.commons.tools;

import java.io.IOException;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.util.EntityUtils;

import eu.neclab.ngsildbroker.commons.exceptions.HttpErrorResponseException;

/**
 * A response handler that throws an {@link HttpErrorResponseException} when a
 * response is received with a non 200-299 status.
 * 
 * @author the leafengine team
 * 
 */
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
