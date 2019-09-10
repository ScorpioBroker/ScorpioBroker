/*** Begin copyright statement *************************************************
 * LeafEngine (eu.nec.leaf.commons)
 * 
 * File: HttpErrorResponseException.java
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
package eu.neclab.ngsildbroker.commons.exceptions;

import org.apache.http.client.HttpResponseException;

/**
 * Exception thrown when an HTTP request returns but with a non 200-299 status
 * code.
 * 
 * @author the leafengine team
 * 
 */
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
