package eu.neclab.ngsildbroker.commons.datatypes;

import java.util.Arrays;
import java.util.List;

import org.springframework.http.HttpStatus;

import com.fasterxml.jackson.annotation.JsonIgnore;

import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

/**
 * @version 1.0
 * @created 09-Jul-2018
 */

public class RestResponse {
	private int type;
	@JsonIgnore
	private HttpStatus status;
	private String title;
	private String details;

	public RestResponse(HttpStatus status, String title, String detail) {
		super();
		this.status = status;
		this.title = title;
		this.details = detail;
		this.type = status.value();
	}

	public RestResponse(ResponseException exception) {
		super();
		this.status = exception.getHttpStatus();
		this.title = exception.getError().getMessage();
		this.details = exception.getMessage();
		this.type = status.value();
	}

	public int getType() {
		return type;
	}

	public HttpStatus getStatus() {
		return status;
	}

	public String getTitle() {
		return title;
	}

	public String getDetail() {
		return details;
	}

}
