package eu.neclab.ngsildbroker.commons.datatypes;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Maps;

import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * @version 1.0
 * @created 09-Jul-2018
 */

public class NGSIRestResponse {
	private String type;
	@JsonIgnore
	private HttpResponseStatus status;
	private String title;
	private String details;

	/*
	 * public RestResponse(HttpStatus status, String title, String detail) {
	 * super(); this.status = status; this.title = title; this.details = detail;
	 * this.type = status.getReasonPhrase(); }
	 */

	public NGSIRestResponse(ErrorType errorType, String details) {
		this.status = HttpResponseStatus.valueOf(errorType.getCode());
		this.title = errorType.getMessage();
		this.details = details;
		this.type = errorType.getErrorType();
	}

	public NGSIRestResponse(ResponseException exception) {
		super();
		this.status = exception.getHttpStatus();
		this.title = exception.getError().getMessage();
		this.details = exception.getMessage();
		this.type = exception.getError().getErrorType();
	}

	public String getType() {
		return type;
	}

	public HttpResponseStatus getStatus() {
		return status;
	}

	public String getTitle() {
		return title;
	}

	public String getDetail() {
		return details;
	}

	public Map<String, Object> toJson() {
		Map<String, Object> result = Maps.newHashMap();
		result.put("type", type);
		result.put("title", title);
		result.put("detail", details);
//		String result = "{\n\t\"type\":\"" + type + "\",\n\t\"title\":\"" + title + "\",\n\t\"detail\":\"" + details
//				+ "\"\n}";
		return result;
	}

}
