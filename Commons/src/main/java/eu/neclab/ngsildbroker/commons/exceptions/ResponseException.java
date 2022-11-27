package eu.neclab.ngsildbroker.commons.exceptions;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.MultiMap;

/**
 * @version 1.0
 * @created 09-Jul-2018
 */
public class ResponseException extends Exception {
	private static final long serialVersionUID = 1L;

	private String endpoint;
	private String cSourceId;
	private int errorCode;
	private Object detail;
	private String title;
	private String type;
	private Map<String, Object> json = Maps.newHashMap();

	public ResponseException(ErrorType error) {
		this(error, error.getTitle());
	}

	public ResponseException(ErrorType error, String detail) {
		this(error, detail, null, null, null);
	}

	public ResponseException(ErrorType error, String detail, String endpoint, MultiMap headers, String cSourceId) {
		this(error.getCode(), error.getType(), error.getTitle(), detail, endpoint, headers, cSourceId);
	}

	public ResponseException(int errorCode, String type, String title, Object detail, String endpoint, MultiMap headers, String cSourceId) {
		this.errorCode = errorCode;
		this.title = title;
		this.detail = detail;
		this.endpoint = endpoint;
		this.cSourceId = cSourceId;
		this.type = type;
		json.put(NGSIConstants.ERROR_TYPE, type);
		json.put(NGSIConstants.ERROR_TITLE, title);
		if(endpoint == null) {
			json.put(NGSIConstants.ERROR_DETAIL, detail);
		}else {
			Map<String, Object> temp = Maps.newHashMap();
			temp.put(NGSIConstants.ERROR_DETAIL_MESSAGE, detail);
			temp.put(NGSIConstants.ERROR_DETAIL_ENDPOINT, endpoint);
			temp.put(NGSIConstants.ERROR_DETAIL_CSOURCE_ID, cSourceId);
			json.put(NGSIConstants.ERROR_DETAIL, temp);
		}
	}

	public String getEndpoint() {
		return endpoint;
	}

	public String getcSourceId() {
		return cSourceId;
	}

	public int getErrorCode() {
		return errorCode;
	}

	public Object getDetail() {
		return detail;
	}

	public Map<String, Object> getJson() {
		return json;
	}

}
