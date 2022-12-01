package eu.neclab.ngsildbroker.commons.exceptions;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.github.jsonldjava.core.Context;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.results.Attrib;
import eu.neclab.ngsildbroker.commons.datatypes.results.CRUDSuccess;
import eu.neclab.ngsildbroker.commons.datatypes.results.NGSILDOperationResult;
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
	private Set<Attrib> attribs;
	private Map<String, Object> json = Maps.newHashMap();

	public ResponseException(ErrorType error) {
		this(error, error.getTitle());
	}

	public ResponseException(ErrorType error, Object detail) {
		this(error, detail, null, null, null, null);
	}

	public ResponseException(ErrorType error, Object detail, Set<Attrib> attribs) {
		this(error, detail, null, null, null, attribs);
	}

	public ResponseException(ErrorType error, Object detail, String endpoint, MultiMap headers, String cSourceId,
			Set<Attrib> attribs) {
		this(error.getCode(), error.getType(), error.getTitle(), detail, endpoint, headers, cSourceId, attribs);
	}

	public ResponseException(int errorCode, String type, String title, Object detail, String endpoint, MultiMap headers,
			String cSourceId, Set<Attrib> attribs) {
		this.errorCode = errorCode;
		this.title = title;
		this.detail = detail;
		this.endpoint = endpoint;
		this.cSourceId = cSourceId;
		this.type = type;
		this.attribs = attribs;
		json.put(NGSIConstants.ERROR_TYPE, type);
		json.put(NGSIConstants.ERROR_TITLE, title);
		json.put(NGSIConstants.ERROR_CODE, errorCode);
		List<Map<String, String>> tmp = Lists.newArrayList();
		for (Attrib attrib : attribs) {
			tmp.add(attrib.getJson());
		}
		Map<String, Object> temp = Maps.newHashMap();
		temp.put(NGSIConstants.NGSI_LD_ATTRIBUTES_SHORT, tmp);
		temp.put(NGSIConstants.ERROR_DETAIL_MESSAGE, detail);
		temp.put(NGSIConstants.ERROR_DETAIL_ENDPOINT, endpoint);
		temp.put(NGSIConstants.ERROR_DETAIL_CSOURCE_ID, cSourceId);
		json.put(NGSIConstants.ERROR_DETAIL, temp);
	}

	public ResponseException(ErrorType error, Object detail, Map<String, Object> entity, Context context) {
		this(error, detail, null, null, null, entity, context);
	}

	public ResponseException(ErrorType error, Object detail, String host, MultiMap headers, String cSourceId,
			Map<String, Object> entity, Context context) {
		this(error.getCode(), error.getType(), error.getTitle(), detail, host, headers, cSourceId, entity, context);
	}

	public ResponseException(int errorCode, String type, String title, Object detail, String host, MultiMap headers,
			String cSourceId, Map<String, Object> entity, Context context) {
		this(errorCode, type, title, detail, host, headers, cSourceId,
				NGSILDOperationResult.getAttribs(entity, context));
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

	public static ResponseException fromPayload(Map<String, Object> entry) {
		Set<Attrib> attribs = null;
		Map<String, Object> entryDetail = (Map<String, Object>) entry.get(NGSIConstants.ERROR_DETAIL);
		String cSourceId = (String) entryDetail.get(NGSIConstants.ERROR_DETAIL_CSOURCE_ID);
		String endpoint = (String) entryDetail.get(NGSIConstants.ERROR_DETAIL_ENDPOINT);
		Object detail = entryDetail.get(NGSIConstants.ERROR_DETAIL_MESSAGE);
		Object tmp = entryDetail.get(NGSIConstants.NGSI_LD_ATTRIBUTES_SHORT);
		if (tmp != null && tmp instanceof List) {
			attribs = Sets.newHashSet();
			List<Map<String, String>> entryAttribs = (List<Map<String, String>>) tmp;
			for (Map<String, String> entryAttrib : entryAttribs) {
				try {
					attribs.add(Attrib.fromPayload(entryAttrib));
				} catch (ResponseException e) {
					// TODO don't know for now we got send some other error message
					e.printStackTrace();
				}
			}
		}

		return new ResponseException((int) entry.get(NGSIConstants.ERROR_CODE),
				(String) entry.get(NGSIConstants.ERROR_TYPE), (String) entry.get(NGSIConstants.ERROR_TITLE), detail,
				endpoint, null, cSourceId, attribs);
	}

}
