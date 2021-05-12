package eu.neclab.ngsildbroker.commons.datatypes;

import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ArrayListMultimap;

public class CSourceRequest extends BaseRequest {

	protected String id;
	protected ObjectMapper objectMapper = new ObjectMapper();

	protected CSourceRegistration csourceRegistration;

	public CSourceRequest() {

	}

	public CSourceRequest(List<Object> context, ArrayListMultimap<String, String> headers) {
		super(headers);
		// TODO Auto-generated constructor stub
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public ObjectMapper getObjectMapper() {
		return objectMapper;
	}

	public void setObjectMapper(ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
	}

	public CSourceRegistration getCsourceRegistration() {
		return csourceRegistration;
	}

	public void setCsourceRegistration(CSourceRegistration csourceRegistration) {
		this.csourceRegistration = csourceRegistration;
	}

}
