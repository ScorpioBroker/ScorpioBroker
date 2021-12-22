package eu.neclab.ngsildbroker.commons.datatypes;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.ArrayListMultimap;

public class CSourceRequest extends BaseRequest {

	protected String id;
	protected Map<String, Object> csourceRegistration;
	public CSourceRequest(ArrayListMultimap<String, String> headers) {
		super(headers);
	}

	public CSourceRequest() {
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Map<String, Object> getCsourceRegistration() {
		return csourceRegistration;
	}

	public void setCsourceRegistration(Map<String, Object> csourceRegistration) {
		this.csourceRegistration = csourceRegistration;
	}
	protected String generateUniqueRegId() {
		String key = "urn:ngsi-ld:csourceregistration:"
				+ UUID.fromString("" + csourceRegistration.hashCode()).toString();
		return key;
	}

	public String getCsourceRegistrationString() {
		if(csourceRegistration == null) {
			return null;
		}
		try {
			return JsonUtils.toString(csourceRegistration);
		} catch (IOException e) {
			logger.error(e.getMessage());
			//should never happen
			return null;
		}
	}
}
