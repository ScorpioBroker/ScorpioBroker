package eu.neclab.ngsildbroker.commons.datatypes.results;

import java.util.Map;

import com.google.common.collect.Maps;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.RestResponse;

public class BatchFailure {
	private String entityId;
	private RestResponse ProblemDetails;

	public BatchFailure(String entityId, RestResponse details) {
		super();
		this.entityId = entityId;
		this.ProblemDetails = details;
	}

	public String getEntityId() {
		return entityId;
	}

	public void setEntityId(String entityId) {
		this.entityId = entityId;
	}

	public RestResponse getDetails() {
		return ProblemDetails;
	}

	public void setDetails(RestResponse details) {
		this.ProblemDetails = details;
	}

	public Map<String, Object> toJson() {
		Map<String, Object> result = Maps.newHashMap();
		result.put(NGSIConstants.JSON_LD_ID, entityId);
		// TODO replace with constant
		result.put("ProblemDetails", ProblemDetails.toJson());
		return result;
	}

}
