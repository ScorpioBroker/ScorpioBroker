package eu.neclab.ngsildbroker.commons.datatypes.results;

import java.util.Map;

import com.google.common.collect.Maps;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.NGSIRestResponse;

public class BatchFailure {
	private String entityId;
	private NGSIRestResponse ProblemDetails;

	public BatchFailure(String entityId, NGSIRestResponse details) {
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

	public NGSIRestResponse getDetails() {
		return ProblemDetails;
	}

	public void setDetails(NGSIRestResponse details) {
		this.ProblemDetails = details;
	}

	public Map<String, Object> toJson() {
		Map<String, Object> result = Maps.newHashMap();
		result.put("entityId", entityId);
		//TODO replace with constant
		result.put("ProblemDetails", ProblemDetails.toJson());
		return result;
	}

}
