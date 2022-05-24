package eu.neclab.ngsildbroker.commons.datatypes.results;

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

}
