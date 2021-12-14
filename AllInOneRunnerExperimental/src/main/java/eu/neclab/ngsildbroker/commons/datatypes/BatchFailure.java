package eu.neclab.ngsildbroker.commons.datatypes;

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
	
	
	

}
