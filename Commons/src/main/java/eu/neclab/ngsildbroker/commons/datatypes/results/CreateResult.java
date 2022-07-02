package eu.neclab.ngsildbroker.commons.datatypes.results;

public class CreateResult {

	String entityId;
	boolean createdOrUpdated;

	public CreateResult(String entityId, boolean createdOrUpdated) {
		super();
		this.entityId = entityId;
		this.createdOrUpdated = createdOrUpdated;
	}

	public String getEntityId() {
		return entityId;
	}

	public void setEntityId(String entityId) {
		this.entityId = entityId;
	}

	public boolean isCreatedOrUpdated() {
		return createdOrUpdated;
	}

	public void setCreatedOrUpdated(boolean createdOrUpdated) {
		this.createdOrUpdated = createdOrUpdated;
	}

}
