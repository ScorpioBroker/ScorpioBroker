package eu.neclab.ngsildbroker.commons.datatypes;

public class HistoryAttribInstance {

	private String entityId;
	private String entityType;
	private String entityCreatedAt;
	private String entityModifiedAt;
	private String attributeId;
	private String elementValue;
	private Boolean overwriteOp;
	private String instanceId;

	public HistoryAttribInstance(String entityId, String entityType, String entityCreatedAt, String entityModifiedAt,
			String attributeId, String elementValue, String instanceId, Boolean overwriteOp) {
		super();
		this.entityId = entityId;
		this.entityType = entityType;
		this.entityCreatedAt = entityCreatedAt;
		this.entityModifiedAt = entityModifiedAt;
		this.attributeId = attributeId;
		this.elementValue = elementValue;
		this.overwriteOp = overwriteOp;
		this.instanceId = instanceId;
	}

	public String getEntityId() {
		return entityId;
	}

	public void setEntityId(String entityId) {
		this.entityId = entityId;
	}

	public String getEntityType() {
		return entityType;
	}

	public void setEntityType(String entityType) {
		this.entityType = entityType;
	}

	public String getEntityCreatedAt() {
		return entityCreatedAt;
	}

	public void setEntityCreatedAt(String entityCreatedAt) {
		this.entityCreatedAt = entityCreatedAt;
	}

	public String getEntityModifiedAt() {
		return entityModifiedAt;
	}

	public void setEntityModifiedAt(String entityModifiedAt) {
		this.entityModifiedAt = entityModifiedAt;
	}

	public String getAttributeId() {
		return attributeId;
	}

	public void setAttributeId(String attributeId) {
		this.attributeId = attributeId;
	}

	public String getElementValue() {
		return elementValue;
	}

	public void setElementValue(String elementValue) {
		this.elementValue = elementValue;
	}

	public Boolean getOverwriteOp() {
		return overwriteOp;
	}

	public void setOverwriteOp(Boolean overwriteOp) {
		this.overwriteOp = overwriteOp;
	}

	public String getInstanceId() {
		return instanceId;
	}

	public void setInstanceId(String instanceId) {
		this.instanceId = instanceId;
	}

}
