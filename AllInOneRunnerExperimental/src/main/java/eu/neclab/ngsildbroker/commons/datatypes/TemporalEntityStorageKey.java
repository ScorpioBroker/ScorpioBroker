package eu.neclab.ngsildbroker.commons.datatypes;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class TemporalEntityStorageKey {


	public TemporalEntityStorageKey(String entityId) {
		super();
		this.entityId = entityId;
	}

	@SerializedName("entityId")
	@Expose
	private String entityId;
	@SerializedName("entityType")
	@Expose
	private String entityType;
	@SerializedName("entityCreatedAt")
	@Expose
	private String entityCreatedAt;
	@SerializedName("entityModifiedAt")
	@Expose
	private String entityModifiedAt;
	@SerializedName("attributeId")
	@Expose
	private String attributeId;
	@SerializedName("instanceId")
	@Expose
	private String instanceId;
	@SerializedName("overwriteOp")
	@Expose
	private Boolean overwriteOp;

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

	public String getInstanceId() {
		return instanceId;
	}

	public void setInstanceId(String instanceId) {
		this.instanceId = instanceId;
	}

	public Boolean getOverwriteOp() {
		return overwriteOp;
	}

	public void setOverwriteOp(Boolean overwriteOp) {
		this.overwriteOp = overwriteOp;
	}
		
}