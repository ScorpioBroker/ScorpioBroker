package eu.neclab.ngsildbroker.commons.datatypes;

import java.util.List;
import java.util.UUID;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;

abstract class BaseEntry {
	private String name;
	private Long observedAt = -1l;
	private List<Property> properties;
	private Object refToAccessControl;
	private List<Relationship> relationships;
	private int timeSeriesId;
	protected String type;
	private Long createdAt = -1l;
	private Long modifiedAt = -1l;
	private String dataSetId;

	BaseEntry(String dataSetId) {
		this.dataSetId = dataSetId;
		if (this.dataSetId == null || this.dataSetId.trim().isEmpty()) {
			this.dataSetId = NGSIConstants.DEFAULT_DATA_SET_ID;
		}
	}

	public String getDataSetId() {
		return dataSetId;
	}

	public void setDataSetId(String dataSetId) {
		this.dataSetId = dataSetId;
		if (this.dataSetId == null || this.dataSetId.trim().isEmpty()) {
			this.dataSetId = UUID.randomUUID().toString();
		}
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public long getObservedAt() {
		return observedAt;
	}

	public void setObservedAt(Long observedAt) {
		if (observedAt == null) {
			this.observedAt = -1l;
		} else {
			this.observedAt = observedAt;
		}
	}

	public List<Property> getProperties() {
		return properties;
	}

	public void setProperties(List<Property> properties) {
		this.properties = properties;
	}

	public Object getRefToAccessControl() {
		return refToAccessControl;
	}

	public void setRefToAccessControl(Object refToAccessControl) {
		this.refToAccessControl = refToAccessControl;
	}

	public List<Relationship> getRelationships() {
		return relationships;
	}

	public void setRelationships(List<Relationship> relationships) {
		this.relationships = relationships;
	}

	public int getTimeSeriesId() {
		return timeSeriesId;
	}

	public void setTimeSeriesId(int timeSeriesId) {
		this.timeSeriesId = timeSeriesId;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public long getCreatedAt() {
		return createdAt;
	}

	public void setCreatedAt(Long createdAt) {
		if (createdAt == null) {
			this.createdAt = -1l;
		} else {
			this.createdAt = createdAt;
		}
	}

	public long getModifiedAt() {
		return modifiedAt;
	}

	public void setModifiedAt(Long modifiedAt) {
		if (modifiedAt == null) {
			this.modifiedAt = -1l;
		} else {
			this.modifiedAt = modifiedAt;
		}
	}

}
