package eu.neclab.ngsildbroker.commons.datatypes;

import java.net.URI;
import java.util.List;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:13:22
 */
public class BaseProperty {

	protected URI id;
	protected String name;
	protected long observedAt = -1;
	protected List<Property> properties;
	private Object refToAccessControl;
	protected List<Relationship> relationships;
	protected int timeSeriesId;
	protected String type;
	private long createdAt = -1;
	private long modifiedAt = -1;
	
	

	public BaseProperty(){

	}

	public void finalize() throws Throwable {

	}

	public URI getId() {
		return id;
	}

	public void setId(URI id) {
		this.id = id;
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

	public void setObservedAt(long observedAt) {
		this.observedAt = observedAt;
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

	public void setCreatedAt(long createdAt) {
		this.createdAt = createdAt;
	}

	public long getModifiedAt() {
		return modifiedAt;
	}

	public void setModifiedAt(long modifiedAt) {
		this.modifiedAt = modifiedAt;
	}
	
	

}