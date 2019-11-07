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
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (createdAt ^ (createdAt >>> 32));
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		result = prime * result + (int) (modifiedAt ^ (modifiedAt >>> 32));
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + (int) (observedAt ^ (observedAt >>> 32));
		result = prime * result + ((properties == null) ? 0 : properties.hashCode());
		result = prime * result + ((refToAccessControl == null) ? 0 : refToAccessControl.hashCode());
		result = prime * result + ((relationships == null) ? 0 : relationships.hashCode());
		result = prime * result + timeSeriesId;
		result = prime * result + ((type == null) ? 0 : type.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		BaseProperty other = (BaseProperty) obj;
		if (createdAt != other.createdAt)
			return false;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		if (modifiedAt != other.modifiedAt)
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (observedAt != other.observedAt)
			return false;
		if (properties == null) {
			if (other.properties != null)
				return false;
		} else if (!properties.equals(other.properties))
			return false;
		if (refToAccessControl == null) {
			if (other.refToAccessControl != null)
				return false;
		} else if (!refToAccessControl.equals(other.refToAccessControl))
			return false;
		if (relationships == null) {
			if (other.relationships != null)
				return false;
		} else if (!relationships.equals(other.relationships))
			return false;
		if (timeSeriesId != other.timeSeriesId)
			return false;
		if (type == null) {
			if (other.type != null)
				return false;
		} else if (!type.equals(other.type))
			return false;
		return true;
	}

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