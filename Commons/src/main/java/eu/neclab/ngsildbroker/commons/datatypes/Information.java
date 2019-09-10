package eu.neclab.ngsildbroker.commons.datatypes;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Information {
	
	private List<EntityInfo> entities;
	private Set<String> properties;
	private Set<String> relationships;
	
	public Information() {
		this.entities=new ArrayList<EntityInfo>();
		this.properties=new HashSet<String>();
		this.relationships=new HashSet<String>();
	}
	public List<EntityInfo> getEntities() {
		return entities;
	}

	public void setEntities(List<EntityInfo> entities) {
		this.entities = entities;
	}

	public Set<String> getProperties() {
		return properties;
	}

	public void setProperties(Set<String> properties) {
		this.properties = properties;
	}

	public Set<String> getRelationships() {
		return relationships;
	}

	public void setRelationships(Set<String> relationships) {
		this.relationships = relationships;
	}

	@Override
	public String toString() {
		return "Information [entities=" + entities + ", properties=" + properties + ", relationships=" + relationships
				+ "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((entities == null) ? 0 : entities.hashCode());
		result = prime * result + ((properties == null) ? 0 : properties.hashCode());
		result = prime * result + ((relationships == null) ? 0 : relationships.hashCode());
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
		Information other = (Information) obj;
		if (entities == null) {
			if (other.entities != null)
				return false;
		} else if (!entities.equals(other.entities))
			return false;
		if (properties == null) {
			if (other.properties != null)
				return false;
		} else if (!properties.equals(other.properties))
			return false;
		if (relationships == null) {
			if (other.relationships != null)
				return false;
		} else if (!relationships.equals(other.relationships))
			return false;
		return true;
	}
	
}
