package eu.neclab.ngsildbroker.commons.datatypes;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Information {
	
	private List<EntityInfo> entities;
	private Set<String> propertyNames;
	private Set<String> relationshipNames;
	
	public Information() {
		this.entities=new ArrayList<EntityInfo>();
		this.propertyNames=new HashSet<String>();
		this.relationshipNames=new HashSet<String>();
	}
	public List<EntityInfo> getEntities() {
		return entities;
	}

	public void setEntities(List<EntityInfo> entities) {
		this.entities = entities;
	}

	public Set<String> getPropertyNames() {
		return propertyNames;
	}

	public void setPropertyNames(Set<String> propertyNames) {
		this.propertyNames = propertyNames;
	}

	public Set<String> getRelationshipNames() {
		return relationshipNames;
	}

	public void setRelationshipNames(Set<String> relationshipNames) {
		this.relationshipNames = relationshipNames;
	}

	@Override
	public String toString() {
		return "Information [entities=" + entities + ", properties=" + propertyNames + ", relationships=" + relationshipNames
				+ "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((entities == null) ? 0 : entities.hashCode());
		result = prime * result + ((propertyNames == null) ? 0 : propertyNames.hashCode());
		result = prime * result + ((relationshipNames == null) ? 0 : relationshipNames.hashCode());
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
		if (propertyNames == null) {
			if (other.propertyNames != null)
				return false;
		} else if (!propertyNames.equals(other.propertyNames))
			return false;
		if (relationshipNames == null) {
			if (other.relationshipNames != null)
				return false;
		} else if (!relationshipNames.equals(other.relationshipNames))
			return false;
		return true;
	}
	
}
