package eu.neclab.ngsildbroker.commons.datatypes;

import java.net.URI;
import java.util.HashMap;
import java.util.List;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:13:22
 */
public class Relationship extends BaseProperty {

	private HashMap<String, RelationshipEntry> dataSetId2object;

	public Relationship() {
		type = "Relationship";
	}

	public void finalize() throws Throwable {

	}

	public HashMap<String, RelationshipEntry> getEntries() {
		return dataSetId2object;
	}

	public void setObjects(HashMap<String, RelationshipEntry> objects) {
		this.dataSetId2object = objects;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((dataSetId2object == null) ? 0 : dataSetId2object.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		Relationship other = (Relationship) obj;
		if (dataSetId2object == null) {
			if (other.dataSetId2object != null)
				return false;
		} else if (!dataSetId2object.equals(other.dataSetId2object))
			return false;
		return true;
	}

	@Override
	public boolean isMultiValue() {
		return dataSetId2object.size() != 1;
	}

}