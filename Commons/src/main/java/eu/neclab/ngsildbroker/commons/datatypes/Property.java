package eu.neclab.ngsildbroker.commons.datatypes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:13:22
 */
public class Property extends BaseProperty {

	
	

	private HashMap<String, PropertyEntry> dataSetId2value;
	public Property(){
		type = "Property";
	}

	public void finalize() throws Throwable {

	}
	
	public void setSingleEntry(PropertyEntry value) {
		HashMap<String, PropertyEntry> temp = new HashMap<String, PropertyEntry>();
		temp.put(value.getDataSetId(), value);
		setEntries(temp);
	}
	
	public HashMap<String, PropertyEntry> getEntries() {
		return dataSetId2value;
	}

	public void setEntries(HashMap<String, PropertyEntry> value) {
		this.dataSetId2value = value;
	}

	
	


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((dataSetId2value == null) ? 0 : dataSetId2value.hashCode());
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
		Property other = (Property) obj;
		if (dataSetId2value == null) {
			if (other.dataSetId2value != null)
				return false;
		} else if (!dataSetId2value.equals(other.dataSetId2value))
			return false;
		return true;
	}

	@Override
	public boolean isMultiValue() {
		return dataSetId2value.size() != 1;
	}

	

}