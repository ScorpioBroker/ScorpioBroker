package eu.neclab.ngsildbroker.commons.datatypes;

import java.util.HashMap;

import com.github.filosganga.geogson.model.Geometry;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:13:22
 */
public class GeoProperty extends BaseProperty {

	
	HashMap<String, GeoPropertyEntry> entries = new HashMap<String, GeoPropertyEntry>();
	
	public GeoProperty(){
//		this.type = "GeoProperty";
	}

	public void finalize() throws Throwable {

	}


	
	
	public HashMap<String, GeoPropertyEntry> getEntries() {
		return entries;
	}

	public void setEntries(HashMap<String, GeoPropertyEntry> entries) {
		this.entries = entries;
	}
	
	

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((entries == null) ? 0 : entries.hashCode());
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
		GeoProperty other = (GeoProperty) obj;
		if (entries == null) {
			if (other.entries != null)
				return false;
		} else if (!entries.equals(other.entries))
			return false;
		return true;
	}

	@Override
	public boolean isMultiValue() {
		return entries.size() != 1;
	}
	

}