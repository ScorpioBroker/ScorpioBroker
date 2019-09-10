package eu.neclab.ngsildbroker.commons.datatypes;

import com.github.filosganga.geogson.model.Geometry;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:13:22
 */
public class GeoProperty extends BaseProperty {

	
	private String value;
	
	private Geometry<?> geoValue;

	public GeoProperty(){
//		this.type = "GeoProperty";
	}

	public void finalize() throws Throwable {

	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String string) {
		this.value = string;
	}


	public Geometry<?> getGeoValue() {
		return geoValue;
	}

	public void setGeoValue(Geometry<?> geoValue) {
		this.geoValue = geoValue;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((geoValue == null) ? 0 : geoValue.hashCode());
		result = prime * result + ((value == null) ? 0 : value.hashCode());
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
		GeoProperty other = (GeoProperty) obj;
		if (geoValue == null) {
			if (other.geoValue != null)
				return false;
		} else if (!geoValue.equals(other.geoValue))
			return false;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		return true;
	}
	

}