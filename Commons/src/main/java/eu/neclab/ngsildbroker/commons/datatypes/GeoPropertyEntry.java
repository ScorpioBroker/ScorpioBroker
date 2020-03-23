package eu.neclab.ngsildbroker.commons.datatypes;

import com.github.filosganga.geogson.model.Geometry;

public class GeoPropertyEntry extends BaseEntry {
	public GeoPropertyEntry(String dataSetId, String value, Geometry<?> geoValue) {
		super(dataSetId);
		this.value = value;
		this.geoValue = geoValue;
	}

	private String value;

	private Geometry<?> geoValue;

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

}
