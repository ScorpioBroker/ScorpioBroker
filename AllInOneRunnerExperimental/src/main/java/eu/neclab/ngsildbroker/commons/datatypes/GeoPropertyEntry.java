package eu.neclab.ngsildbroker.commons.datatypes;

import com.github.filosganga.geogson.model.Geometry;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;

public class GeoPropertyEntry extends BaseEntry {
	public GeoPropertyEntry(String dataSetId, String value, Geometry<?> geoValue) {
		super(dataSetId);
		this.value = value;
		this.geoValue = geoValue;
		this.type = NGSIConstants.NGSI_LD_GEOPROPERTY;
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
