package eu.neclab.ngsildbroker.commons.datatypes;

import com.github.filosganga.geogson.model.Geometry;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;

public class GeoPropertyEntry extends BaseEntry {
	private Geometry<?> geoValue;

	public GeoPropertyEntry(String dataSetId, Geometry<?> geoValue) {
		super(dataSetId);

		this.geoValue = geoValue;
		this.type = NGSIConstants.NGSI_LD_GEOPROPERTY;
	}

	public Geometry<?> getGeoValue() {
		return geoValue;
	}

	public void setGeoValue(Geometry<?> geoValue) {
		this.geoValue = geoValue;
	}

}
