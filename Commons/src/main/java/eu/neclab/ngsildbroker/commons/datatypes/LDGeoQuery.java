package eu.neclab.ngsildbroker.commons.datatypes;

import java.util.List;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.enums.Geometry;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:13:22
 */
public class LDGeoQuery {

	private List<Double> coordinates;
	private Geometry geometry;
	private String geoProperty = NGSIConstants.NGSI_LD_LOCATION;
	private GeoRelation geoRelation;

	public LDGeoQuery(){

	}

	public List<Double> getCoordinates() {
		return coordinates;
	}

	public void setCoordinates(List<Double> coordinates) {
		this.coordinates = coordinates;
	}

	public Geometry getGeometry() {
		return geometry;
	}

	public void setGeometry(Geometry geometry) {
		this.geometry = geometry;
	}

	public String getGeoProperty() {
		return geoProperty;
	}

	public void setGeoProperty(String geoProperty) {
		this.geoProperty = geoProperty;
	}

	public GeoRelation getGeoRelation() {
		return geoRelation;
	}

	public void setGeoRelation(GeoRelation geoRelation) {
		this.geoRelation = geoRelation;
	}

	
}