package eu.neclab.ngsildbroker.commons.datatypes;

import java.util.List;

import eu.neclab.ngsildbroker.commons.enums.Geometry;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:13:22
 */
public class LDGeoQuery {

	private List<Double> coordinates;
	private Geometry geometry;
	private String geoProperty = "location";
	private GeoRelation geoRelation;
//	private boolean nextAnd = true;
//	private LDGeoQuery next;

	public LDGeoQuery(){

	}

	
	
//	public boolean isNextAnd() {
//		return nextAnd;
//	}
//
//
//
//	public void setNextAnd(boolean nextAnd) {
//		this.nextAnd = nextAnd;
//	}
//
//
//
//	public LDGeoQuery getNext() {
//		return next;
//	}
//
//
//
//	public void setNext(LDGeoQuery next) {
//		this.next = next;
//	}



	public void finalize() throws Throwable {

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