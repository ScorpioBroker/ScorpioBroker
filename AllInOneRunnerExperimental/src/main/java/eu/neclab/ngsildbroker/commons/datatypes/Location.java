package eu.neclab.ngsildbroker.commons.datatypes;


/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:13:22
 */
public class Location {

	private double latitude;
	private double longitude;

	public Location(){

	}

	public void finalize() throws Throwable {

	}

	public double getLatitude() {
		return latitude;
	}

	public void setLatitude(double latitude) {
		this.latitude = latitude;
	}

	public double getLongitude() {
		return longitude;
	}

	public void setLongitude(double longitude) {
		this.longitude = longitude;
	}

	
}