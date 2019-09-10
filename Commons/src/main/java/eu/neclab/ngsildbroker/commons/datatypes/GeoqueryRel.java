package eu.neclab.ngsildbroker.commons.datatypes;

public class GeoqueryRel {
	private String georelOp = null;
	private String distanceType = null;
	private String distanceValue = null;
	public String getGeorelOp() {
		return georelOp;
	}
	public void setGeorelOp(String georelOp) {
		this.georelOp = georelOp;
	}
	public String getDistanceType() {
		return distanceType;
	}
	public void setDistanceType(String distanceType) {
		this.distanceType = distanceType;
	}
	public String getDistanceValue() {
		return distanceValue;
	}
	public void setDistanceValue(String distanceValue) {
		this.distanceValue = distanceValue;
	}
	
	
}