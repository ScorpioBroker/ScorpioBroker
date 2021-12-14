package eu.neclab.ngsildbroker.commons.datatypes;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;

public class GeoqueryRel {
	private String georelOp = null;
	private String distanceType = null;
	private String distanceValue = null;
	
	
	public GeoqueryRel() {
		super();
	}
	
	
	public GeoqueryRel(GeoRelation georel) {
		super();
		this.georelOp = georel.getRelation();
		if(georel.getMaxDistance() != null && georel.getMaxDistanceAsDouble() > 0) {
			this.distanceType = NGSIConstants.GEO_REL_MAX_DISTANCE;
			this.distanceValue = "" + georel.getMaxDistance();
		}else if(georel.getMinDistance() != null && georel.getMinDistanceAsDouble() > 0) {
			this.distanceType = NGSIConstants.GEO_REL_MIN_DISTANCE;
			this.distanceValue = "" + georel.getMinDistance();
		}
	}


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