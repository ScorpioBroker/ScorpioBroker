package eu.neclab.ngsildbroker.commons.datatypes;

import java.io.Serializable;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:13:22
 */
public class GeoRelation implements Serializable{

	/**
	 *
	 */
	private static final long serialVersionUID = 5081747550503416154L;
	private Object maxDistance;
	private Double maxDistanceAsDouble;
	private Object minDistance;
	private Double minDistanceAsDouble;
	private String relation;

	public GeoRelation() {

	}

	public void finalize() throws Throwable {

	}

	public Object getMaxDistance() {
		return maxDistance;
	}

	public Double getMaxDistanceAsDouble() {
		return maxDistanceAsDouble;
	}

	public Double getMinDistanceAsDouble() {
		return minDistanceAsDouble;
	}

	public void setMaxDistance(Object maxDistance) {
		this.maxDistance = maxDistance;
		if (maxDistance instanceof Integer) {
			maxDistanceAsDouble = ((Integer) maxDistance).doubleValue();
		} else {
			maxDistanceAsDouble = (Double) maxDistance;
		}
	}

	public Object getMinDistance() {
		return minDistance;
	}

	public void setMinDistance(Object minDistance) {
		this.minDistance = minDistance;
		if (minDistance instanceof Integer) {
			minDistanceAsDouble = ((Integer) minDistance).doubleValue();
		} else {
			minDistanceAsDouble = (Double) minDistance;
		}
	}

	public String getRelation() {
		return relation;
	}

	public void setRelation(String relation) {
		this.relation = relation;
	}

	public String getABNFString() {
		StringBuilder result = new StringBuilder();
		result.append(relation);
		if (maxDistance != null) {
			result.append(";");
			result.append("maxDistance");
			result.append("==");
			result.append(maxDistance);
		}
		if (minDistance != null) {
			result.append(";");
			result.append("minDistance");
			result.append("==");
			result.append(minDistance);
		}
		return result.toString();

	}

}