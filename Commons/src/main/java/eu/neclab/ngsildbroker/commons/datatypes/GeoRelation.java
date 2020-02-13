package eu.neclab.ngsildbroker.commons.datatypes;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:13:22
 */
public class GeoRelation {

	private Double maxDistance;
	private Double minDistance;
	private String relation;

	public GeoRelation() {

	}

	public void finalize() throws Throwable {

	}

	public Double getMaxDistance() {
		return maxDistance;
	}

	public void setMaxDistance(Double maxDistance) {
		this.maxDistance = maxDistance;
	}

	public Double getMinDistance() {
		return minDistance;
	}

	public void setMinDistance(Double minDistance) {
		this.minDistance = minDistance;
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
			if (maxDistance % 1 == 0) {
				result.append(maxDistance.intValue());
			} else {
				result.append(maxDistance);
			}
		}
		if (minDistance != null) {
			result.append(";");
			result.append("minDistance");
			result.append("==");
			if (minDistance % 1 == 0) {
				result.append(minDistance.intValue());
			} else {
				result.append(minDistance);
			}
		}
		return result.toString();

	}

}