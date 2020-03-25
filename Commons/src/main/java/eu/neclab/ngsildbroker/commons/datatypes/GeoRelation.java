package eu.neclab.ngsildbroker.commons.datatypes;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:13:22
 */
public class GeoRelation {

	private Object maxDistance;
	private Object minDistance;
	private String relation;

	public GeoRelation() {

	}

	public void finalize() throws Throwable {

	}

	public Object getMaxDistance() {
		return maxDistance;
	}

	public void setMaxDistance(Object maxDistance) {
		this.maxDistance = maxDistance;
	}

	public Object getMinDistance() {
		return minDistance;
	}

	public void setMinDistance(Object minDistance) {
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