package eu.neclab.ngsildbroker.commons.datatypes.terms;

public class TemporalQueryTerm {
	String timeProperty;
	String timerel;
	String timeAt;
	String endTimeAt;

	public TemporalQueryTerm(String timeProperty, String timerel, String timeAt, String endTimeAt) {
		this.timeProperty = timeProperty;
		this.timerel = timerel;
		this.timeAt = timeAt;
		this.endTimeAt = endTimeAt;
	}

	public String getTimerel() {
		return timerel;
	}

	public void setTimerel(String timerel) {
		this.timerel = timerel;
	}

	public String getTimeProperty() {
		return timeProperty;
	}

	public void setTimeProperty(String timeProperty) {
		this.timeProperty = timeProperty;
	}

	public String getTimeAt() {
		return timeAt;
	}

	public void setTimeAt(String timeAt) {
		this.timeAt = timeAt;
	}

	public String getEndTimeAt() {
		return endTimeAt;
	}

	public void setEndTimeAt(String endTimeAt) {
		this.endTimeAt = endTimeAt;
	}

}
