package eu.neclab.ngsildbroker.commons.datatypes;

import java.util.Date;

import eu.neclab.ngsildbroker.commons.enums.TemporalRelation;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:13:22
 */
public class LDTemporalQuery {

	private Date endTimeAt;
	private TemporalRelation temprel;
	private Date timeAt;
	private String timeProperty = "observedAt";

	public LDTemporalQuery(){

	}

	public void finalize() throws Throwable {

	}

	public Date getEndTimeAt() {
		return endTimeAt;
	}

	public void setEndTime(Date endTime) {
		this.endTimeAt = endTime;
	}

	public TemporalRelation getTemprel() {
		return temprel;
	}

	public void setTemprel(TemporalRelation temprel) {
		this.temprel = temprel;
	}

	public Date getTimeAt() {
		return timeAt;
	}

	public void setTimeAt(Date timeAt) {
		this.timeAt = timeAt;
	}

	public String getTimeProperty() {
		return timeProperty;
	}

	public void setTimeProperty(String timeProperty) {
		this.timeProperty = timeProperty;
	}
	

}