package eu.neclab.ngsildbroker.commons.datatypes;

import java.util.Date;

import eu.neclab.ngsildbroker.commons.enums.TemporalRelation;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:13:22
 */
public class LDTemporalQuery {

	private Date endTime;
	private TemporalRelation temprel;
	private Date time;
	private String timeProperty = "observedAt";

	public LDTemporalQuery(){

	}

	public void finalize() throws Throwable {

	}

	public Date getEndTime() {
		return endTime;
	}

	public void setEndTime(Date endTime) {
		this.endTime = endTime;
	}

	public TemporalRelation getTemprel() {
		return temprel;
	}

	public void setTemprel(TemporalRelation temprel) {
		this.temprel = temprel;
	}

	public Date getTime() {
		return time;
	}

	public void setTime(Date time) {
		this.time = time;
	}

	public String getTimeProperty() {
		return timeProperty;
	}

	public void setTimeProperty(String timeProperty) {
		this.timeProperty = timeProperty;
	}
	

}