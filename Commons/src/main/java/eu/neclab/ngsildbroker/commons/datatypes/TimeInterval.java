package eu.neclab.ngsildbroker.commons.datatypes;

import java.util.Date;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:13:23
 */
public class TimeInterval {

	private Date startAt;
	private Date stop;

	public TimeInterval(){

	}
	
	public Date getStartAt() {
		return startAt;
	}

	public void setStartAt(Date startAt) {
		this.startAt = startAt;
	}

	public Date getStop() {
		return stop;
	}

	public void setStop(Date stop) {
		this.stop = stop;
	}

	public void finalize() throws Throwable {

	}

}