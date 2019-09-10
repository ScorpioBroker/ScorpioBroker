package eu.neclab.ngsildbroker.commons.datatypes;

import java.util.Date;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:13:23
 */
public class TimeInterval {

	private Date start;
	private Date stop;

	public TimeInterval(){

	}
	
	public Date getStart() {
		return start;
	}

	public void setStart(Date start) {
		this.start = start;
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