package eu.neclab.ngsildbroker.commons.datatypes;

import java.util.Date;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:13:23
 */
public class TimeInterval {

	private Long start;
	private Long stop;

	public TimeInterval(){

	}
	
	public Long getStart() {
		return start;
	}

	public void setStart(Long start) {
		this.start = start;
	}
	
	public Long getStop() {
		return stop;
	}

	public void setStop(Long stop) {
		this.stop = stop;
	}

	public void finalize() throws Throwable {

	}

}