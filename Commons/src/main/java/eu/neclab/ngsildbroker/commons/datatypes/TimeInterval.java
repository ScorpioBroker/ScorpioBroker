package eu.neclab.ngsildbroker.commons.datatypes;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:13:23
 */
public class TimeInterval {

	private Long startAt;
	private Long stop;

	public TimeInterval(){

	}
	
	public Long getStartAt() {
		return startAt;
	}

	public void setStartAt(Long startAt) {
		this.startAt = startAt;
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