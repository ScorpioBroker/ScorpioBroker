package eu.neclab.ngsildbroker.commons.datatypes;

import java.util.ArrayList;

public class BatchResult {
	
	private ArrayList<String> success = new ArrayList<String>();
	private ArrayList<BatchFailure> fails = new ArrayList<BatchFailure>();
	
	public void addSuccess(String entityId) {
		success.add(entityId);
	}
	
	public void addFail(BatchFailure fail) {
		fails.add(fail);
	}
	
	public ArrayList<String> getSuccess() {
		return success;
	}
	public ArrayList<BatchFailure> getFails() {
		return fails;
	}
	
	
	
	
	
			

}
