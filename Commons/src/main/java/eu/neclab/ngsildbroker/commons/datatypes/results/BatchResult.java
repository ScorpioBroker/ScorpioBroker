package eu.neclab.ngsildbroker.commons.datatypes.results;

import java.util.ArrayList;
import java.util.List;

public class BatchResult {

	private ArrayList<String> success = new ArrayList<String>();
	private ArrayList<BatchFailure> fails = new ArrayList<BatchFailure>();

	public void addSuccess(String entityId) {
		success.add(entityId);
	}

	public void addFail(BatchFailure fail) {
		fails.add(fail);
	}

	public void addSuccesses(List<String> entityId) {
		success.addAll(entityId);
	}

	public void addFails(List<BatchFailure> fail) {
		fails.addAll(fail);
	}

	public ArrayList<String> getSuccess() {
		return success;
	}

	public ArrayList<BatchFailure> getFails() {
		return fails;
	}

}
