package eu.neclab.ngsildbroker.commons.datatypes.results;

import java.util.List;

import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

public class NGSILDOperationResult {
	List<Object> successes;
	List<ResponseException> failures;

	public List<Object> getSuccesses() {
		return successes;
	}

	public void setSuccesses(List<Object> successes) {
		this.successes = successes;
	}

	public void addSuccess(Object success) {
		this.successes.add(success);
	}

	public List<ResponseException> getFailures() {
		return failures;
	}

	public void setFailures(List<ResponseException> failures) {
		this.failures = failures;
	}

	public void addFailure(ResponseException failure) {
		this.failures.add(failure);
	}

}
