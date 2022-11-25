package eu.neclab.ngsildbroker.commons.datatypes.results;

import java.util.List;
import java.util.Map;

import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

public class NGSILDOperationResult {
	List<CRUDBaseResult> successes;
	List<ResponseException> failures;

	public List<CRUDBaseResult> getSuccesses() {
		return successes;
	}

	public void setSuccesses(List<CRUDBaseResult> successes) {
		this.successes = successes;
	}

	public void addSuccess(CRUDBaseResult success) {
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

	public Map<String, List<Object>> getJson() {
		// TODO Auto-generated method stub
		return null;
	}

}
