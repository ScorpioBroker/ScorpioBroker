package eu.neclab.ngsildbroker.commons.datatypes.results;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

public class NGSILDOperationResult {
	private int operationType;
	private String entityId;

	private List<CRUDBaseResult> successes;
	private List<ResponseException> failures;

	public NGSILDOperationResult(int operationType, String entityId) {
		super();
		this.operationType = operationType;
		this.entityId = entityId;
	}

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
		Map<String, List<Object>> result = Maps.newHashMap();
		if (!successes.isEmpty()) {
			List<Object> temp = Lists.newArrayList();
			for (CRUDBaseResult entry : successes) {
				temp.add(entry.getJson());
			}
			result.put("success", temp);
		}
		if (!failures.isEmpty()) {
			List<Object> temp = Lists.newArrayList();
			for (ResponseException entry : failures) {
				temp.add(entry.getJson());
			}
			result.put("failure", temp);
		}
		return result;
	}

}
