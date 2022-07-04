package eu.neclab.ngsildbroker.commons.datatypes.results;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

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

	public String toJsonString() {
		try {
			return JsonUtils.toPrettyString(toJson());
		} catch (IOException e) {
			return null;
		}
	}

	public Map<String, Object> toJson() {
		Map<String, Object> top = Maps.newHashMap();
		top.put("success", getSuccess());
		top.put("errors", getJsonFails());
		return top;
	}

	private List<Map<String, Object>> getJsonFails() {
		List<Map<String, Object>> result = Lists.newArrayList();
		for (BatchFailure fail : fails) {

			result.add(fail.toJson());

		}
		return result;
	}

}
