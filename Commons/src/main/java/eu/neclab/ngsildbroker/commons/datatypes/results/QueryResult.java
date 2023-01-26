package eu.neclab.ngsildbroker.commons.datatypes.results;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:13:22
 */
public class QueryResult {

	private String qToken;
	private Integer limit;
	private Integer offset;
	private Long resultsLeftAfter;
	private Long resultsLeftBefore;
	private List<Map<String, Object>> data;
	private Map<String, Map<String, Object>> entityId2Data = Maps.newHashMap();
	private Long count = 0L;

	public QueryResult() {
	}

	public Long getResultsLeftBefore() {
		return resultsLeftBefore;
	}

	public void setResultsLeftBefore(Long resultsLeftBefore) {
		this.resultsLeftBefore = resultsLeftBefore;
	}

	public Long getResultsLeftAfter() {
		return resultsLeftAfter;
	}

	public void setResultsLeftAfter(Long resultsLeft) {
		this.resultsLeftAfter = resultsLeft;
	}

	public String getqToken() {
		return qToken;
	}

	public void setqToken(String qToken) {
		this.qToken = qToken;
	}

	public Integer getLimit() {
		return limit;
	}

	public void setLimit(Integer limit) {
		this.limit = limit;
	}

	public Integer getOffset() {
		return offset;
	}

	public void setOffset(Integer offset) {
		this.offset = offset;
	}

	public List<Map<String, Object>> getData() {
		return data;
	}

	public void setData(List<Map<String, Object>> data) {
		this.data = data;
	}

	public Long getCount() {
		return count;
	}

	public void setCount(Long count) {
		this.count = count;
	}

	public Map<String, Map<String, Object>> getEntityId2Data() {
		return entityId2Data;
	}

	public void setEntityId2Data(Map<String, Map<String, Object>> entityId2Data) {
		this.entityId2Data = entityId2Data;
	}

//	public void finalize() throws Throwable {
//
//	}
}