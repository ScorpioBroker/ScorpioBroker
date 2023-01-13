package eu.neclab.ngsildbroker.commons.datatypes.results;

import java.util.List;
import java.util.Map;

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

//	public void finalize() throws Throwable {
//
//	}
}