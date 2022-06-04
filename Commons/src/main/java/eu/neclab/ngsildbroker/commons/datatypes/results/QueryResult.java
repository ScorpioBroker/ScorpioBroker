package eu.neclab.ngsildbroker.commons.datatypes.results;

import java.util.List;
import java.util.Map;

import eu.neclab.ngsildbroker.commons.enums.ErrorType;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:13:22
 */
public class QueryResult extends BaseResult {

	private String qToken;
	private Integer limit;
	private Integer offset;
	private Integer resultsLeftAfter;
	private Integer resultsLeftBefore;
	private List<Map<String, Object>> data;
	private Integer count = 0;

	public QueryResult(List<Map<String, Object>> data, String errorMsg, ErrorType errorType, int shortErrorMsg,
			boolean success) {
		super(errorMsg, errorType, shortErrorMsg, success);
		this.data = data;
	}

	public Integer getResultsLeftBefore() {
		return resultsLeftBefore;
	}

	public void setResultsLeftBefore(Integer resultsLeftBefore) {
		this.resultsLeftBefore = resultsLeftBefore;
	}

	public Integer getResultsLeftAfter() {
		return resultsLeftAfter;
	}

	public void setResultsLeftAfter(Integer resultsLeft) {
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

	public Integer getCount() {
		return count;
	}

	public void setCount(Integer count) {
		this.count = count;
	}

	public void finalize() throws Throwable {

	}
}