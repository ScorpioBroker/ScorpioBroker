package eu.neclab.ngsildbroker.commons.datatypes.results;

import java.util.List;

import eu.neclab.ngsildbroker.commons.enums.ErrorType;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:13:22
 */
public class QueryResult extends BaseResult {

	public QueryResult(List<String> dataString, String errorMsg, ErrorType errorType, int shortErrorMsg,
			boolean success) {
		super(errorMsg, errorType, shortErrorMsg, success);
		this.dataString = dataString;
	}

	private String qToken;
	private Integer limit;
	private Integer offset;
	private Long resultsLeftAfter;
	private Long resultsLeftBefore;
	private List<String> dataString;
	private List<String> actualDataString;
	private Long count = 0l;

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

	public List<String> getDataString() {
		return dataString;
	}

	public void setDataString(List<String> dataString) {
		this.dataString = dataString;
	}

	public List<String> getActualDataString() {
		return actualDataString;
	}

	public void setActualDataString(List<String> actualDataString) {
		this.actualDataString = actualDataString;
	}

	public Long getCount() {
		return count;
	}

	public void setCount(Long count) {
		this.count = count;
	}

	public void finalize() throws Throwable {

	}
}