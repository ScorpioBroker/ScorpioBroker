package eu.neclab.ngsildbroker.commons.datatypes;

import java.util.List;

import eu.neclab.ngsildbroker.commons.enums.ErrorType;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:13:22
 */
public class QueryResult extends BaseResult {

	public QueryResult(List<String> dataString, String errorMsg, ErrorType errorType, int shortErrorMsg, boolean success) {
		super(errorMsg, errorType, shortErrorMsg, success);
		this.dataString = dataString;
	}

		
	private String qToken;
	private Integer limit;
	private Integer offset;
	private Integer resultsLeftAfter;
	private Integer resultsLeftBefore;

	private List<String> dataString;
	
	

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


	public List<String> getDataString() {
		return dataString;
	}

	public void setDataString(List<String> dataString) {
		this.dataString = dataString;
	}

	public void finalize() throws Throwable {

	}

}