package eu.neclab.ngsildbroker.commons.datatypes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

import eu.neclab.ngsildbroker.commons.enums.ErrorType;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:13:22
 */
public class QueryResult extends BaseResult {

	private ObjectMapper objectMapper = new ObjectMapper();
	private String qToken;
	private Integer limit;
	private Integer offset;
	private Integer resultsLeftAfter;
	private Integer resultsLeftBefore;
	private List<String> dataString;
	private ArrayNode data;
	private List<String> actualDataString;
	private Integer count;
	private ArrayNode actualData;

	public QueryResult(ArrayNode data, String errorMsg, ErrorType errorType, int shortErrorMsg, boolean success)  {
		super(errorMsg, errorType, shortErrorMsg, success);
		try {
			setData(data);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public QueryResult(List<String> data, String errorMsg, ErrorType errorType, int shortErrorMsg, boolean success)  {
		super(errorMsg, errorType, shortErrorMsg, success);
		try {
			setDataString(data);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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


	public ArrayNode getData() {
		return data;
	}

	public void setData(ArrayNode data) throws JsonProcessingException {
		this.data = data;
		this.dataString = new ArrayList<String>();
		writeToList(data, this.dataString);
	}

	public ArrayNode getActualData() {
		return actualData;
	}
	
	public void setActualData(ArrayNode actualData) throws JsonProcessingException {
		this.actualData = actualData;
		this.actualDataString = new ArrayList<String>();
		writeToList(actualData, actualDataString);
	}
	
	private List<String> getDataString() {
		return dataString;
	}



	private void setDataString(List<String> dataString) throws IOException {
		this.dataString = dataString;
		this.data = objectMapper.createArrayNode();
		writeToArrayNode(dataString, data);
	}



	private void writeToArrayNode(List<String> dataList, ArrayNode data) throws IOException {
		for(String dataEntry: dataList) {
			data.add(objectMapper.readTree(dataEntry));
		}
	}



	public List<String> getActualDataString() {
		return actualDataString;
	}



	public void setActualDataString(List<String> actualDataString) throws IOException {
		this.actualDataString = actualDataString;
		this.actualData = objectMapper.createArrayNode();
		writeToArrayNode(actualDataString, actualData);
	}



	public Integer getCount() {
		return count;
	}
	private void writeToList(ArrayNode data, List<String> list) throws JsonProcessingException {
		Iterator<JsonNode> it = data.elements();
		while(it.hasNext()) {
			list.add(objectMapper.writeValueAsString(it.next()));
		}
	}
	public void setCount(Integer count) {
		this.count = count;
	}

	public void finalize() throws Throwable {

	}
}