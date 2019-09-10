package eu.neclab.ngsildbroker.commons.datatypes;

import java.util.List;

import eu.neclab.ngsildbroker.commons.enums.ErrorType;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:13:22
 */
public class CSourceQueryResult extends BaseResult {

	public CSourceQueryResult(List<CSourceRegistration> data, String errorMsg, ErrorType errorType, int shortErrorMsg, boolean success) {
		super(errorMsg, errorType, shortErrorMsg, success);
		this.data = data;
	}



	public List<CSourceRegistration> getData() {
		return data;
	}



	public void setData(List<CSourceRegistration> data) {
		this.data = data;
	}



	private List<CSourceRegistration> data;



	public void finalize() throws Throwable {

	}

}