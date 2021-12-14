package eu.neclab.ngsildbroker.commons.datatypes;

import eu.neclab.ngsildbroker.commons.enums.ErrorType;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:13:22
 */
public class DeleteResult extends BaseResult {

	public DeleteResult(Entity data, String errorMsg, ErrorType errorType, int shortErrorMsg, boolean success) {
		super(errorMsg, errorType, shortErrorMsg, success);
		this.data = data;
		
	}



	private Entity data;

	

	public Entity getData() {
		return data;
	}



	public void setData(Entity data) {
		this.data = data;
	}



	public void finalize() throws Throwable {

	}

}