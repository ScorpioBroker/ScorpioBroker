package eu.neclab.ngsildbroker.commons.datatypes;

import java.util.Map;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:13:22
 */
public class BaseOperation {

	protected Map<String, String> customFlags;
	
	public BaseOperation() {
		
	}

	public BaseOperation(Map<String, String> customFlags){
		this.customFlags = customFlags;

	}

	
	public Map<String, String> getCustomFlags() {
		return customFlags;
	}


	public void setCustomFlags(Map<String, String> customFlags) {
		this.customFlags = customFlags;
	}


	public void finalize() throws Throwable {

	}

}