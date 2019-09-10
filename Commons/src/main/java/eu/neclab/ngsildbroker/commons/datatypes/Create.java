package eu.neclab.ngsildbroker.commons.datatypes;

import java.util.Map;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:13:22
 */
public class Create extends BaseOperation {

	private Entity data;
	private LDContext ldContext;

	

	
	


	public Create(Map<String, String> customFlags, Entity data, LDContext ldContext) {
		super(customFlags);
		this.data = data;
		this.ldContext = ldContext;
	}


	public Entity getData() {
		return data;
	}


	public void setData(Entity data) {
		this.data = data;
	}


	public LDContext getLdContext() {
		return ldContext;
	}


	public void setLdContext(LDContext ldContext) {
		this.ldContext = ldContext;
	}


	public void finalize() throws Throwable {

	}

}