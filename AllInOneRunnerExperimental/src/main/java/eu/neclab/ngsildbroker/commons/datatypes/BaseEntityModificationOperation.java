package eu.neclab.ngsildbroker.commons.datatypes;

import java.net.URI;
import java.util.List;
import java.util.Map;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:13:22
 */
public class BaseEntityModificationOperation extends BaseOperation {

	private List<BaseProperty> data;
	private URI id;
	private LDContext ldContext;

	

	public BaseEntityModificationOperation(Map<String, String> customFlags, List<BaseProperty> data, URI id,
			LDContext ldContext) {
		super(customFlags);
		this.data = data;
		this.id = id;
		this.ldContext = ldContext;
	}



	public void finalize() throws Throwable {

	}



	public List<BaseProperty> getData() {
		return data;
	}



	public void setData(List<BaseProperty> data) {
		this.data = data;
	}



	public URI getId() {
		return id;
	}



	public void setId(URI id) {
		this.id = id;
	}



	public LDContext getLdContext() {
		return ldContext;
	}



	public void setLdContext(LDContext ldContext) {
		this.ldContext = ldContext;
	}
	
	

}