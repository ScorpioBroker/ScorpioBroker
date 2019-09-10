package eu.neclab.ngsildbroker.commons.datatypes;

import java.net.URI;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:13:22
 */
public class Relationship extends BaseProperty {

	private URI object;
	

	public Relationship(){
		type = "Relationship";
	}

	public void finalize() throws Throwable {

	}

	public URI getObject() {
		return object;
	}

	public void setObject(URI object) {
		this.object = object;
	}

	
}