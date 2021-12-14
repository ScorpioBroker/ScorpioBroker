package eu.neclab.ngsildbroker.commons.datatypes;

import java.net.URI;
import java.util.List;
import java.util.Map;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:13:22
 */
public class Append extends BaseEntityModificationOperation {

	

	public Append(Map<String, String> customFlags, List<BaseProperty> data, URI id, LDContext ldContext) {
		super(customFlags, data, id, ldContext);
	}

	public void finalize() throws Throwable {

	}

}