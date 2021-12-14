package eu.neclab.ngsildbroker.commons.datatypes;

import java.net.URI;
import java.util.List;
import java.util.Map;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:13:22
 */
public class CSourceQuery extends Query {

	

	public CSourceQuery(Map<String, String> customFlags, List<String> attributeNames, List<EntityInfo> entities,
			String ldContext, LDGeoQuery ldGeoQuery, String ldQuery, LDTemporalQuery ldTempQuery,
			List<URI> requestorList) {
		super(customFlags, attributeNames, entities, ldContext, ldGeoQuery, ldQuery, ldTempQuery, requestorList);
		// TODO Auto-generated constructor stub
	}

	public void finalize() throws Throwable {

	}

}