package eu.neclab.ngsildbroker.commons.datatypes.results;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import io.vertx.mutiny.core.MultiMap;

public class CreateResult extends CRUDBaseResult {
	String entityId;

	public CreateResult(String entityId) {
		this(entityId, null, null, null);
	}

	public CreateResult(String entityId, String endpoint, MultiMap headers, String cSourceId) {
		super(endpoint, headers, cSourceId);
		json.put(NGSIConstants.QUERY_PARAMETER_ID, entityId);
	}

	public String getEntityId() {
		return entityId;
	}

	@Override
	public Object getJson() {
		return json;
	}

}
