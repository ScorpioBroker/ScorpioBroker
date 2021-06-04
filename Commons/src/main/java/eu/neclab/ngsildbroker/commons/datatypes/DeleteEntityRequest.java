package eu.neclab.ngsildbroker.commons.datatypes;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

public class DeleteEntityRequest extends EntityRequest {
	public DeleteEntityRequest() {
		super(AppConstants.OPERATION_DELETE_ENTITY, null);
	}

	public DeleteEntityRequest(String entityid, ArrayListMultimap<String, String> headers) throws ResponseException {
		super(AppConstants.OPERATION_DELETE_ENTITY, headers);
		this.id=entityid;
		this.keyValue=null;

	}

}
