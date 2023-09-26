package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.util.Map;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.tools.EntityTools;

public class CreateCSourceRequest extends CSourceRequest {

	/**
	 * 
	 */
	private static final long serialVersionUID = -9047230753068234324L;

	public CreateCSourceRequest(String tenant, Map<String, Object> resolved) {
		super(tenant, (String) resolved.get(NGSIConstants.JSON_LD_ID), resolved,
				AppConstants.CREATE_REQUEST);
		EntityTools.addSysAttrs(getPayload(), getSendTimestamp());
	}

}
