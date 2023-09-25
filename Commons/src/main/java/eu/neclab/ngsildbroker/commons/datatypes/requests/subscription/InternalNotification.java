package eu.neclab.ngsildbroker.commons.datatypes.requests.subscription;

import java.util.Map;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;

public class InternalNotification extends BaseRequest {

	

	/**
	 * 
	 */
	private static final long serialVersionUID = -6832696880800268499L;

	public InternalNotification() {
		// for serialzation
	}

	public InternalNotification(String tenant, String subscriptionId, Map<String, Object> notification) {
		super(tenant, subscriptionId, notification, AppConstants.INTERNAL_NOTIFICATION_REQUEST);
	}

}
