package eu.neclab.ngsildbroker.commons.datatypes.requests.subscription;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;

public class DeleteSubscriptionRequest extends SubscriptionRequest {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7208223686502399741L;

	public DeleteSubscriptionRequest(String tenant, String subscriptionId) {
		super(tenant, subscriptionId, AppConstants.DELETE_SUBSCRIPTION_REQUEST);
	}

}
