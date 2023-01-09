package eu.neclab.ngsildbroker.commons.datatypes.requests.subscription;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;

public class DeleteSubscriptionRequest extends SubscriptionRequest {

	public DeleteSubscriptionRequest(String tenant, String subscriptionId) {
		super(tenant, subscriptionId, AppConstants.DELETE_SUBSCRIPTION_REQUEST);
	}

}
