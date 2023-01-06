package eu.neclab.ngsildbroker.commons.datatypes.requests.subscription;

import java.util.Map;

import com.github.jsonldjava.core.Context;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;

public class UpdateSubscriptionRequest extends BaseRequest {

	public UpdateSubscriptionRequest(String tenant, String subscriptionId, Map<String, Object> update, Context context) {
		super(tenant, subscriptionId, update, null, AppConstants.UPDATE_SUBSCRIPTION_REQUEST);
	}

}
