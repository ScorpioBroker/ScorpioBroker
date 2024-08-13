package eu.neclab.ngsildbroker.commons.datatypes.requests.subscription;

import java.util.Map;

import com.github.jsonldjava.core.Context;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;

public class UpdateSubscriptionRequest extends SubscriptionRequest {

	/**
	 * 
	 */
	private static final long serialVersionUID = 53529246590592034L;

	public UpdateSubscriptionRequest(String tenant, String subscriptionId, Map<String, Object> update,
			Context context) {
		this.tenant = tenant;
		this.id = subscriptionId;
		this.payload = update;
		this.context = context;
		this.requestType = AppConstants.UPDATE_SUBSCRIPTION_REQUEST;
		

	}

}
