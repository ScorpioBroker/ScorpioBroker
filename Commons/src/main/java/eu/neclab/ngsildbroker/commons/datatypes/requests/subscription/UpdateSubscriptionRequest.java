package eu.neclab.ngsildbroker.commons.datatypes.requests.subscription;

import java.util.Map;

import com.github.jsonldjava.core.Context;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;

public class UpdateSubscriptionRequest extends BaseRequest {

	/**
	 * 
	 */
	private static final long serialVersionUID = 53529246590592034L;
	private Context context;

	public UpdateSubscriptionRequest(String tenant, String subscriptionId, Map<String, Object> update,
			Context context) {
		super(tenant, subscriptionId, update, AppConstants.UPDATE_SUBSCRIPTION_REQUEST);
		this.context = context;
	}

	public Context getContext() {
		return context;
	}

	public void setContext(Context context) {
		this.context = context;
	}

}
