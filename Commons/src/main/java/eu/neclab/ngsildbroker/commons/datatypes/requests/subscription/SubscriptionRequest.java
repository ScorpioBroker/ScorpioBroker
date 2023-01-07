package eu.neclab.ngsildbroker.commons.datatypes.requests.subscription;

import java.util.Map;

import com.github.jsonldjava.core.Context;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import io.quarkus.runtime.annotations.IgnoreProperty;

public class SubscriptionRequest extends BaseRequest {

	@IgnoreProperty
	private Subscription subscription;

	private Context context;

	private boolean isInternal;

	public SubscriptionRequest() {
		// default constructor for serialization
	}

	public SubscriptionRequest(String tenant, Map<String, Object> subscription, Context context, boolean isInternal)
			throws ResponseException {
		super(tenant, (String) subscription.get(NGSIConstants.JSON_LD_ID), subscription, null,
				AppConstants.CREATE_SUBSCRIPTION_REQUEST);
		this.context = context;
		this.subscription = Subscription.expandSubscription(subscription, context, false);
		this.isInternal = isInternal;
	}

	@Override
	public void setPayload(Map<String, Object> payload) {
		// TODO Auto-generated method stub
		super.setPayload(payload);
		try {
			this.subscription = Subscription.expandSubscription(payload, context, false);
		} catch (ResponseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public Subscription getSubscription() {
		return subscription;
	}

	public void setSubscription(Subscription subscription) {
		this.subscription = subscription;
	}

	public Context getContext() {
		return context;
	}

	public void setContext(Context context) {
		this.context = context;
	}

	public boolean isInternal() {
		return isInternal;
	}

	public void setInternal(boolean isInternal) {
		this.isInternal = isInternal;
	}

}
