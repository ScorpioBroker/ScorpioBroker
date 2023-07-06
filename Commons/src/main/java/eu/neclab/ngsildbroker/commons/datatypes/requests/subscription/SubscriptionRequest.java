package eu.neclab.ngsildbroker.commons.datatypes.requests.subscription;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonSetter;
import com.github.jsonldjava.core.Context;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

public class SubscriptionRequest extends BaseRequest {

	private Subscription subscription;

	private Context context;

	public SubscriptionRequest() {
		// default constructor for serialization
	}

	public SubscriptionRequest(String tenant, Map<String, Object> subscription, Context context)
			throws ResponseException {
		super(tenant, (String) subscription.get(NGSIConstants.JSON_LD_ID), subscription, null,
				AppConstants.CREATE_SUBSCRIPTION_REQUEST);
		this.context = context;
		this.subscription = Subscription.expandSubscription(subscription, context, false);

	}

	protected SubscriptionRequest(String tenant, String subscriptionId, int deleteSubscriptionRequest) {
		super(tenant, subscriptionId, null, null, deleteSubscriptionRequest);
	}

	@Override
	public void setPayload(Map<String, Object> payload) {
		super.setPayload(payload);
		try {
			if (payload != null)
				this.subscription = Subscription.expandSubscription(payload, context, false);
		} catch (ResponseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@JsonSetter("payload")
	void setPayloadForDeserialize(Map<String, Object> payload) {
		super.setPayload(payload);
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

	@Override
	public String toString() {
		return "SubscriptionRequest [subscription=" + subscription + ", context=" + context + "]";
	}

}
