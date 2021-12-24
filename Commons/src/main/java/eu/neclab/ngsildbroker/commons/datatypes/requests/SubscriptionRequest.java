package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.util.List;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.datatypes.Subscription;

public class SubscriptionRequest extends BaseRequest{
	private Subscription subscription;
	private List<Object> context;
	

	public SubscriptionRequest(Subscription subscription, List<Object> context2,
			ArrayListMultimap<String, String> headers) {
		super(headers, subscription.getId(), null, -1);
		this.context = context2;
		this.subscription = subscription;
	}

	public List<Object> getContext() {
		return context;
	}

	public void setContext(List<Object> context) {
		this.context = context;
	}

	public Subscription getSubscription() {
		return subscription;
	}

	public void setSubscription(Subscription subscription) {
		this.subscription = subscription;
	}

	public ArrayListMultimap<String, String> getHeaders() {
		return headers;
	}

	public void setHeaders(ArrayListMultimap<String, String> headers) {
		this.headers = headers;
	}

}
