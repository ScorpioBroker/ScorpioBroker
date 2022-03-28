package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.util.List;

import com.google.common.collect.ArrayListMultimap;
import com.google.gson.annotations.Expose;

import eu.neclab.ngsildbroker.commons.datatypes.Subscription;

public class SubscriptionRequest extends BaseRequest {
	@Expose
	private Subscription subscription;
	@Expose
	private List<Object> context;

	private boolean active;

	private int type;

	public SubscriptionRequest() {
		// default constructor for serialization
	}

	public SubscriptionRequest(Subscription subscription, List<Object> context2,
			ArrayListMultimap<String, String> headers, int type) {
		super(headers, subscription.getId(), null, type);
		this.active = true;
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

	public boolean isActive() {
		return active;
	}

	public void setActive(boolean active) {
		this.active = active;
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

}
