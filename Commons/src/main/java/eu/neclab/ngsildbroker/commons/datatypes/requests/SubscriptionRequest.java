package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.interfaces.ScorpioBaseObject;

public class SubscriptionRequest extends BaseRequest {
	private Subscription subscription;
	private List<Object> context;

	public SubscriptionRequest() {
		// default constructor for serialization
	}

	public SubscriptionRequest(Subscription subscription, List<Object> context2,
			ArrayListMultimap<String, String> headers, int type) {
		super(headers, subscription.getId(), null, type);
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

	@Override
	public Object duplicate() {
		return new SubscriptionRequest(new Subscription(subscription), new ArrayList<Object>(context),
				ArrayListMultimap.create(headers), getRequestType());
	}

}
