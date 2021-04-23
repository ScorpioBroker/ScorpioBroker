package eu.neclab.ngsildbroker.commons.datatypes;

import java.util.List;

import com.google.common.collect.ArrayListMultimap;

public class SubscriptionRequest extends BaseRequest{
	Subscription subscription;
	List<Object> context;
	
	public SubscriptionRequest(Subscription subscription, List<Object> context, ArrayListMultimap<String, String> headers) {
		super(headers);
		this.context = context;
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
	
	
	
}
