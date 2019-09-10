package eu.neclab.ngsildbroker.commons.datatypes;

import java.util.List;

public class SubscriptionRequest {
	Subscription subscription;
	List<Object> context;
	
	public SubscriptionRequest(Subscription subscription, List<Object> context) {
		this.subscription = subscription;
		this.context = context;
	}
	public Subscription getSubscription() {
		return subscription;
	}
	public void setSubscription(Subscription subscription) {
		this.subscription = subscription;
	}
	public List<Object> getContext() {
		return context;
	}
	public void setContext(List<Object> context) {
		this.context = context;
	}
	
	
}
