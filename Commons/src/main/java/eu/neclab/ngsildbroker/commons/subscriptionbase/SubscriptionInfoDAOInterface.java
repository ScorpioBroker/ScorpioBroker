package eu.neclab.ngsildbroker.commons.subscriptionbase;

import java.util.List;
import java.util.Set;

import com.google.common.collect.Table;

import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

public interface SubscriptionInfoDAOInterface {

	public Table<String, String, Set<String>> getIds2Type() throws ResponseException;

	public List<String> getStoredSubscriptions();
	
	public void storeSubscription(SubscriptionRequest sub);
	
	public void deleteSubscription(SubscriptionRequest sub);

	public List<String> getEntriesFromSub(SubscriptionRequest subscriptionRequest) throws ResponseException;
}