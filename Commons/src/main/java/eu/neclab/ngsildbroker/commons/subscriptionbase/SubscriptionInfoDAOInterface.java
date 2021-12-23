package eu.neclab.ngsildbroker.commons.subscriptionbase;

import java.util.List;
import java.util.Set;

import com.google.common.collect.Table;

import eu.neclab.ngsildbroker.commons.datatypes.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

public interface SubscriptionInfoDAOInterface {

	public Table<String, String, Set<String>> getIds2Type() throws ResponseException;

	public List<String> getStoredSubscriptions();

	public void storedSubscriptions(Table<String, String, SubscriptionRequest> tenant2subscriptionId2Subscription);
}