package eu.neclab.ngsildbroker.commons.subscriptionbase;

import java.util.List;
import java.util.Set;

import com.google.common.collect.Table;

import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import io.smallrye.mutiny.Uni;

public interface SubscriptionInfoDAOInterface {

	public Uni<Table<String, String, Set<String>>> getIds2Type();

	public Uni<List<String>> getStoredSubscriptions();

	public Uni<Void> storeSubscription(SubscriptionRequest sub);

	public Uni<Void> deleteSubscription(SubscriptionRequest sub);

	public Uni<List<String>> getEntriesFromSub(SubscriptionRequest subscriptionRequest);
}