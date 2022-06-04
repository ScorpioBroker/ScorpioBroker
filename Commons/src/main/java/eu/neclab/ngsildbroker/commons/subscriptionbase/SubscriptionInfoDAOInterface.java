package eu.neclab.ngsildbroker.commons.subscriptionbase;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Table;

import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import io.smallrye.mutiny.Uni;

public interface SubscriptionInfoDAOInterface {

	public Uni<Table<String, String, Set<String>>> getIds2Type();

	public Uni<List<String>> getStoredSubscriptions();

	public Uni<Void> storeSubscription(SubscriptionRequest sub);

	public Uni<Void> deleteSubscription(SubscriptionRequest sub);

	public Uni<List<Map<String, Object>>> getEntriesFromSub(SubscriptionRequest subscriptionRequest);
}