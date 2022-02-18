package eu.neclab.ngsildbroker.commons.subscriptionbase;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.datatypes.results.QueryResult;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.ngsiqueries.ParamsResolver;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;
import eu.neclab.ngsildbroker.commons.storage.StorageDAO;
import io.vertx.mutiny.pgclient.PgPool;
import io.vertx.mutiny.sqlclient.Tuple;

public abstract class BaseSubscriptionInfoDAO extends StorageDAO implements SubscriptionInfoDAOInterface {
	private final static Logger logger = LoggerFactory.getLogger(BaseSubscriptionInfoDAO.class);

	public Table<String, String, Set<String>> getIds2Type() throws ResponseException {
		Table<String, String, Set<String>> result = HashBasedTable.create();
		String sql = getSQLForTypes();

		for (Entry<String, PgPool> tenant2Client : clientManager.getAllClients().entrySet()) {
			tenant2Client.getValue().query(sql).executeAndAwait().forEach(t -> {
				Object id = t.getValue("id");
				Object type = t.getValue("type");
				if (id != null && type != null) {
					addToResult(result, tenant2Client.getKey(), id.toString(), type.toString());
				}
			});
		}

		return result;
	}

	protected abstract String getSQLForTypes();

	private void addToResult(Table<String, String, Set<String>> result, String key, String id, String type) {
		Set<String> value = result.get(key, id);

		if (value == null) {
			value = new HashSet<String>();
			result.put(key, id, value);
		}
		value.add(type);
	}

	public List<String> getStoredSubscriptions() {
		ArrayList<String> result = new ArrayList<String>();
		for (Entry<String, PgPool> tenant2Client : clientManager.getAllClients().entrySet()) {
			PgPool client = tenant2Client.getValue();
			client.query("SELECT subscription_request FROM subscriptions").executeAndAwait().forEach(r -> {
				result.add(r.getString(0));
			});
		}
		return result;
	}

	public void storeSubscriptions(Table<String, String, SubscriptionRequest> tenant2subscriptionId2Subscription) {
		Set<String> tenants = tenant2subscriptionId2Subscription.rowKeySet();
		for (String tenant : tenants) {
			Map<String, SubscriptionRequest> row = tenant2subscriptionId2Subscription.row(tenant);
			PgPool client = clientManager.getClient(tenant, true);
			client.query("DELETE FROM subscriptions").executeAndAwait();
			for (Entry<String, SubscriptionRequest> entry : row.entrySet()) {
				client.preparedQuery("INSERT INTO subscriptions (subscription_id, subscription_request) VALUES (?, ?)")
						.executeAndAwait(Tuple.of(entry.getKey(), DataSerializer.toJson(entry.getValue())));
			}
		}
	}

	@Override
	public List<String> getEntriesFromSub(SubscriptionRequest subscriptionRequest) throws ResponseException {
		String tenant = subscriptionRequest.getTenant();
		if (AppConstants.INTERNAL_NULL_KEY.equals(tenant)) {
			tenant = null;
		}
		Subscription subscription = subscriptionRequest.getSubscription();
		List<QueryParams> qps = ParamsResolver.getQueryParamsFromSubscription(subscription);
		return query(qps, tenant);
	}

	private List<String> query(List<QueryParams> qps, String tenant) throws ResponseException {
		ArrayList<String> result = new ArrayList<String>();
		for (QueryParams qp : qps) {
			qp.setTenant(tenant);
			QueryResult qr = query(qp).await().atMost(Duration.ofMillis(500));
			List<String> resultString = qr.getActualDataString();
			if (resultString != null) {
				result.addAll(resultString);
			}
		}
		return result;
	}
}
