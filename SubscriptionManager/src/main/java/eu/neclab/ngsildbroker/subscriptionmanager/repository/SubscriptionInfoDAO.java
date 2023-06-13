package eu.neclab.ngsildbroker.subscriptionmanager.repository;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import com.google.common.collect.Lists;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.DeleteSubscriptionRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.UpdateSubscriptionRequest;
import eu.neclab.ngsildbroker.commons.storage.ClientManager;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.mutiny.tuples.Tuple3;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.Tuple;

@ApplicationScoped
public class SubscriptionInfoDAO {

	@Inject
	ClientManager clientManager;

	public Uni<Void> createSubscription(SubscriptionRequest request, String contextId) {
		return clientManager.getClient(request.getTenant(), true).onItem().transformToUni(client -> {
			return client
					.preparedQuery(
							"INSERT INTO subscriptions(subscription_id, subscription, context) VALUES ($1, $2, $3)")
					.execute(Tuple.of(request.getId(), new JsonObject(request.getPayload()), contextId)).onItem()
					.transformToUni(rows -> Uni.createFrom().voidItem());
		});
	}

	public Uni<Tuple2<Map<String, Object>, Object>> updateSubscription(UpdateSubscriptionRequest request,
			String contextId) {
		return clientManager.getClient(request.getTenant(), false).onItem().transformToUni(client -> {
			return client.preparedQuery(
					"UPDATE subscriptions SET subscription=subscription || $2, context=$3 WHERE subscription_id=$1 RETURNING subscription")
					.execute(Tuple.of(request.getId(), new JsonObject(request.getPayload()), contextId)).onItem()
					.transform(i -> Tuple2.of(request.getPayload(), request.getContext().serialize().get("@context")));
		});
	}

	public Uni<RowSet<Row>> deleteSubscription(DeleteSubscriptionRequest request) {
		return clientManager.getClient(request.getTenant(), false).onItem()
				.transformToUni(client -> client.preparedQuery("DELETE FROM subscriptions WHERE subscription_id=$1")
						.execute(Tuple.of(request.getId())));
	}

	public Uni<RowSet<Row>> getAllSubscriptions(String tenant, int limit, int offset) {
		return clientManager.getClient(tenant, false).onItem().transformToUni(
				client -> client.preparedQuery("SELECT subscription FROM subscriptions LIMIT $1 OFFSET $2")
						.execute(Tuple.of(limit, offset)).onFailure().retry().atMost(3));
	}

	public Uni<RowSet<Row>> getSubscription(String tenant, String subscriptionId) {
		return clientManager.getClient(tenant, false).onItem()
				.transformToUni(client -> client
						.preparedQuery("SELECT subscription FROM subscriptions WHERE subscription_id=$1")
						.execute(Tuple.of(subscriptionId)).onFailure().retry().atMost(3));
	}

	public Uni<Void> updateNotificationSuccess(String tenant, String id, String date) {
		return clientManager.getClient(tenant, false).onItem().transformToUni(client -> {
			return client
					.preparedQuery("UPDATE subscriptions SET subscription = subscription || ('{\""
							+ NGSIConstants.NGSI_LD_TIMES_SENT + "\": [{\"" + NGSIConstants.JSON_LD_VALUE
							+ "\": '|| (subscription#>>'{" + NGSIConstants.NGSI_LD_TIMES_SENT + ",0, "
							+ NGSIConstants.JSON_LD_VALUE + "}')::integer + 1 ||'}],\""
							+ NGSIConstants.NGSI_LD_LAST_SUCCESS + "\": [{\"" + NGSIConstants.JSON_LD_TYPE + "\": \""
							+ NGSIConstants.NGSI_LD_DATE_TIME + "\", \"" + NGSIConstants.JSON_LD_VALUE
							+ "\": \"$1\"}],\"" + NGSIConstants.NGSI_LD_LAST_NOTIFICATION + "\": [{\""
							+ NGSIConstants.JSON_LD_TYPE + "\": \"" + NGSIConstants.NGSI_LD_DATE_TIME + "\", \""
							+ NGSIConstants.JSON_LD_VALUE + "\": \"$1\"}]}')::jsonb WHERE subscription_id=$2")
					.execute(Tuple.of(date, id)).onFailure().retry().atMost(3).onItem()
					.transformToUni(t -> Uni.createFrom().voidItem());
		});
	}

	public Uni<Void> updateNotificationFailure(String tenant, String id, String date) {
		return clientManager.getClient(tenant, false).onItem().transformToUni(client -> client
				.preparedQuery("UPDATE subscriptions SET subscription = subscription || ('{\""
						+ NGSIConstants.NGSI_LD_TIMES_FAILED + "\": [{\"" + NGSIConstants.JSON_LD_VALUE
						+ "\": '|| (subscription#>>'{" + NGSIConstants.NGSI_LD_TIMES_FAILED + ",0, "
						+ NGSIConstants.JSON_LD_VALUE + "}')::integer + 1 ||'}],\"" + NGSIConstants.NGSI_LD_LAST_FAILURE
						+ "\": [{\"" + NGSIConstants.JSON_LD_TYPE + "\": \"" + NGSIConstants.NGSI_LD_DATE_TIME
						+ "\", \"" + NGSIConstants.JSON_LD_VALUE + "\": \"$1\"}],\""
						+ NGSIConstants.NGSI_LD_LAST_NOTIFICATION + "\": [{\"" + NGSIConstants.JSON_LD_TYPE + "\": \""
						+ NGSIConstants.NGSI_LD_DATE_TIME + "\", \"" + NGSIConstants.JSON_LD_VALUE
						+ "\": \"$1\"}]}')::jsonb WHERE subscription_id=$2")
				.execute(Tuple.of(date, id)).onFailure().retry().atMost(3).onItem()
				.transformToUni(t -> Uni.createFrom().voidItem()));
	}

	public Uni<List<Tuple3<String, Map<String, Object>, Map<String, Object>>>> loadSubscriptions() {
		return clientManager.getClient(AppConstants.INTERNAL_NULL_KEY, false).onItem().transformToUni(client -> {
			return client.preparedQuery("select tenant_id from tenant").execute().onItem().transformToUni(rows -> {
				List<Uni<RowSet<Row>>> unis = Lists.newArrayList();
				rows.forEach(row -> {
					unis.add(
							clientManager.getClient(row.getString(0), false).onItem()
									.transformToUni(tenantClient -> tenantClient
											.preparedQuery("SELECT '" + row.getString(0)
													+ "', subscriptions.subscription, context FROM subscriptions")
											.execute()));
				});
				unis.add(client.preparedQuery("SELECT '" + AppConstants.INTERNAL_NULL_KEY
						+ "', subscriptions.subscription, context FROM subscriptions").execute());

				return Uni.combine().all().unis(unis).combinedWith(list -> {
					List<Tuple3<String, Map<String, Object>, Map<String, Object>>> result = new ArrayList<>();

					return client.preparedQuery("select jsonb_object_agg(id,body) as col from public.contexts")
							.execute().onItem().transform(rows1 -> {
								JsonObject jsonContexts = rows1.iterator().next().getJsonObject(0);
								Map<String, Object> mapContexts;
								if (jsonContexts != null)
									mapContexts = jsonContexts.getMap();
								else
									return result;
								for (Object obj : list) {
									@SuppressWarnings("unchecked")
									RowSet<Row> rowset = (RowSet<Row>) obj;
									rowset.forEach(
											row -> result.add(Tuple3.of(row.getString(0), row.getJsonObject(1).getMap(),
													(Map<String, Object>) mapContexts.get(row.getString(2)))));
								}
								return result;
							});

				}).onItem().transformToUni(x -> x);
			});
		});

	}

}
