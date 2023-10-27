package eu.neclab.ngsildbroker.subscriptionmanager.repository;

import com.google.common.collect.Lists;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.DeleteSubscriptionRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.UpdateSubscriptionRequest;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.storage.ClientManager;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.mutiny.tuples.Tuple3;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.Tuple;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
					"UPDATE subscriptions SET subscription=subscription || $2, context=$3 WHERE subscription_id=$1 RETURNING subscriptions.subscription")
					.execute(Tuple.of(request.getId(), new JsonObject(request.getPayload()), contextId)).onItem()
					.transform(i -> Tuple2.of(i.iterator().next().getJsonObject("subscription").getMap(),
							request.getContext().serialize().get("@context")));
		});
	}

	public Uni<RowSet<Row>> deleteSubscription(DeleteSubscriptionRequest request) {
		return clientManager.getClient(request.getTenant(), false).onItem()
				.transformToUni(client -> client.preparedQuery("DELETE FROM subscriptions WHERE subscription_id=$1")
						.execute(Tuple.of(request.getId())).onItem().transformToUni(rows -> {
							if (rows.rowCount() == 0) {
								return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound));
							}
							return Uni.createFrom().item(rows);
						}));
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
			String sql = "UPDATE subscriptions SET subscription = jsonb_set(jsonb_set(jsonb_set(subscription, '{"
					+ NGSIConstants.NGSI_LD_TIMES_SENT + "}', jsonb_build_array(jsonb_build_object('"
					+ NGSIConstants.JSON_LD_VALUE + "', (subscription #>> '{" + NGSIConstants.NGSI_LD_TIMES_SENT + ",0,"
					+ NGSIConstants.JSON_LD_VALUE + "}')::integer + 1)), true), '{" + NGSIConstants.NGSI_LD_LAST_SUCCESS
					+ "}', jsonb_build_array(jsonb_build_object('" + NGSIConstants.JSON_LD_TYPE + "', '"
					+ NGSIConstants.NGSI_LD_DATE_TIME + "', '" + NGSIConstants.JSON_LD_VALUE + "', $1::text)), true),'{"
					+ NGSIConstants.NGSI_LD_LAST_NOTIFICATION + "}', jsonb_build_array(jsonb_build_object('"
					+ NGSIConstants.JSON_LD_TYPE + "', '" + NGSIConstants.NGSI_LD_DATE_TIME + "', '"
					+ NGSIConstants.JSON_LD_VALUE + "', $1::text)), true) WHERE subscription_id=$2";
			return client.preparedQuery(sql).execute(Tuple.of(date, id)).onFailure().retry().atMost(3).onItem()
					.transformToUni(t -> Uni.createFrom().voidItem());
		});
	}

	public Uni<Void> updateNotificationFailure(String tenant, String id, String date) {
		String sql = "UPDATE subscriptions SET subscription = jsonb_set(jsonb_set(jsonb_set(subscription, '{"
				+ NGSIConstants.NGSI_LD_TIMES_FAILED + "}', jsonb_build_array(jsonb_build_object('"
				+ NGSIConstants.JSON_LD_VALUE + "', (subscription #>> '{" + NGSIConstants.NGSI_LD_TIMES_FAILED + ",0,"
				+ NGSIConstants.JSON_LD_VALUE + "}')::integer + 1)), true), '{" + NGSIConstants.NGSI_LD_LAST_FAILURE
				+ "}', jsonb_build_array(jsonb_build_object('" + NGSIConstants.JSON_LD_TYPE + "', '"
				+ NGSIConstants.NGSI_LD_DATE_TIME + "', '" + NGSIConstants.JSON_LD_VALUE + "', $1::text)), true),'{"
				+ NGSIConstants.NGSI_LD_LAST_NOTIFICATION + "}', jsonb_build_array(jsonb_build_object('"
				+ NGSIConstants.JSON_LD_TYPE + "', '" + NGSIConstants.NGSI_LD_DATE_TIME + "', '"
				+ NGSIConstants.JSON_LD_VALUE + "', $1::text)), true) WHERE subscription_id=$2";
		return clientManager.getClient(tenant, false).onItem()
				.transformToUni(client -> client.preparedQuery(sql).execute(Tuple.of(date, id)).onFailure().retry()
						.atMost(3).onItem().transformToUni(t -> Uni.createFrom().voidItem()));
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

	public Uni<Tuple2<Map<String, Object>, Map<String, Object>>> loadSubscription(String tenant, String id) {
		return clientManager.getClient(tenant, false).onItem().transformToUni(client -> {
			return client.preparedQuery("SELECT subscription, context FROM subscriptions WHERE id=$1")
					.execute(Tuple.of(id)).onItem().transformToUni(rows -> {
						if (rows.size() == 0) {
							Tuple2<Map<String, Object>, Map<String, Object>> r = Tuple2.of(null, null);
							return Uni.createFrom().item(r);
						}
						Row first = rows.iterator().next();
						Map<String, Object> subscription = first.getJsonObject(0).getMap();
						String contextId = first.getString(1);
						return clientManager.getClient(AppConstants.INTERNAL_NULL_KEY, false).onItem()
								.transformToUni(defaultClient -> {
									return defaultClient.preparedQuery("SELECT body FROM contexts WHERE id=$1")
											.execute(Tuple.of(contextId)).onItem().transformToUni(rows1 -> {
												if (rows1.size() == 0) {
													return defaultClient
															.preparedQuery("SELECT body FROM contexts WHERE id=$1")
															.execute(Tuple.of(AppConstants.INTERNAL_NULL_KEY)).onItem()
															.transform(rows2 -> {
																Row defaultContextRow = rows2.iterator().next();
																return Tuple2.of(subscription,
																		defaultContextRow.getJsonObject(0).getMap());
															});
												} else {
													Row contextRow = rows1.iterator().next();
													return Uni.createFrom().item(Tuple2.of(subscription,
															contextRow.getJsonObject(0).getMap()));
												}

											});

								});

					});
		});
	}

}
