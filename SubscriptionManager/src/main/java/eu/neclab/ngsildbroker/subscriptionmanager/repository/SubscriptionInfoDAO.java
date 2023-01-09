package eu.neclab.ngsildbroker.subscriptionmanager.repository;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.Lists;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.EntityInfo;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.DeleteSubscriptionRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.UpdateSubscriptionRequest;
import eu.neclab.ngsildbroker.commons.datatypes.terms.ScopeQueryTerm;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.storage.ClientManager;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.mutiny.tuples.Tuple3;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.Tuple;

@Singleton
public class SubscriptionInfoDAO {

	@Inject
	ClientManager clientManager;

	public Uni<RowSet<Row>> createSubscription(SubscriptionRequest request) {
		return clientManager.getClient(request.getTenant(), false).onItem().transformToUni(client -> {
			return client.preparedQuery(
					"INSERT INTO subscriptions(subscription_id, subscription, context) VALUES ($1, $2, $3)")
					.execute(Tuple.of(request.getId(), new JsonObject(request.getPayload()),
							new JsonObject(request.getContext().serialize())));
		});
	}

	public Uni<RowSet<Row>> updateSubscription(UpdateSubscriptionRequest request) {
		return clientManager.getClient(request.getTenant(), false).onItem().transformToUni(client -> {
			return client.preparedQuery(
					"UPDATE subscriptions SET subscription=subscription || $2, context=$3 WHERE subscription_id=$1 RETURNING subscription, context")
					.execute(Tuple.of(request.getId(), new JsonObject(request.getPayload()),
							new JsonObject(request.getContext().serialize())));
		});
	}

	public Uni<RowSet<Row>> deleteSubscription(DeleteSubscriptionRequest request) {
		return clientManager.getClient(request.getTenant(), false).onItem().transformToUni(client -> {
			return client.preparedQuery("DELETE FROM subscriptions WHERE subscription_id=$1")
					.execute(Tuple.of(request.getId()));
		});
	}

	public Uni<RowSet<Row>> getAllSubscriptions(String tenant, int limit, int offset) {
		return clientManager.getClient(tenant, false).onItem().transformToUni(client -> {
			return client.preparedQuery("SELECT subscription, COUNT(*) FROM subscriptions LIMIT $1 OFFSET $2")
					.execute(Tuple.of(limit, offset)).onFailure().retry().atMost(3);
		});
	}

	public Uni<RowSet<Row>> getSubscription(String tenant, String subscriptionId) {
		return clientManager.getClient(tenant, false).onItem().transformToUni(client -> {
			return client.preparedQuery("SELECT subscription FROM subscriptions WHERE subscription_id=$1")
					.execute(Tuple.of(subscriptionId)).onFailure().retry().atMost(3);
		});
	}

	public static void main(String[] args) throws JsonGenerationException, IOException {
		JsonLdProcessor.init("https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.6.jsonld");

		Context context = JsonLdProcessor.getCoreContextClone();
		Context test = new Context().parse(context.serialize().get("@context"), false);

		System.out.println(JsonUtils.toPrettyString(context.serialize()));
		System.out.println(JsonUtils.toPrettyString(test.serialize()));
	}

	public Uni<RowSet<Row>> getRegById(String tenant, String id) {
		return clientManager.getClient(tenant, false).onItem().transformToUni(client -> {
			return client.preparedQuery("SELECT reg FROM csource WHERE id = $1").execute(Tuple.of(id)).onFailure()
					.retry().atMost(3);
		});
	}

	public Uni<Void> updateNotificationSuccess(String tenant, String id, String date) {
		return clientManager.getClient(tenant, false).onItem().transformToUni(client -> {
			return client
					.preparedQuery("UPDATE registry_subscription SET subscription = subscription || ('{\""
							+ NGSIConstants.NGSI_LD_TIMES_SENT + "\": [{\"" + NGSIConstants.JSON_LD_VALUE
							+ "\": '|| subscription@>>'{" + NGSIConstants.NGSI_LD_TIMES_SENT + ",0, "
							+ NGSIConstants.JSON_LD_VALUE + "}'::integer + 1 ||'}],\""
							+ NGSIConstants.NGSI_LD_LAST_SUCCESS + "\": '[{\"" + NGSIConstants.JSON_LD_TYPE + "\": \""
							+ NGSIConstants.NGSI_LD_DATE_TIME + "\", \"" + NGSIConstants.JSON_LD_VALUE + "\": \"$1\"}],"
							+ NGSIConstants.NGSI_LD_LAST_NOTIFICATION + "\": '[{\"" + NGSIConstants.JSON_LD_TYPE
							+ "\": \"" + NGSIConstants.NGSI_LD_DATE_TIME + "\", \"" + NGSIConstants.JSON_LD_VALUE
							+ "\": \"$1\"}])::jsonb WHERE subscription_id=$2")
					.execute(Tuple.of(date, id)).onFailure().retry().atMost(3).onItem()
					.transformToUni(t -> Uni.createFrom().voidItem());
		});
	}

	public Uni<Void> updateNotificationFailure(String tenant, String id, String date) {
		return clientManager.getClient(tenant, false).onItem().transformToUni(client -> {
			return client
					.preparedQuery("UPDATE registry_subscription SET subscription = subscription || ('{\""
							+ NGSIConstants.NGSI_LD_TIMES_FAILED + "\": [{\"" + NGSIConstants.JSON_LD_VALUE
							+ "\": '|| subscription@>>'{" + NGSIConstants.NGSI_LD_TIMES_FAILED + ",0, "
							+ NGSIConstants.JSON_LD_VALUE + "}'::integer + 1 ||'}],\""
							+ NGSIConstants.NGSI_LD_LAST_FAILURE + "\": '[{\"" + NGSIConstants.JSON_LD_TYPE + "\": \""
							+ NGSIConstants.NGSI_LD_DATE_TIME + "\", \"" + NGSIConstants.JSON_LD_VALUE + "\": \"$1\"}],"
							+ NGSIConstants.NGSI_LD_LAST_NOTIFICATION + "\": '[{\"" + NGSIConstants.JSON_LD_TYPE
							+ "\": \"" + NGSIConstants.NGSI_LD_DATE_TIME + "\", \"" + NGSIConstants.JSON_LD_VALUE
							+ "\": \"$1\"}])::jsonb WHERE subscription_id=$2")
					.execute(Tuple.of(date, id)).onFailure().retry().atMost(3).onItem()
					.transformToUni(t -> Uni.createFrom().voidItem());
		});
	}

	public Uni<List<Tuple3<String, Map<String, Object>, Map<String, Object>>>> loadSubscriptions() {
		return clientManager.getClient(AppConstants.INTERNAL_NULL_KEY, false).onItem().transformToUni(client -> {
			return client.preparedQuery("select tenant_id from tenant").execute().onItem().transformToUni(rows -> {
				List<Uni<RowSet<Row>>> unis = Lists.newArrayList();
				rows.forEach(row -> {
					unis.add(clientManager.getClient(row.getString(0), false).onItem().transformToUni(tenantClient -> {
						return tenantClient.preparedQuery(
								"SELECT " + row.getString(0) + ", subscription, context FROM subscriptions")
								.execute();
					}));
				});
				unis.add(clientManager.getClient(AppConstants.INTERNAL_NULL_KEY, false).onItem()
						.transformToUni(tenantClient -> {
							return tenantClient.preparedQuery("SELECT " + AppConstants.INTERNAL_NULL_KEY
									+ ", subscription, context FROM subscriptions").execute();
						}));

				return Uni.combine().all().unis(unis).combinedWith(list -> {
					List<Tuple3<String, Map<String, Object>, Map<String, Object>>> result = Lists.newArrayList();
					for (Object obj : list) {
						@SuppressWarnings("unchecked")
						RowSet<Row> rowset = (RowSet<Row>) obj;
						rowset.forEach(row -> {
							result.add(Tuple3.of(row.getString(0), row.getJsonObject(1).getMap(),
									row.getJsonObject(2).getMap()));
						});
					}
					return result;
				});
			});
		});

	}

	public Uni<RowSet<Row>> getInitialNotificationData(SubscriptionRequest subscriptionRequest) {
		return clientManager.getClient(subscriptionRequest.getTenant(), false).onItem().transformToUni(client -> {
			List<Object> tupleItems = Lists.newArrayList();
			String sql = "with a as (select cs_id from contextsourceinformation WHERE ";
			boolean sqlAdded = false;
			int dollar = 1;
			Subscription subscription = subscriptionRequest.getSubscription();
			Iterator<EntityInfo> it = subscription.getEntities().iterator();
			while (it.hasNext()) {
				EntityInfo entityInformation = it.next();
				sql += "(";
				if (entityInformation.getId() != null) {
					sql += "((e_id is null or e_id  = $" + dollar + ") and (e_id_p is null or e_id_p ~ $" + dollar
							+ "))";
					dollar++;
					tupleItems.add(entityInformation.getId().toString());
					if (entityInformation.getType() != null) {
						sql += " and ";
					}
				} else if (entityInformation.getIdPattern() != null) {
					sql += "((e_id is null or $" + dollar + " ~ e_id) and (e_id_p is null or e_id_p = $" + dollar
							+ "))";
					dollar++;
					tupleItems.add(entityInformation.getIdPattern());
					if (entityInformation.getType() != null) {
						sql += " and ";
					}
				}
				if (entityInformation.getType() != null) {
					sql += "(e_type is null or e_type in $" + dollar + ")";
					dollar++;
					tupleItems.add(entityInformation.getType());
				}
				sql += ")";
				if (it.hasNext()) {
					sql += " and ";
				}
				sqlAdded = true;
			}

			if (subscription.getAttributeNames() != null) {
				if (sqlAdded) {
					sql += " and ";
				}
				sql += "(e_prop is null or e_prop in $" + dollar + ") and (e_rel is null or e_rel in $" + dollar + ")";
				tupleItems.add(subscription.getAttributeNames());
				dollar++;
				sqlAdded = true;
			}

			if (subscription.getLdGeoQuery() != null) {
				if (sqlAdded) {
					sql += " and ";
				}
				try {
					Tuple2<StringBuilder, Integer> tmp = subscription.getLdGeoQuery().getGeoSQLQuery(tupleItems, dollar,
							"i_location");
					sql += tmp.getItem1().toString();
					dollar = tmp.getItem2();
					sqlAdded = true;
				} catch (ResponseException e) {
					return Uni.createFrom().failure(e);
				}
			}

			if (subscription.getScopeQuery() != null) {
				if (sqlAdded) {
					sql += " and ";
				}
				sql += "(scopes IS NULL OR ";
				ScopeQueryTerm current = subscription.getScopeQuery();
				while (current != null) {
					sql += " matchscope(scopes, " + current.getSQLScopeQuery() + ")";

					if (current.hasNext()) {
						if (current.isNextAnd()) {
							sql += " and ";
						} else {
							sql += " or ";
						}
					}
					current = current.getNext();
				}
				sql += ")";

			}

			sql += ") select csource.reg from a left join csource on a.cs_id = csource.id";
			if (subscription.getCsf() != null) {
				// if (sqlAdded) {
				// sql += " and ";
				// }
				// dollar++;
			}

			return client.preparedQuery(sql).execute(Tuple.from(tupleItems)).onFailure().retry().atMost(3);
		});
	}

}
