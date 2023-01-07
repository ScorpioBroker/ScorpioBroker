package eu.neclab.ngsildbroker.registry.subscriptionmanager.repository;

import java.io.IOException;
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
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.DeleteSubscriptionRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.UpdateSubscriptionRequest;
import eu.neclab.ngsildbroker.commons.datatypes.terms.ScopeQueryTerm;
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

@Singleton
public class RegistrySubscriptionInfoDAO {

	@Inject
	ClientManager clientManager;

	public Uni<RowSet<Row>> createSubscription(SubscriptionRequest request) {
		return clientManager.getClient(request.getTenant(), false).onItem().transformToUni(client -> {
			return client.preparedQuery(
					"INSERT INTO registry_subscriptions(subscription_id, subscription, context) VALUES ($1, $2, $3)")
					.execute(Tuple.of(request.getId(), new JsonObject(request.getPayload()),
							new JsonObject(request.getContext().serialize())));
		});
	}

	public Uni<RowSet<Row>> updateSubscription(UpdateSubscriptionRequest request) {
		return clientManager.getClient(request.getTenant(), false).onItem().transformToUni(client -> {
			return client.preparedQuery(
					"UPDATE registry_subscriptions SET subscription=subscription || $2, context=$3 WHERE subscription_id=$1 RETURNING subscription, context")
					.execute(Tuple.of(request.getId(), new JsonObject(request.getPayload()),
							new JsonObject(request.getContext().serialize())));
		});
	}

	public Uni<RowSet<Row>> deleteSubscription(DeleteSubscriptionRequest request) {
		return clientManager.getClient(request.getTenant(), false).onItem().transformToUni(client -> {
			return client.preparedQuery("DELETE FROM registry_subscriptions WHERE subscription_id=$1")
					.execute(Tuple.of(request.getId()));
		});
	}

	public Uni<RowSet<Row>> getAllSubscriptions(String tenant, int limit, int offset) {
		return clientManager.getClient(tenant, false).onItem().transformToUni(client -> {
			return client.preparedQuery("SELECT subscription, COUNT(*) FROM registry_subscriptions LIMIT $1 OFFSET $2")
					.execute(Tuple.of(limit, offset)).onFailure().retry().atMost(3);
		});
	}

	public Uni<RowSet<Row>> getSubscription(String tenant, String subscriptionId) {
		return clientManager.getClient(tenant, false).onItem().transformToUni(client -> {
			return client.preparedQuery("SELECT subscription FROM registry_subscriptions WHERE subscription_id=$1")
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
			return client.preparedQuery("UPDATE registry_subscription SET subscription = jsonb_set(subscription, '')")
					.execute(Tuple.of(id)).onFailure().retry().atMost(3).onItem()
					.transformToUni(t -> Uni.createFrom().voidItem());
		});
	}

	public Uni<Void> updateNotificationFailure(String tenant, String id, String format) {
		// TODO Auto-generated method stub
		return null;
	}

	public Uni<List<Tuple3<String, Map<String, Object>, Map<String, Object>>>> loadSubscriptions() {
		return clientManager.getClient(AppConstants.INTERNAL_NULL_KEY, false).onItem().transformToUni(client -> {
			return client.preparedQuery("select tenant_id from tenant").execute().onItem().transformToUni(rows -> {
				List<Uni<RowSet<Row>>> unis = Lists.newArrayList();
				rows.forEach(row -> {
					unis.add(clientManager.getClient(row.getString(0), false).onItem().transformToUni(tenantClient -> {
						return tenantClient.preparedQuery(
								"SELECT " + row.getString(0) + ", subscription, context FROM registry_subscriptions")
								.execute();
					}));
				});
				unis.add(clientManager.getClient(AppConstants.INTERNAL_NULL_KEY, false).onItem()
						.transformToUni(tenantClient -> {
							return tenantClient.preparedQuery("SELECT " + AppConstants.INTERNAL_NULL_KEY
									+ ", subscription, context FROM registry_subscriptions").execute();
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

	public Uni<RowSet<Row>> getInitialNotificationData(SubscriptionRequest message) {
		return clientManager.getClient(message.getTenant(), false).onItem().transformToUni(client -> {
			List<Object> tupleItems = Lists.newArrayList();
			String sql = "with a as (select cs_id from contextsourceinformation WHERE ";
			boolean sqlAdded = false;
			int dollar = 1;
			message.getSubscription().getEntities().get(0).getId();
			if (ids != null) {
				sql += "(e_id in $" + dollar + " or e_id is null) and (e_id_p is null or any($" + dollar
						+ ") like e_id_p)";
				dollar++;
				tupleItems.add(ids);
				sqlAdded = true;
			}
			if (idPattern != null) {
				if (sqlAdded) {
					sql += " and ";
				}
				sql += "(e_id is null or $" + dollar + " ~ e_id)";
				tupleItems.add(idPattern);
				dollar++;
				sqlAdded = true;
			}
			if (typeQuery != null) {
				if (sqlAdded) {
					sql += " and ";
				}
				tupleItems.add(typeQuery.getAllTypes());
				dollar++;
				sql += "(e_type is null or e_type in $" + dollar + ")";
			}
			if (attrsQuery != null) {
				if (sqlAdded) {
					sql += " and ";
				}
				sql += "(e_prop is null or e_prop in $" + dollar + ") and (e_rel is null or e_rel in $" + dollar + ")";
				tupleItems.add(attrsQuery.getAttrs());
				dollar++;
			}
			if (geoQuery != null) {
				if (sqlAdded) {
					sql += " and ";
				}
				try {
					Tuple2<StringBuilder, Integer> tmp = geoQuery.getGeoSQLQuery(tupleItems, dollar, "i_location");
					sql += tmp.getItem1().toString();
					dollar = tmp.getItem2();
				} catch (ResponseException e) {
					return Uni.createFrom().failure(e);
				}
			}
			if (scopeQuery != null) {
				if (sqlAdded) {
					sql += " and ";
				}
				sql += "(scopes IS NULL OR ";
				ScopeQueryTerm current = scopeQuery;
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
			if (csf != null) {
				// if (sqlAdded) {
				// sql += " and ";
				// }
				// dollar++;
			}

			return client.preparedQuery(sql).execute(Tuple.from(tupleItems)).onFailure().retry().atMost(3);
	}

}
