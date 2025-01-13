package eu.neclab.ngsildbroker.registry.subscriptionmanager.repository;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.annotation.PostConstruct;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import com.google.common.collect.Lists;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.EntityInfo;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
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
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.ext.web.client.WebClient;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.Tuple;

@Singleton
public class RegistrySubscriptionInfoDAO {

	private static Logger logger = LoggerFactory.getLogger(RegistrySubscriptionInfoDAO.class);

	@Inject
	ClientManager clientManager;
	@Inject
	Vertx vertx;
	WebClient webClient;

	@PostConstruct
	void setup() {
		webClient = WebClient.create(vertx);
	}

	public Uni<RowSet<Row>> createSubscription(SubscriptionRequest request) {
		return clientManager.getClient(request.getTenant(), true).onItem().transformToUni(
				client -> webClient.postAbs("http://localhost:9090/ngsi-ld/v1/jsonldContexts/createimplicitly/")
						.sendJsonObject(new JsonObject(request.getContext().serialize())).onItemOrFailure()
						.transformToUni((item, failure) -> {
							if (failure != null)
								return Uni.createFrom().failure(new Throwable("Something went wrong"));
							String contextId = item.bodyAsString();
							return client.preparedQuery(
									"INSERT INTO registry_subscriptions(subscription_id, subscription, context) VALUES ($1, $2, $3)")
									.execute(
											Tuple.of(request.getId(), new JsonObject(request.getPayload()), contextId));
						}));
	}

	public Uni<Tuple2<Map<String, Object>, Object>> updateSubscription(UpdateSubscriptionRequest request) {
		return clientManager.getClient(request.getTenant(), false).onItem().transformToUni(
				client -> webClient.postAbs("http://localhost:9090/ngsi-ld/v1/jsonldContexts/createimplicitly/")
						.sendJsonObject(new JsonObject(request.getContext().serialize())).onItemOrFailure()
						.transformToUni((item, failure) -> {
							if (failure != null)
								throw new RuntimeException();
							String contextId = item.bodyAsString();
							return client.preparedQuery(
									"UPDATE registry_subscriptions SET subscription=subscription || $2, context=$3 WHERE subscription_id=$1 RETURNING subscription")
									.execute(Tuple.of(request.getId(), new JsonObject(request.getPayload()), contextId))
									.onItem().transformToUni(rows -> {
										if (rows.size() == 0) {
											return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound,
													request.getId() + " not found"));
										} else {
											return Uni.createFrom()
													.item(Tuple2.of(
															rows.iterator().next().getJsonObject("subscription")
																	.getMap(),
															request.getContext().serialize().get("@context")));
										}
									});
						}));
	}

	public Uni<RowSet<Row>> deleteSubscription(DeleteSubscriptionRequest request) {
		return clientManager.getClient(request.getTenant(), false).onItem().transformToUni(
				client -> client.preparedQuery("DELETE FROM registry_subscriptions WHERE subscription_id=$1")
						.execute(Tuple.of(request.getId())).onItem().transformToUni(rows -> {
							if (rows.rowCount() == 0) {
								return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound));
							}
							return Uni.createFrom().item(rows);
						}));
	}

	public Uni<RowSet<Row>> getAllSubscriptions(String tenant, int limit, int offset) {
		return clientManager.getClient(tenant, false).onItem().transformToUni(client -> {
			return client.preparedQuery("SELECT subscription  FROM registry_subscriptions LIMIT $1 OFFSET $2")
					.execute(Tuple.of(limit, offset)).onFailure().retry().atMost(3);
		});
	}

	public Uni<RowSet<Row>> getSubscription(String tenant, String subscriptionId) {
		return clientManager.getClient(tenant, false).onItem().transformToUni(client -> {
			return client.preparedQuery(
					"SELECT subscription, contexts.body as contextBody, contexts.id FROM registry_subscriptions LEFT JOIN contexts ON registry_subscriptions.context = contexts.id WHERE subscription_id=$1")
					.execute(Tuple.of(subscriptionId)).onFailure().retry().atMost(3);
		});
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
					.preparedQuery("UPDATE registry_subscriptions SET subscription = subscription || ('{\""
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
		return clientManager.getClient(tenant, false).onItem().transformToUni(client -> {
			return client
					.preparedQuery("UPDATE registry_subscriptions SET subscription = subscription || ('{\""
							+ NGSIConstants.NGSI_LD_TIMES_FAILED + "\": [{\"" + NGSIConstants.JSON_LD_VALUE
							+ "\": '|| (subscription#>>'{" + NGSIConstants.NGSI_LD_TIMES_FAILED + ",0, "
							+ NGSIConstants.JSON_LD_VALUE + "}')::integer + 1 ||'}],\""
							+ NGSIConstants.NGSI_LD_LAST_FAILURE + "\": [{\"" + NGSIConstants.JSON_LD_TYPE + "\": \""
							+ NGSIConstants.NGSI_LD_DATE_TIME + "\", \"" + NGSIConstants.JSON_LD_VALUE
							+ "\": \"$1\"}],\"" + NGSIConstants.NGSI_LD_LAST_NOTIFICATION + "\": [{\""
							+ NGSIConstants.JSON_LD_TYPE + "\": \"" + NGSIConstants.NGSI_LD_DATE_TIME + "\", \""
							+ NGSIConstants.JSON_LD_VALUE + "\": \"$1\"}]}')::jsonb WHERE subscription_id=$2")
					.execute(Tuple.of(date, id)).onFailure().retry().atMost(3).onItem()
					.transformToUni(t -> Uni.createFrom().voidItem());
		});
	}

	@SuppressWarnings("unchecked")
	public Uni<List<Tuple3<String, Map<String, Object>, Map<String, Object>>>> loadSubscriptions() {
		return clientManager.getClient(AppConstants.INTERNAL_NULL_KEY, false).onItem().transformToUni(client -> {
			return client.preparedQuery("select tenant_id from tenant").execute().onItem().transformToUni(rows -> {
				List<Uni<RowSet<Row>>> unis = Lists.newArrayList();
				rows.forEach(row -> {
					unis.add(clientManager.getClient(row.getString(0), false).onItem().transformToUni(tenantClient -> {
						return tenantClient.preparedQuery(
								"SELECT '" + row.getString(0) + "', subscription, context FROM registry_subscriptions")
								.execute();
					}));
				});
				unis.add(clientManager.getClient(AppConstants.INTERNAL_NULL_KEY, false).onItem()
						.transformToUni(tenantClient -> {
							return tenantClient.preparedQuery("SELECT '" + AppConstants.INTERNAL_NULL_KEY
									+ "', subscription, context FROM registry_subscriptions").execute();
						}));

				return Uni.combine().all().unis(unis).with(list -> {
					List<Tuple3<String, Map<String, Object>, Map<String, Object>>> result = Lists.newArrayList();
//					for (Object obj : list) {
//						@SuppressWarnings("unchecked")
//						RowSet<Row> rowset = (RowSet<Row>) obj;
//						rowset.forEach(row -> {
//							result.add(Tuple3.of(row.getString(0), row.getJsonObject(1).getMap(),
//									row.getJsonObject(2).getMap()));
//						});
//					}
//					return result;
					return clientManager.getClient(AppConstants.INTERNAL_NULL_KEY, false).onItem()
							.transformToUni(pgPool -> {
								return pgPool
										.preparedQuery("select jsonb_object_agg(id,body) as col from public.contexts")
										.execute().onItem().transform(rows1 -> {
											JsonObject jsonContexts = rows1.iterator().next().getJsonObject(0);
											Map<String, Object> mapContexts;
											if (jsonContexts != null)
												mapContexts = jsonContexts.getMap();
											else
												return result;
											for (Object obj : list) {
												RowSet<Row> rowset = (RowSet<Row>) obj;
												rowset.forEach(row -> result.add(Tuple3.of(row.getString(0),
														row.getJsonObject(1).getMap(),
														(Map<String, Object>) mapContexts.get(row.getString(2)))));
											}
											return result;
										});
							});
				}).onItem().transformToUni(x -> x);
			});
		});

	}

	public Uni<RowSet<Row>> getInitialNotificationData(SubscriptionRequest subscriptionRequest) {
		return clientManager.getClient(subscriptionRequest.getTenant(), false).onItem().transformToUni(client -> {

			Tuple tuple = Tuple.tuple();
			StringBuilder sql = new StringBuilder("with a as (select cs_id from csourceinformation WHERE ");
			boolean sqlAdded = false;
			int dollar = 1;
			Subscription subscription = subscriptionRequest.getSubscription();
			Iterator<EntityInfo> it = subscription.getEntities().iterator();
			while (it.hasNext()) {
				EntityInfo entityInformation = it.next();
				sql.append("(");
				if (entityInformation.getId() != null) {
					sql.append("((e_id is null or e_id  = $" + dollar + ") and (e_id_p is null or e_id_p ~ $" + dollar
							+ "))");
					dollar++;

					tuple.addString(entityInformation.getId().toString());
					if (entityInformation.getTypeTerm() != null) {
						sql.append(" and ");
					}
				} else if (entityInformation.getIdPattern() != null) {
					sql.append("((e_id is null or $" + dollar + " ~ e_id) and (e_id_p is null or e_id_p = $" + dollar
							+ "))");
					dollar++;
					tuple.addString(entityInformation.getIdPattern());
					if (entityInformation.getTypeTerm() != null) {
						sql.append(" and ");
					}
				}
				if (entityInformation.getTypeTerm() != null) {
					// dollar = entityInformation.getTypeTerm().toSql(sql, tuple, dollar);
					Set<String> types = entityInformation.getTypeTerm().getAllTypes();
					sql.append("e_type IN (");
					for (String type : types) {
						sql.append('$');
						sql.append(dollar);
						sql.append(',');
						dollar++;
						tuple.addString(type);
					}
					sql.setCharAt(sql.length() - 1, ')');

				}
				sql.append(")");
				if (it.hasNext()) {
					sql.append(" and ");
				}
				sqlAdded = true;
			}

			if (subscription.getAttributeNames() != null) {
				if (sqlAdded) {
					sql.append(" and ");
				}
				sql.append("(e_prop is null or e_prop = any($" + dollar + ")) and (e_rel is null or e_rel = any($"
						+ dollar + "))");
				tuple.addArrayOfString(subscription.getAttributeNames().toArray(new String[0]));
				dollar++;
				sqlAdded = true;
			}

			if (subscription.getLdGeoQuery() != null) {
				if (sqlAdded) {
					sql.append(" and ");
				}
				try {
					Tuple2<StringBuilder, Integer> tmp = subscription.getLdGeoQuery().getGeoSQLQuery(tuple, dollar,
							"i_location");
					sql.append(tmp.getItem1().toString());
					dollar = tmp.getItem2();
					sqlAdded = true;
				} catch (ResponseException e) {
					return Uni.createFrom().failure(e);
				}
			}

			if (subscription.getScopeQuery() != null) {
				if (sqlAdded) {
					sql.append(" and ");
				}
				sql.append("(scopes IS NULL OR ");
				ScopeQueryTerm current = subscription.getScopeQuery();
				while (current != null) {
					sql.append(" matchscope(scopes, " + current.getSQLScopeQuery() + ")");

					if (current.hasNext()) {
						if (current.isNextAnd()) {
							sql.append(" and ");
						} else {
							sql.append(" or ");
						}
					}
					current = current.getNext();
				}
				sql.append(")");

			}

			sql.append(") select csource.reg from a left join csource on a.cs_id = csource.id");
			if (subscription.getCsf() != null) {
				// if (sqlAdded) {
				// sql += " and ";
				// }
				// dollar++;
			}

//			logger.debug("SQL I noti: " + sql);
//			logger.debug("Tuple I noti: " + tuple.deepToString());
			return client.preparedQuery(sql.toString()).execute(tuple).onFailure().retry().atMost(3);
		});
	}

	public Uni<Tuple2<Map<String, Object>, Map<String, Object>>> loadRegSubscription(String tenant, String id) {
		return clientManager.getClient(tenant, false).onItem().transformToUni(client -> {
			return client
					.preparedQuery("SELECT subscription, context FROM registry_subscriptions WHERE subscription_id=$1")
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

	public Uni<Tuple2<Map<String, Object>, Map<String, Object>>> loadSubscription(String tenant, String id) {
		return clientManager.getClient(tenant, false).onItem().transformToUni(client -> {
			return client.preparedQuery("SELECT subscription, context FROM subscriptions WHERE subscription_id=$1")
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
