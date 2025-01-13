package eu.neclab.ngsildbroker.subscriptionmanager.repository;

import com.github.jsonldjava.core.JsonLDService;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.EntityInfo;
import eu.neclab.ngsildbroker.commons.datatypes.RegistrationEntry;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.DeleteSubscriptionRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.UpdateSubscriptionRequest;
import eu.neclab.ngsildbroker.commons.datatypes.terms.ScopeQueryTerm;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.storage.ClientManager;
import eu.neclab.ngsildbroker.commons.tools.DBUtil;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.mutiny.tuples.Tuple3;
import io.smallrye.mutiny.tuples.Tuple4;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.Tuple;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class SubscriptionInfoDAO {

	Logger logger = LoggerFactory.getLogger(SubscriptionInfoDAO.class);

	@Inject
	ClientManager clientManager;
	
	@Inject
	JsonLDService ldService;

	public Uni<Void> createSubscription(SubscriptionRequest request, String contextId) {
		return clientManager.getClient(request.getTenant(), true).onItem().transformToUni(client -> {
			return client
					.preparedQuery(
							"INSERT INTO subscriptions(subscription_id, subscription, context) VALUES ($1, $2, $3)")
					.execute(Tuple.of(request.getId(), new JsonObject(request.getPayload()), contextId)).onItem()
					.transformToUni(rows -> Uni.createFrom().voidItem());
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

	public Uni<Tuple2<Map<String, Object>, Object>> updateSubscription(UpdateSubscriptionRequest request,
			String contextId) {
		return clientManager.getClient(request.getTenant(), false).onItem().transformToUni(client -> {
			return client.preparedQuery(
					"UPDATE subscriptions SET subscription=subscription || $2, context=$3 WHERE subscription_id=$1 RETURNING subscriptions.subscription")
					.execute(Tuple.of(request.getId(), new JsonObject(request.getPayload()), contextId)).onItem()
					.transformToUni(rows -> {
						if (rows.size() == 0) {
							return Uni.createFrom()
									.failure(new ResponseException(ErrorType.NotFound, request.getId() + " not found"));
						} else {
							return Uni.createFrom()
									.item(Tuple2.of(rows.iterator().next().getJsonObject("subscription").getMap(),
											request.getContext().serialize().get("@context")));
						}
					});
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
		return clientManager.getClient(tenant, false).onItem().transformToUni(client -> client.preparedQuery(
				"SELECT subscription, contexts.body as contextBody, contexts.id FROM subscriptions LEFT JOIN contexts ON subscriptions.context = contexts.id WHERE subscription_id=$1")
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

	public Uni<List<Tuple4<String, Map<String, Object>, String, Map<String, Object>>>> loadSubscriptions() {
		return clientManager.getClient(AppConstants.INTERNAL_NULL_KEY, false).onItem().transformToUni(client -> {
			return client.preparedQuery("select tenant_id from tenant").execute().onItem().transformToUni(rows -> {
				List<Uni<RowSet<Row>>> unis = Lists.newArrayList();
				rows.forEach(row -> {
					unis.add(clientManager.getClient(row.getString(0), false).onItem()
							.transformToUni(tenantClient -> tenantClient.preparedQuery("SELECT '" + row.getString(0)
									+ "', subscriptions.subscription, context as contextId, contexts.body as contextBody FROM subscriptions LEFT JOIN contexts ON subscriptions.context = contexts.id")
									.execute()));
				});
				unis.add(client.preparedQuery("SELECT '" + AppConstants.INTERNAL_NULL_KEY
						+ "', subscriptions.subscription, context as contextId, contexts.body as contextBody FROM subscriptions LEFT JOIN contexts ON subscriptions.context = contexts.id")
						.execute());

				return Uni.combine().all().unis(unis).with(list -> {
					List<Tuple4<String, Map<String, Object>, String, Map<String, Object>>> result = new ArrayList<>();
					for (Object obj : list) {
						@SuppressWarnings("unchecked")
						RowSet<Row> rowset = (RowSet<Row>) obj;
						rowset.forEach(row -> {
							String tenant = row.getString(0);
							Map<String, Object> sub = row.getJsonObject(1).getMap();
							String ctxId = row.getString(2);
							JsonObject ctx = row.getJsonObject(3);
							Map<String, Object> ctxMap;
							if (ctx == null) {
								logger.error("Failed to read context for subscription "
										+ sub.get(NGSIConstants.JSON_LD_ID) + " on tenant " + tenant);
								ctxMap = null;
							} else {
								ctxMap = ctx.getMap();
							}
							result.add(Tuple4.of(tenant, sub, ctxId, ctxMap));
						});
					}
					return result;
				});
			});
		});

	}

	public Uni<Tuple3<Map<String, Object>, String, Map<String, Object>>> loadSubscription(String tenant, String id) {
		return clientManager.getClient(tenant, false).onItem().transformToUni(client -> {
			return client.preparedQuery(
					"SELECT subscription, context as contextId, contexts.body as contextBody FROM subscriptions LEFT JOIN contexts ON subscriptions.context = contexts.id WHERE subscription_id=$1")
					.execute(Tuple.of(id)).onItem().transform(rows -> {
						if (rows.size() == 0) {
							Tuple3<Map<String, Object>, String, Map<String, Object>> r = Tuple3.of(null, null, null);
							return r;
						}
						Row first = rows.iterator().next();
						Map<String, Object> subscription = first.getJsonObject(0).getMap();
						String contextId = first.getString(1);
						JsonObject ctx = first.getJsonObject(2);
						Map<String, Object> ctxMap;
						if (ctx == null) {
							logger.error("Failed to read context for subscription " + id + " on tenant " + tenant);
							ctxMap = null;
						} else {
							ctxMap = ctx.getMap();
						}
						return Tuple3.of(subscription, contextId, ctxMap);

					});
		});
	}

	public Uni<RowSet<Row>> getRegById(String tenant, String id) {
		return clientManager.getClient(tenant, false).onItem().transformToUni(client -> {
			return client.preparedQuery("SELECT reg FROM csource WHERE id = $1").execute(Tuple.of(id)).onFailure()
					.retry().atMost(3);
		});
	}
	
	public Uni<Table<String, String, List<RegistrationEntry>>> getAllRegistries() {
		return DBUtil.getAllRegistries(clientManager, ldService,
				"SELECT cs_id, c_id, e_id, e_id_p, e_type, e_prop, e_rel, ST_AsGeoJSON(i_location), scopes, EXTRACT(MILLISECONDS FROM expires), endpoint, tenant_id, headers, reg_mode, createEntity, updateEntity, appendAttrs, updateAttrs, deleteAttrs, deleteEntity, createBatch, upsertBatch, updateBatch, deleteBatch, upsertTemporal, appendAttrsTemporal, deleteAttrsTemporal, updateAttrsTemporal, deleteAttrInstanceTemporal, deleteTemporal, mergeEntity, replaceEntity, replaceAttrs, mergeBatch, retrieveEntity, queryEntity, queryBatch, retrieveTemporal, queryTemporal, retrieveEntityTypes, retrieveEntityTypeDetails, retrieveEntityTypeInfo, retrieveAttrTypes, retrieveAttrTypeDetails, retrieveAttrTypeInfo, createSubscription, updateSubscription, retrieveSubscription, querySubscription, deleteSubscription, queryEntityMap, createEntityMap, updateEntityMap, deleteEntityMap, retrieveEntityMap FROM csourceinformation WHERE queryentity OR querybatch OR retrieveentity OR createSubscription",
				logger);

	}

}
