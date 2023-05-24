package eu.neclab.ngsildbroker.historyentitymanager.repository;

import java.io.IOException;
import java.text.ParseException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.github.jsonldjava.core.JsonLdConsts;
import org.locationtech.spatial4j.context.SpatialContextFactory;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.exception.InvalidShapeException;
import org.locationtech.spatial4j.io.GeoJSONReader;
import org.locationtech.spatial4j.shape.Shape;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.DBConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.RegistrationEntry;
import eu.neclab.ngsildbroker.commons.datatypes.RemoteHost;
import eu.neclab.ngsildbroker.commons.datatypes.requests.AppendHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BatchRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CreateHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteAttrHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteAttrInstanceHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteAttributeRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.UpdateAttrHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.storage.ClientManager;
import eu.neclab.ngsildbroker.commons.tools.DBUtil;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.MultiMap;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowIterator;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.Tuple;
import io.vertx.pgclient.PgException;

@Singleton
public class HistoryDAO {

	private static Logger logger = LoggerFactory.getLogger(HistoryDAO.class);

	@Inject
	ClientManager clientManager;

	public Uni<Boolean> createHistoryEntity(CreateHistoryEntityRequest request) {

		return clientManager.getClient(request.getTenant(), true).onItem().transformToUni(client -> {
			Map<String, Object> payload = request.getPayload();
			return client.getConnection().onItem().transformToUni(conn -> {
				Tuple tuple = Tuple.of(payload.remove(NGSIConstants.JSON_LD_ID),
						((List<String>) payload.remove(NGSIConstants.JSON_LD_TYPE)).toArray(new String[0]),
						DBUtil.getLocalDateTime(payload.remove(NGSIConstants.NGSI_LD_CREATED_AT)),
						DBUtil.getLocalDateTime(payload.remove(NGSIConstants.NGSI_LD_MODIFIED_AT)));
				String sql = "INSERT INTO " + DBConstants.DBTABLE_TEMPORALENTITY
						+ " (id, e_types, createdat, modifiedat";
				Object scope = payload.remove(NGSIConstants.NGSI_LD_SCOPE);
				if (scope == null) {
					sql += ") VALUES($1, $2, $3, $4) ";
				} else {
					sql += ", scopes) VALUES($1, $2, $3, $4, getScopes($5)) ";
				}

				sql += "ON CONFLICT(id) DO UPDATE SET e_types = ARRAY(SELECT DISTINCT UNNEST("
						+ DBConstants.DBTABLE_TEMPORALENTITY
						+ ".e_types || EXCLUDED.e_types)), modifiedat = EXCLUDED.modifiedat ";
				if (scope == null) {
					sql += ", scopes = null ";
				} else {
					sql += ", scopes = getScopes($5) ";
				}

				sql += "RETURNING (" + DBConstants.DBTABLE_TEMPORALENTITY + ".modifiedat = "
						+ DBConstants.DBTABLE_TEMPORALENTITY + ".createdat)";
				logger.debug(sql);
				return conn.preparedQuery(sql).execute(tuple).onItem().transformToUni(rows -> {
					List<Tuple> batch = Lists.newArrayList();
					for (Entry<String, Object> entry : payload.entrySet()) {
						@SuppressWarnings("unchecked")
						List<Map<String, Object>> entries = (List<Map<String, Object>>) entry.getValue();
						for (Map<String, Object> attribEntry : entries) {
							attribEntry.put(NGSIConstants.NGSI_LD_INSTANCE_ID, List.of(
									Map.of(NGSIConstants.JSON_LD_ID, "instanceid:" + UUID.randomUUID().toString())));
							batch.add(Tuple.of(request.getId(), entry.getKey(), new JsonObject(attribEntry)));
						}
					}
					logger.debug("INSERT INTO " + DBConstants.DBTABLE_TEMPORALENTITY_ATTRIBUTEINSTANCE
							+ " (temporalentity_id, attributeid, data) VALUES ($1, $2, $3::jsonb)");
					return conn
							.preparedQuery("INSERT INTO " + DBConstants.DBTABLE_TEMPORALENTITY_ATTRIBUTEINSTANCE
									+ " (temporalentity_id, attributeid, data) VALUES ($1, $2, $3::jsonb)")
							.executeBatch(batch).onItem().transformToUni(t -> conn.close().onItem().transform(v -> {
								return !rows.iterator().next().getBoolean(0);
							}));
				});
			});
		});
	}

	public Uni<Void> batchUpsertHistoryEntity(BatchRequest request) {
		return clientManager.getClient(request.getTenant(), true).onItem().transformToUni(client -> {
			return client.getConnection().onItem().transformToUni(conn -> {
				List<Tuple> batchNoType = Lists.newArrayList();
				List<Tuple> batchType = Lists.newArrayList();
				List<Tuple> batchAttribs = Lists.newArrayList();
				for (Map<String, Object> payload : request.getRequestPayload()) {
					String entityId = (String) payload.remove(NGSIConstants.JSON_LD_ID);
					if (payload.containsKey(NGSIConstants.JSON_LD_TYPE)) {
						Tuple tuple = Tuple.of(entityId,
								((List<String>) payload.remove(NGSIConstants.JSON_LD_TYPE)).toArray(new String[0]),
								DBUtil.getLocalDateTime(payload.remove(NGSIConstants.NGSI_LD_CREATED_AT)),
								DBUtil.getLocalDateTime(payload.remove(NGSIConstants.NGSI_LD_MODIFIED_AT)));
						if (payload.containsKey(NGSIConstants.NGSI_LD_SCOPE)) {
							tuple.addJsonArray(new JsonArray(
									(List<Map<String, String>>) payload.remove(NGSIConstants.NGSI_LD_SCOPE)));
						} else {
							tuple.addJsonArray(null);
						}
						batchType.add(tuple);

					} else {
						payload.remove(NGSIConstants.NGSI_LD_CREATED_AT);
						Tuple tuple = Tuple
								.of(DBUtil.getLocalDateTime(payload.remove(NGSIConstants.NGSI_LD_MODIFIED_AT)));
						if (payload.containsKey(NGSIConstants.NGSI_LD_SCOPE)) {
							tuple.addJsonArray(new JsonArray(
									(List<Map<String, String>>) payload.remove(NGSIConstants.NGSI_LD_SCOPE)));
						} else {
							tuple.addJsonArray(null);
						}
						tuple.addString(entityId);
						batchNoType.add(tuple);
					}
					for (Entry<String, Object> entry : payload.entrySet()) {
						@SuppressWarnings("unchecked")
						List<Map<String, Object>> entries = (List<Map<String, Object>>) entry.getValue();
						for (Map<String, Object> attribEntry : entries) {
							attribEntry.put(NGSIConstants.NGSI_LD_INSTANCE_ID, List.of(
									Map.of(NGSIConstants.JSON_LD_ID, "instanceid:" + UUID.randomUUID().toString())));
							batchAttribs.add(Tuple.of(entityId, entry.getKey(), new JsonObject(attribEntry)));
						}
					}
				}
				String typeSql = "INSERT INTO " + DBConstants.DBTABLE_TEMPORALENTITY
						+ " (id, e_types, createdat, modifiedat, scopes) VALUES($1, $2, $3, $4, getScopes($5)) "
						+ "ON CONFLICT(id) DO UPDATE SET e_types = ARRAY(SELECT DISTINCT UNNEST("
						+ DBConstants.DBTABLE_TEMPORALENTITY
						+ ".e_types || EXCLUDED.e_types)), modifiedat = EXCLUDED.modifiedat, ";
				if (request.getRequestType() == AppConstants.APPEND_REQUEST) {
					typeSql += "scopes = CASE WHEN EXCLUDED.scopes IS NULL THEN temporalentity.scopes ELSE EXCLUDED.scopes END ";
				} else {
					typeSql += "scopes = EXCLUDED.scopes ";
				}

				typeSql += "RETURNING (modifiedat = createdat)";
				List<Uni<RowSet<Row>>> tmpList = new ArrayList<>(2);
				if (!batchType.isEmpty()) {
					tmpList.add(conn.preparedQuery(typeSql).executeBatch(batchType));
				}
				if (!batchNoType.isEmpty()) {
					tmpList.add(conn.preparedQuery("UPDATE " + DBConstants.DBTABLE_TEMPORALENTITY
							+ " SET modifiedat = $1, scopes = CASE WHEN $2 IS NULL THEN scopes ELSE getScopes($2) END WHERE id=$3")
							.executeBatch(batchNoType));
				}
				return Uni.combine().all().unis(tmpList).combinedWith(l -> {
					return conn
							.preparedQuery("INSERT INTO " + DBConstants.DBTABLE_TEMPORALENTITY_ATTRIBUTEINSTANCE
									+ " (temporalentity_id, attributeid, data) VALUES ($1, $2, $3::jsonb)")
							.executeBatch(batchAttribs);

				}).onItem().transformToUni(t -> {
					return t.onItem().transformToUni(x -> conn.close());
				});
			});
		});
	}

	public Uni<Void> setDeletedBatchHistoryEntity(BatchRequest request) {
		return clientManager.getClient(request.getTenant(), false).onItem().transformToUni(client -> {
			List<Tuple> batch = Lists.newArrayList();
			request.getEntityIds().forEach(entityId -> {
				batch.add(Tuple.of(
						LocalDateTime.ofInstant(Instant.ofEpochMilli(request.getSendTimestamp()), ZoneId.of("Z")),
						entityId));
			});
			return client.preparedQuery("UPDATE temporalentity SET deletedat=$1 WHERE id=$2").executeBatch(batch)
					.onItem().transformToUni(t -> Uni.createFrom().voidItem());
		});
	}

	public Uni<Void> deleteHistoryEntity(DeleteHistoryEntityRequest request) {
		return clientManager.getClient(request.getTenant(), true).onItem().transformToUni(client -> {
			String sql = "DELETE FROM " + DBConstants.DBTABLE_TEMPORALENTITY + " WHERE id = $1";
			return client.preparedQuery(sql).execute(Tuple.of(request.getId())).onFailure().recoverWithUni(e -> {
				if (e instanceof PgException) {
					if (((PgException) e).getCode().equals(AppConstants.SQL_NOT_FOUND)) {
						return Uni.createFrom().failure(
								new ResponseException(ErrorType.NotFound, request.getId() + " does not exist"));
					}
				}
				return Uni.createFrom().failure(e);
			}).onItem().transformToUni(rows -> Uni.createFrom().voidItem());
		});
	}

	public Uni<Void> appendToHistoryEntity(AppendHistoryEntityRequest request) {
		return clientManager.getClient(request.getTenant(), true).onItem().transformToUni(client -> {
			Map<String, Object> payload = request.getPayload();
			return client.getConnection().onItem().transformToUni(conn -> {
				Uni<RowSet<Row>> query1;
				Tuple tuple = Tuple.tuple();
				tuple.addLocalDateTime(DBUtil.getLocalDateTime(payload.remove(NGSIConstants.NGSI_LD_MODIFIED_AT)));

				String sql = "UPDATE " + DBConstants.DBTABLE_TEMPORALENTITY + " SET modifiedat = $1";
				int dollarCount = 2;
				if (payload.containsKey(NGSIConstants.JSON_LD_TYPE)
						&& payload.get(NGSIConstants.JSON_LD_TYPE) != null) {
					sql += ",e_types = ARRAY(SELECT DISTINCT UNNEST(e_types || $" + dollarCount + "))";
					tuple.addArrayOfString(
							((List<String>) payload.remove(NGSIConstants.JSON_LD_TYPE)).toArray(new String[0]));
					dollarCount++;
				}
				if (payload.containsKey(NGSIConstants.NGSI_LD_SCOPE)) {
					sql += ", SET scopes = getScopes($" + dollarCount + ")";
					dollarCount++;
					tuple.addJsonArray(
							new JsonArray((List<Map<String, String>>) payload.remove(NGSIConstants.NGSI_LD_SCOPE)));
				}
				sql += " WHERE id=$" + dollarCount;
				tuple.addString(request.getId());
				payload.remove(NGSIConstants.JSON_LD_TYPE);
				payload.remove(NGSIConstants.JSON_LD_ID);
				return conn.preparedQuery(sql).execute(tuple).onFailure().recoverWithUni(e -> {
					if (e instanceof PgException) {
						if (((PgException) e).getCode().equals(AppConstants.SQL_NOT_FOUND)) {
							return Uni.createFrom().failure(
									new ResponseException(ErrorType.NotFound, request.getId() + " does not exist"));
						}
					}
					return Uni.createFrom().failure(e);
				}).onItem().transformToUni(rows -> {
					List<Tuple> batch = Lists.newArrayList();
					for (Entry<String, Object> entry : payload.entrySet()) {
						@SuppressWarnings("unchecked")
						List<Map<String, Object>> entries = (List<Map<String, Object>>) entry.getValue();
						for (Map<String, Object> attribEntry : entries) {
							attribEntry.put(NGSIConstants.NGSI_LD_INSTANCE_ID, List.of(
									Map.of(NGSIConstants.JSON_LD_ID, "instanceid:" + UUID.randomUUID().toString())));
							batch.add(Tuple.of(request.getId(), entry.getKey(), new JsonObject(attribEntry)));
						}
					}
					return conn
							.preparedQuery("INSERT INTO " + DBConstants.DBTABLE_TEMPORALENTITY_ATTRIBUTEINSTANCE
									+ " (temporalentity_id, attributeid, data) VALUES ($1, $2, $3::jsonb)")
							.executeBatch(batch).onItem().transformToUni(t -> conn.close());
				});

			});
		});
	}

	public Uni<Void> updateAttrInstanceInHistoryEntity(UpdateAttrHistoryEntityRequest request) {
		return clientManager.getClient(request.getTenant(), true).onItem().transformToUni(client -> {
			Object payload = new JsonObject(
					((List<Map<String, Object>>) request.getPayload().get(request.getAttrId())).get(0));
			return client.getConnection().onItem().transformToUni(conn -> {
				return conn.preparedQuery("UPDATE " + DBConstants.DBTABLE_TEMPORALENTITY_ATTRIBUTEINSTANCE
						+ " SET data = data || $1::jsonb WHERE attributeid=$2 AND instanceid=$3 AND temporalentity_id=$4")
						.execute(Tuple.of(payload, request.getAttrId(), request.getInstanceId(), request.getId()))
						.onFailure().recoverWithUni(e -> {
							if (e instanceof PgException) {
								if (((PgException) e).getCode().equals(AppConstants.SQL_NOT_FOUND)) {
									return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound,
											request.getId() + " does not exist"));
								}
							}
							return Uni.createFrom().failure(e);
						}).onItem().transformToUni(rows -> {
							return conn
									.preparedQuery("UPDATE " + DBConstants.DBTABLE_TEMPORALENTITY
											+ " SET modifiedat = $1 WHERE id = $2")
									.execute(Tuple.of(
											DBUtil.getLocalDateTime(
													request.getPayload().get(NGSIConstants.NGSI_LD_MODIFIED_AT)),
											request.getId()))
									.onItem().transformToUni(t -> conn.close());
						});

			});
		});
	}

	public Uni<Void> deleteAttrFromHistoryEntity(DeleteAttrHistoryEntityRequest request) {
		return clientManager.getClient(request.getTenant(), true).onItem().transformToUni(client -> {
			return client.getConnection().onItem().transformToUni(conn -> {
				Uni<RowSet<Row>> query1;
				if (request.getDatasetId() == null) {
					if (request.isDeleteAll()) {
						query1 = conn
								.preparedQuery("DELETE FROM " + DBConstants.DBTABLE_TEMPORALENTITY_ATTRIBUTEINSTANCE
										+ " WHERE attributeid=$1 AND temporalentity_id=$2")
								.execute(Tuple.of(request.getAttribName(), request.getId()));
					} else {
						query1 = conn
								.preparedQuery("DELETE FROM " + DBConstants.DBTABLE_TEMPORALENTITY_ATTRIBUTEINSTANCE
										+ " WHERE attributeid=$1 AND temporalentity_id=$2 AND NOT data ?| array['"
										+ NGSIConstants.NGSI_LD_DATA_SET_ID + "']")
								.execute(Tuple.of(request.getAttribName(), request.getId()));
					}
				} else {
					query1 = conn
							.preparedQuery("DELETE FROM " + DBConstants.DBTABLE_TEMPORALENTITY_ATTRIBUTEINSTANCE
									+ " WHERE attributeid=$1 AND temporalentity_id=$2 AND '"
									+ NGSIConstants.NGSI_LD_DATA_SET_ID + "' ? data AND data@>>'{"
									+ NGSIConstants.NGSI_LD_DATA_SET_ID + ",0," + NGSIConstants.JSON_LD_ID + "}'=$3")
							.execute(Tuple.of(request.getAttribName(), request.getId(), request.getDatasetId()));
				}

				return query1.onFailure().recoverWithUni(e -> {
					if (e instanceof PgException) {
						if (((PgException) e).getCode().equals(AppConstants.SQL_NOT_FOUND)) {
							return Uni.createFrom().failure(
									new ResponseException(ErrorType.NotFound, request.getId() + " does not exist"));
						}
					}
					return Uni.createFrom().failure(e);
				}).onItem().transformToUni(rows -> {
					return conn
							.preparedQuery("UPDATE " + DBConstants.DBTABLE_TEMPORALENTITY
									+ " SET modifiedat = $1 WHERE id = $2")
							.execute(Tuple.of(LocalDateTime.ofInstant(Instant.ofEpochMilli(request.getSendTimestamp()),
									ZoneId.of("Z")), request.getId()))
							.onItem().transformToUni(t -> conn.close());
				});
			});
		});
	}

	public Uni<Void> deleteAttrInstanceInHistoryEntity(DeleteAttrInstanceHistoryEntityRequest request) {
		return clientManager.getClient(request.getTenant(), true).onItem().transformToUni(client -> {
			return client.getConnection().onItem().transformToUni(conn -> {
				return conn
						.preparedQuery("DELETE FROM " + DBConstants.DBTABLE_TEMPORALENTITY_ATTRIBUTEINSTANCE
								+ " WHERE attributeid=$1 AND instanceid=$2 AND temporalentity_id=$3")
						.execute(Tuple.of(request.getAttrId(), request.getInstanceId(), request.getId())).onFailure()
						.recoverWithUni(e -> {
							if (e instanceof PgException) {
								if (((PgException) e).getCode().equals(AppConstants.SQL_NOT_FOUND)) {
									return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound,
											request.getId() + " does not exist"));
								}
							}
							return Uni.createFrom().failure(e);
						}).onItem().transformToUni(rows -> {
							return conn
									.preparedQuery("UPDATE " + DBConstants.DBTABLE_TEMPORALENTITY
											+ " SET modifiedat = $1 WHERE id = $2")
									.execute(Tuple.of(
											LocalDateTime.ofInstant(Instant.ofEpochMilli(request.getSendTimestamp()),
													ZoneId.of("Z")),
											request.getId()))
									.onItem().transformToUni(t -> Uni.createFrom().voidItem());
						});
			});
		});
	}

	public Uni<Void> setEntityDeleted(BaseRequest request) {
		return clientManager.getClient(request.getTenant(), false).onItem().transformToUni(client -> {
			return client.preparedQuery("UPDATE temporalentity SET deletedat=$1 WHERE id=$2")
					// You would think that we do the conversion in the db but somehow postgres
					// can't easily convert utc into a timestamp without a timezone
					.execute(Tuple.of(
							LocalDateTime.ofInstant(Instant.ofEpochMilli(request.getSendTimestamp()), ZoneId.of("Z")),
							request.getId()))
					.onItem().transformToUni(t -> Uni.createFrom().voidItem());
		});

	}

	public Uni<Void> setAttributeDeleted(DeleteAttributeRequest request) {
		return clientManager.getClient(request.getTenant(), true).onItem().transformToUni(client -> {
			return client.getConnection().onItem().transformToUni(conn -> {
				LocalDateTime now = LocalDateTime.ofInstant(Instant.ofEpochMilli(request.getSendTimestamp()),
						ZoneId.of("Z"));
				String nowString = SerializationTools.notifiedAt_formatter.format(now);
				String instanceId = UUID.randomUUID().toString();
				JsonObject deletePayload = getAttribDeletedPayload(nowString, instanceId, request.getDatasetId());
				return conn.preparedQuery("INSERT INTO " + DBConstants.DBTABLE_TEMPORALENTITY_ATTRIBUTEINSTANCE
						+ " (temporalentity_id, attributeid, data, deletedat, instanceId) VALUES ($1, $2, $3::jsonb, $4, $5")
						.execute(Tuple.of(request.getId(), request.getAttribName(), deletePayload, now, instanceId))
						.onFailure().recoverWithUni(e -> {
							if (e instanceof PgException) {
								if (((PgException) e).getCode().equals(AppConstants.SQL_NOT_FOUND)) {
									return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound,
											request.getId() + " does not exist"));
								}
							}
							return Uni.createFrom().failure(e);
						}).onItem().transformToUni(rows -> {
							return conn
									.preparedQuery("UPDATE " + DBConstants.DBTABLE_TEMPORALENTITY
											+ " SET modifiedat = $1 WHERE id = $2")
									.execute(Tuple.of(now, request.getId())).onItem()
									.transformToUni(t -> Uni.createFrom().voidItem());
						});
			});
		});
	}

	private JsonObject getAttribDeletedPayload(String now, String instanceId, String datasetId) {
		Map<String, Object> result = Maps.newHashMap();
		result.put(NGSIConstants.NGSI_LD_DELETED_AT, Lists.newArrayList(
				Map.of(NGSIConstants.JSON_LD_TYPE, NGSIConstants.NGSI_LD_DATE_TIME, NGSIConstants.JSON_LD_VALUE, now)));
		result.put(NGSIConstants.NGSI_LD_INSTANCE_ID, Lists.newArrayList(Map.of(NGSIConstants.JSON_LD_ID, instanceId)));
		if (datasetId != null) {
			result.put(NGSIConstants.NGSI_LD_DATA_SET_ID,
					Lists.newArrayList(Map.of(NGSIConstants.JSON_LD_ID, instanceId)));
		}
		return new JsonObject(result);
	}

	public Uni<Table<String, String, List<RegistrationEntry>>> getAllRegistries() {
		return clientManager.getClient(AppConstants.INTERNAL_NULL_KEY, false).onItem().transformToUni(client -> {
			return client.preparedQuery("SELECT tenant_id FROM tenant").execute().onItem()
					.transformToUni(tenantRows -> {
						List<Uni<Tuple2<String, RowSet<Row>>>> unis = Lists.newArrayList();
						RowIterator<Row> it = tenantRows.iterator();
						String sql = "SELECT cs_id, c_id, e_id, e_id_p, e_type, e_prop, e_rel, ST_AsGeoJSON(i_location), scopes, EXTRACT(MILLISECONDS FROM expires), endpoint, tenant_id, headers, reg_mode, createEntity, updateEntity, appendAttrs, updateAttrs, deleteAttrs, deleteEntity, createBatch, upsertBatch, updateBatch, deleteBatch, upsertTemporal, appendAttrsTemporal, deleteAttrsTemporal, updateAttrsTemporal, deleteAttrInstanceTemporal, deleteTemporal, mergeEntity, replaceEntity, replaceAttrs, mergeBatch, retrieveEntity, queryEntity, queryBatch, retrieveTemporal, queryTemporal, retrieveEntityTypes, retrieveEntityTypeDetails, retrieveEntityTypeInfo, retrieveAttrTypes, retrieveAttrTypeDetails, retrieveAttrTypeInfo, createSubscription, updateSubscription, retrieveSubscription, querySubscription, deleteSubscription FROM csourceinformation WHERE upsertTemporal OR appendAttrsTemporal OR deleteAttrsTemporal OR updateAttrsTemporal OR deleteAttrInstanceTemporal OR deleteTemporal";
						unis.add(client.preparedQuery(sql).execute().onItem()
								.transform(rows -> Tuple2.of(AppConstants.INTERNAL_NULL_KEY, rows)));
						while (it.hasNext()) {
							unis.add(clientManager.getClient(it.next().getString(0), false).onItem()
									.transformToUni(tenantClient -> {
										return tenantClient.preparedQuery(sql).execute().onItem().transform(
												tenantReg -> Tuple2.of(AppConstants.INTERNAL_NULL_KEY, tenantReg));
									}));
						}
						return Uni.combine().all().unis(unis).combinedWith(list -> {
							Table<String, String, List<RegistrationEntry>> result = HashBasedTable.create();
							for (Object obj : list) {
								Tuple2<String, RowSet<Row>> tuple = (Tuple2<String, RowSet<Row>>) obj;
								String tenant = tuple.getItem1();
								RowIterator<Row> it2 = tuple.getItem2().iterator();
								while (it2.hasNext()) {
									Row row = it2.next();
									List<RegistrationEntry> entries = result.get(tenant, row.getString(1));
									if (entries == null) {
										entries = Lists.newArrayList();
										result.put(tenant, row.getString(1), entries);
									}
									entries.add(DBUtil.getRegistrationEntry(row, tenant, logger));
								}
							}
							return result;
						});
					});
		});

	}

}