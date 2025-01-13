package eu.neclab.ngsildbroker.historyentitymanager.repository;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.jsonldjava.core.JsonLDService;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.DBConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.RegistrationEntry;
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
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.Tuple;
import io.vertx.pgclient.PgException;

@Singleton
@SuppressWarnings("unchecked")
public class HistoryDAO {

	private static Logger logger = LoggerFactory.getLogger(HistoryDAO.class);

	@Inject
	ClientManager clientManager;

	@Inject
	JsonLDService ldService;

	public Uni<Boolean> createHistoryEntity(CreateHistoryEntityRequest request) {

		return clientManager.getClient(request.getTenant(), true).onItem().transformToUni(client -> {
			Map<String, Object> payload = request.getFirstPayload();
			// return client.getConnection().onItem().transformToUni(conn -> {
			Tuple tuple = Tuple.of(payload.remove(NGSIConstants.JSON_LD_ID),
					((List<String>) payload.remove(NGSIConstants.JSON_LD_TYPE)).toArray(new String[0]),
					((List<Map<String, String>>) payload.remove(NGSIConstants.NGSI_LD_CREATED_AT)).get(0)
							.get(NGSIConstants.JSON_LD_VALUE),
					((List<Map<String, String>>) payload.remove(NGSIConstants.NGSI_LD_MODIFIED_AT)).get(0)
							.get(NGSIConstants.JSON_LD_VALUE));
			StringBuilder sql = new StringBuilder("INSERT INTO ");
			sql.append(DBConstants.DBTABLE_TEMPORALENTITY);
			sql.append(" (id, e_types, createdat, modifiedat");
			Object scope = payload.remove(NGSIConstants.NGSI_LD_SCOPE);
			if (scope == null) {
				sql.append(") VALUES($1, $2, $3::text::timestamp, $4::text::timestamp) ");
			} else {
				sql.append(", scopes) VALUES($1, $2, $3::text::timestamp, $4::text::timestamp, getScopes($5::jsonb)) ");
			}

			sql.append("ON CONFLICT(id) DO UPDATE SET e_types = ARRAY(SELECT DISTINCT UNNEST(");
			sql.append(DBConstants.DBTABLE_TEMPORALENTITY);
			sql.append(".e_types || EXCLUDED.e_types)), modifiedat = EXCLUDED.modifiedat ");
			if (scope == null) {
				sql.append(", scopes = null ");
			} else {
				sql.append(", scopes = getScopes($5::jsonb) ");
			}

			sql.append("RETURNING (");
			sql.append(DBConstants.DBTABLE_TEMPORALENTITY);
			sql.append(".modifiedat = ");
			sql.append(DBConstants.DBTABLE_TEMPORALENTITY);
			sql.append(".createdat)");

			// logger.debug(tuple.deepToString());
			return client.preparedQuery(sql.toString()).execute(tuple).onItem().transformToUni(rows -> {
				List<Tuple> batch = Lists.newArrayList();
				Object location = payload.get(NGSIConstants.NGSI_LD_LOCATION);
				JsonObject geoLocation = null;
				String insertSql;
				if (location != null) {
					List<Map<String, List<Map<String, Object>>>> tmp = (List<Map<String, List<Map<String, Object>>>>) location;
					geoLocation = new JsonObject(tmp.get(0).get(NGSIConstants.NGSI_LD_HAS_VALUE).get(0));
					insertSql = "INSERT INTO " + DBConstants.DBTABLE_TEMPORALENTITY_ATTRIBUTEINSTANCE
							+ " (temporalentity_id, attributeid, data, location) VALUES ($1, $2, $3::jsonb, ST_SetSRID(ST_GeomFromGeoJSON(getGeoJson($4)), 4326))";
				} else {
					insertSql = "INSERT INTO " + DBConstants.DBTABLE_TEMPORALENTITY_ATTRIBUTEINSTANCE
							+ " (temporalentity_id, attributeid, data) VALUES ($1, $2, $3::jsonb)";
				}
				for (Entry<String, Object> entry : payload.entrySet()) {
					List<Map<String, Object>> entries = (List<Map<String, Object>>) entry.getValue();
					for (Map<String, Object> attribEntry : entries) {
						attribEntry.put(NGSIConstants.NGSI_LD_INSTANCE_ID, List
								.of(Map.of(NGSIConstants.JSON_LD_ID, "instanceid:" + UUID.randomUUID().toString())));
						if (location != null) {
							batch.add(Tuple.of(request.getFirstId(), entry.getKey(), new JsonObject(attribEntry),
									geoLocation));
						} else {
							batch.add(Tuple.of(request.getFirstId(), entry.getKey(), new JsonObject(attribEntry)));
						}
					}
				}
				return client.preparedQuery(insertSql).executeBatch(batch).onItem()
						.transform(rows1 -> !rows.iterator().next().getBoolean(0));

			});
		});
		// });
	}

	public Uni<Void> batchUpsertHistoryEntity(BatchRequest request) {
		return clientManager.getClient(request.getTenant(), true).onItem().transformToUni(client -> {
			// return client.getConnection().onItem().transformToUni(conn -> {
			List<Tuple> batchNoType = Lists.newArrayList();
			List<Tuple> batchType = Lists.newArrayList();
			List<Tuple> batchAttribs = Lists.newArrayList();
			List<Tuple> batchAttribsWtihLocation = Lists.newArrayList();
			for (Entry<String, List<Map<String, Object>>> payloadListEntry : request.getPayload().entrySet()) {
				String entityId = payloadListEntry.getKey();
				List<Map<String, Object>> payloadList = payloadListEntry.getValue();
				for (Map<String, Object> payload : payloadList) {
					payload.remove(NGSIConstants.JSON_LD_ID);
					Object location = payload.get(NGSIConstants.NGSI_LD_LOCATION);
					JsonObject geoLocation = null;

					if (location != null) {
						List<Map<String, List<Map<String, Object>>>> tmp = (List<Map<String, List<Map<String, Object>>>>) location;
						geoLocation = new JsonObject(tmp.get(0).get(NGSIConstants.NGSI_LD_HAS_VALUE).get(0));
					}
					if (payload.containsKey(NGSIConstants.JSON_LD_TYPE)) {
						Tuple tuple = Tuple.of(entityId);

						tuple.addArrayOfString(
								((List<String>) payload.remove(NGSIConstants.JSON_LD_TYPE)).toArray(new String[0]));
						if (payload.containsKey(NGSIConstants.NGSI_LD_CREATED_AT)) {
							tuple.addString(
									((List<Map<String, String>>) payload.remove(NGSIConstants.NGSI_LD_CREATED_AT))
											.get(0).get(NGSIConstants.JSON_LD_VALUE));
						} else {
							tuple.addString(null);
						}
						if (payload.containsKey(NGSIConstants.NGSI_LD_MODIFIED_AT)) {
							tuple.addString(
									((List<Map<String, String>>) payload.remove(NGSIConstants.NGSI_LD_MODIFIED_AT))
											.get(0).get(NGSIConstants.JSON_LD_VALUE));
						} else {
							tuple.addString(null);
						}
						if (payload.containsKey(NGSIConstants.NGSI_LD_SCOPE)) {
							tuple.addJsonArray(new JsonArray(
									(List<Map<String, String>>) payload.remove(NGSIConstants.NGSI_LD_SCOPE)));
						} else {
							tuple.addJsonArray(null);
						}
						// logger.debug("batch type" + tuple.deepToString());
						batchType.add(tuple);

					} else {
						payload.remove(NGSIConstants.NGSI_LD_CREATED_AT);
						Tuple tuple = Tuple
								.of(((List<Map<String, String>>) payload.remove(NGSIConstants.NGSI_LD_MODIFIED_AT))
										.get(0).get(NGSIConstants.JSON_LD_VALUE));
						if (payload.containsKey(NGSIConstants.NGSI_LD_SCOPE)) {
							tuple.addJsonArray(new JsonArray(
									(List<Map<String, String>>) payload.remove(NGSIConstants.NGSI_LD_SCOPE)));
						} else {
							tuple.addJsonArray(null);
						}
						tuple.addString(entityId);
						// logger.debug("batch no type" + tuple.deepToString());
						batchNoType.add(tuple);
					}
					List<Tuple> attribsToFill;
					if (location == null) {
						attribsToFill = batchAttribs;
					} else {
						attribsToFill = batchAttribsWtihLocation;
					}
					for (Entry<String, Object> entry : payload.entrySet()) {
						List<Map<String, Object>> entries = (List<Map<String, Object>>) entry.getValue();
						for (Map<String, Object> attribEntry : entries) {
							attribEntry.put(NGSIConstants.NGSI_LD_INSTANCE_ID, List.of(
									Map.of(NGSIConstants.JSON_LD_ID, "instanceid:" + UUID.randomUUID().toString())));
							Tuple tuple;
							if (location != null) {
								tuple = Tuple.of(entityId, entry.getKey(), new JsonObject(attribEntry), geoLocation);
								attribsToFill.add(tuple);
							} else {
								tuple = Tuple.of(entityId, entry.getKey(), new JsonObject(attribEntry));
								attribsToFill.add(tuple);
							}
							// logger.debug("attrib tuple " + tuple.deepToString());

						}
					}
				}
			}
			StringBuilder typeSql = new StringBuilder("INSERT INTO ");
			typeSql.append(DBConstants.DBTABLE_TEMPORALENTITY);
			typeSql.append(
					" (id, e_types, createdat, modifiedat, scopes) VALUES($1, $2, $3::text::timestamp, $4::text::timestamp, getScopes($5::jsonb)) ON CONFLICT(id) DO UPDATE SET e_types = ARRAY(SELECT DISTINCT UNNEST(");
			typeSql.append(DBConstants.DBTABLE_TEMPORALENTITY);
			typeSql.append(".e_types || EXCLUDED.e_types)), modifiedat = EXCLUDED.modifiedat, ");
			if (request.getRequestType() == AppConstants.APPEND_REQUEST) {
				typeSql.append(
						"scopes = CASE WHEN EXCLUDED.scopes IS NULL THEN temporalentity.scopes ELSE EXCLUDED.scopes END ");
			} else {
				typeSql.append("scopes = EXCLUDED.scopes ");
			}

			typeSql.append("RETURNING (modifiedat = createdat)");
			List<Uni<RowSet<Row>>> tmpList = new ArrayList<>(2);
			if (!batchType.isEmpty()) {

				tmpList.add(client.preparedQuery(typeSql.toString()).executeBatch(batchType));
			}
			if (!batchNoType.isEmpty()) {
				String noTypeSql = "UPDATE " + DBConstants.DBTABLE_TEMPORALENTITY
						+ " SET modifiedat = $1::text::timestamp, scopes = CASE WHEN $2 IS NULL THEN scopes ELSE getScopes($2::jsonb) END WHERE id=$3";
				logger.debug(noTypeSql);
				tmpList.add(client.preparedQuery(noTypeSql).executeBatch(batchNoType));
			}
			return Uni.combine().all().unis(tmpList).with(l -> l).onItem().transformToUni(l -> {
				List<Uni<RowSet<Row>>> attribList = Lists.newArrayList();
				String sql;
				if (!batchAttribs.isEmpty()) {
					sql = "INSERT INTO " + DBConstants.DBTABLE_TEMPORALENTITY_ATTRIBUTEINSTANCE
							+ " (temporalentity_id, attributeid, data) VALUES ($1, $2, $3::jsonb)";
					logger.debug("batch " + sql);
					attribList.add(client.preparedQuery(sql).executeBatch(batchAttribs));
				}
				if (!batchAttribsWtihLocation.isEmpty()) {
					sql = "INSERT INTO " + DBConstants.DBTABLE_TEMPORALENTITY_ATTRIBUTEINSTANCE
							+ " (temporalentity_id, attributeid, data, location) VALUES ($1, $2, $3::jsonb, ST_SetSRID(ST_GeomFromGeoJSON(getGeoJson($4)), 4326))";
					logger.debug("batch location " + sql);
					attribList.add(client.preparedQuery(sql).executeBatch(batchAttribsWtihLocation));
				}
				return Uni.combine().all().unis(attribList).with(l2 -> l2).onItem().transform(l1 -> l1);

			}).onItem().transformToUni(t -> Uni.createFrom().voidItem());
		});
		// });
	}

	public Uni<Void> setDeletedBatchHistoryEntity(BatchRequest request) {
		return clientManager.getClient(request.getTenant(), false).onItem().transformToUni(client -> {
			List<Tuple> batch = Lists.newArrayList();
			request.getIds().forEach(entityId -> {
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
			String sql = "DELETE FROM " + DBConstants.DBTABLE_TEMPORALENTITY + " WHERE id = $1 RETURNING "
					+ DBConstants.DBTABLE_TEMPORALENTITY;
			return client.preparedQuery(sql).execute(Tuple.of(request.getFirstId())).onFailure().recoverWithUni(e -> {
				if (e instanceof PgException pge) {
					if (pge.getSqlState().equals(AppConstants.SQL_NOT_FOUND)) {
						return Uni.createFrom().failure(
								new ResponseException(ErrorType.NotFound, request.getFirstId() + " does not exist"));
					}
				}
				return Uni.createFrom().failure(e);
			}).onItem().transformToUni(rows -> {
				if (rows.size() == 0) {
					return Uni.createFrom().failure(
							new ResponseException(ErrorType.NotFound, request.getFirstId() + " does not exist"));
				}
				return Uni.createFrom().voidItem();
			});
		});
	}

	public Uni<Void> appendToHistoryEntity(AppendHistoryEntityRequest request) {
		return clientManager.getClient(request.getTenant(), true).onItem().transformToUni(client -> {
			Map<String, Object> payload = request.getFirstPayload();
			// return client.getConnection().onItem().transformToUni(conn -> {
			Tuple tuple = Tuple.tuple();
			tuple.addString(((List<Map<String, String>>) payload.remove(NGSIConstants.NGSI_LD_MODIFIED_AT)).get(0)
					.get(NGSIConstants.JSON_LD_VALUE));

			StringBuilder sql = new StringBuilder("UPDATE ");
			sql.append(DBConstants.DBTABLE_TEMPORALENTITY);
			sql.append(" SET modifiedat = $1::text::timestamp");
			int dollarCount = 2;
			if (payload.containsKey(NGSIConstants.JSON_LD_TYPE) && payload.get(NGSIConstants.JSON_LD_TYPE) != null) {
				sql.append(",e_types = ARRAY(SELECT DISTINCT UNNEST(e_types || $" + dollarCount + "))");
				tuple.addArrayOfString(
						((List<String>) payload.remove(NGSIConstants.JSON_LD_TYPE)).toArray(new String[0]));
				dollarCount++;
			}
			if (payload.containsKey(NGSIConstants.NGSI_LD_SCOPE)) {
				sql.append(", SET scopes = getScopes($");
				sql.append(dollarCount);
				sql.append("::jsonb)");
				dollarCount++;
				tuple.addJsonArray(
						new JsonArray((List<Map<String, String>>) payload.remove(NGSIConstants.NGSI_LD_SCOPE)));
			}
			sql.append(" WHERE id=$");
			sql.append(dollarCount);
			tuple.addString(request.getFirstId());
			payload.remove(NGSIConstants.JSON_LD_TYPE);
			payload.remove(NGSIConstants.JSON_LD_ID);
			return client.preparedQuery(sql.toString()).execute(tuple).onFailure().recoverWithUni(e -> {
				if (e instanceof PgException pge) {
					if (pge.getSqlState().equals(AppConstants.SQL_NOT_FOUND)) {
						return Uni.createFrom().failure(
								new ResponseException(ErrorType.NotFound, request.getFirstId() + " does not exist"));
					}
				}
				return Uni.createFrom().failure(e);
			}).onItem().transformToUni(rows -> {
				List<Tuple> batch = Lists.newArrayList();
				Object createdAt = payload.remove(NGSIConstants.NGSI_LD_CREATED_AT);
				for (Entry<String, Object> entry : payload.entrySet()) {
					List<Map<String, Object>> entries = (List<Map<String, Object>>) entry.getValue();
					for (Map<String, Object> attribEntry : entries) {
						if (createdAt != null) {
							attribEntry.put(NGSIConstants.NGSI_LD_CREATED_AT, createdAt);
						}
						attribEntry.put(NGSIConstants.NGSI_LD_INSTANCE_ID, List
								.of(Map.of(NGSIConstants.JSON_LD_ID, "instanceid:" + UUID.randomUUID().toString())));
						batch.add(Tuple.of(request.getFirstId(), entry.getKey(), new JsonObject(attribEntry)));
					}
				}
				return client
						.preparedQuery("INSERT INTO " + DBConstants.DBTABLE_TEMPORALENTITY_ATTRIBUTEINSTANCE
								+ " (temporalentity_id, attributeid, data) VALUES ($1, $2, $3::jsonb)")
						.executeBatch(batch);
			}).onItem().transformToUni(t -> Uni.createFrom().voidItem());

		});
		// });
	}

	public Uni<Void> updateAttrInstanceInHistoryEntity(UpdateAttrHistoryEntityRequest request) {
		return clientManager.getClient(request.getTenant(), true).onItem().transformToUni(client -> {
			Object payload = new JsonObject(
					((List<Map<String, Object>>) request.getFirstPayload().get(request.getAttribName())).get(0));
			// return client.getConnection().onItem().transformToUni(conn -> {
			return client.preparedQuery("UPDATE " + DBConstants.DBTABLE_TEMPORALENTITY_ATTRIBUTEINSTANCE
					+ " SET data = data || $1::jsonb WHERE attributeid=$2 AND instanceid=$3 AND temporalentity_id=$4")
					.execute(Tuple.of(payload, request.getAttribName(), request.getInstanceId(), request.getFirstId()))
					.onFailure().recoverWithUni(e -> {
						if (e instanceof PgException pge) {
							if (pge.getSqlState().equals(AppConstants.SQL_NOT_FOUND)) {
								return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound,
										request.getFirstId() + " does not exist"));
							}
						}
						return Uni.createFrom().failure(e);
					}).onItem().transformToUni(rows -> {
						return client
								.preparedQuery("UPDATE " + DBConstants.DBTABLE_TEMPORALENTITY
										+ " SET modifiedat = $1::text::timestamp WHERE id = $2")
								.execute(Tuple.of(((List<Map<String, String>>) request.getFirstPayload()
										.get(NGSIConstants.NGSI_LD_MODIFIED_AT)).get(0)
										.get(NGSIConstants.JSON_LD_VALUE), request.getFirstId()))
								.onItem().transformToUni(t -> Uni.createFrom().voidItem());
					});

		});
//		});
	}

	public Uni<Void> deleteAttrFromHistoryEntity(DeleteAttrHistoryEntityRequest request) {
		return clientManager.getClient(request.getTenant(), true).onItem().transformToUni(client -> {
			// return client.getConnection().onItem().transformToUni(conn -> {
			Uni<RowSet<Row>> query1;
			if (request.getDatasetId() == null) {
				if (request.isDeleteAll()) {
					query1 = client
							.preparedQuery("DELETE FROM " + DBConstants.DBTABLE_TEMPORALENTITY_ATTRIBUTEINSTANCE
									+ " WHERE attributeid=$1 AND temporalentity_id=$2")
							.execute(Tuple.of(request.getAttribName(), request.getFirstId()));
				} else {
					query1 = client
							.preparedQuery("DELETE FROM " + DBConstants.DBTABLE_TEMPORALENTITY_ATTRIBUTEINSTANCE
									+ " WHERE attributeid=$1 AND temporalentity_id=$2 AND NOT data ?| array['"
									+ NGSIConstants.NGSI_LD_DATA_SET_ID + "']")
							.execute(Tuple.of(request.getAttribName(), request.getFirstId()));
				}
			} else {
				query1 = client
						.preparedQuery("DELETE FROM " + DBConstants.DBTABLE_TEMPORALENTITY_ATTRIBUTEINSTANCE
								+ " WHERE attributeid=$1 AND temporalentity_id=$2 AND '"
								+ NGSIConstants.NGSI_LD_DATA_SET_ID + "' ? data AND data@>>'{"
								+ NGSIConstants.NGSI_LD_DATA_SET_ID + ",0," + NGSIConstants.JSON_LD_ID + "}'=$3")
						.execute(Tuple.of(request.getAttribName(), request.getFirstId(), request.getDatasetId()));
			}

			return query1.onFailure().recoverWithUni(e -> {
				if (e instanceof PgException pge) {
					if (pge.getSqlState().equals(AppConstants.SQL_NOT_FOUND)) {
						return Uni.createFrom().failure(
								new ResponseException(ErrorType.NotFound, request.getFirstId() + " does not exist"));
					}
				}
				return Uni.createFrom().failure(e);
			}).onItem().transformToUni(rows -> {
				return client
						.preparedQuery(
								"UPDATE " + DBConstants.DBTABLE_TEMPORALENTITY + " SET modifiedat = $1 WHERE id = $2")
						.execute(Tuple.of(LocalDateTime.ofInstant(Instant.ofEpochMilli(request.getSendTimestamp()),
								ZoneId.of("Z")), request.getFirstId()));

			}).onItem().transformToUni(t -> Uni.createFrom().voidItem());
		});
		// });
	}

	public Uni<Void> deleteAttrInstanceInHistoryEntity(DeleteAttrInstanceHistoryEntityRequest request) {
		return clientManager.getClient(request.getTenant(), true).onItem().transformToUni(client -> {
			// return client.getConnection().onItem().transformToUni(conn -> {
			return client
					.preparedQuery("DELETE FROM " + DBConstants.DBTABLE_TEMPORALENTITY_ATTRIBUTEINSTANCE
							+ " WHERE attributeid=$1 AND instanceid=$2 AND temporalentity_id=$3")
					.execute(Tuple.of(request.getAttribName(), request.getInstanceId(), request.getFirstId()))
					.onFailure().recoverWithUni(e -> {
						if (e instanceof PgException pge) {
							if (pge.getSqlState().equals(AppConstants.SQL_NOT_FOUND)) {
								return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound,
										request.getFirstId() + " does not exist"));
							}
						}
						return Uni.createFrom().failure(e);
					}).onItem().transformToUni(rows -> {
						return client
								.preparedQuery("UPDATE " + DBConstants.DBTABLE_TEMPORALENTITY
										+ " SET modifiedat = $1 WHERE id = $2")
								.execute(Tuple.of(LocalDateTime
										.ofInstant(Instant.ofEpochMilli(request.getSendTimestamp()), ZoneId.of("Z")),
										request.getFirstId()));
					}).onItem().transformToUni(t -> Uni.createFrom().voidItem());
		});
		// });
	}

	public Uni<Void> setEntityDeleted(BaseRequest request) {
		return clientManager.getClient(request.getTenant(), false).onItem().transformToUni(client -> {
			return client.preparedQuery("UPDATE temporalentity SET deletedat=$1 WHERE id=$2")
					// You would think that we do the conversion in the db but somehow postgres
					// can't easily convert utc into a timestamp without a timezone
					.execute(Tuple.of(
							LocalDateTime.ofInstant(Instant.ofEpochMilli(request.getSendTimestamp()), ZoneId.of("Z")),
							request.getFirstId()))
					.onItem().transformToUni(t -> Uni.createFrom().voidItem());
		});

	}

	public Uni<Void> setAttributeDeleted(BaseRequest request) {
		return clientManager.getClient(request.getTenant(), true).onItem().transformToUni(client -> {
			// return client.getConnection().onItem().transformToUni(conn -> {
			LocalDateTime now = LocalDateTime.ofInstant(Instant.ofEpochMilli(request.getSendTimestamp()),
					ZoneId.of("Z"));
			String nowString = SerializationTools.notifiedAt_formatter.format(now);
			String instanceId = UUID.randomUUID().toString();
			JsonObject deletePayload = getAttribDeletedPayload(nowString, instanceId, request.getDatasetId());
			return client.preparedQuery("INSERT INTO " + DBConstants.DBTABLE_TEMPORALENTITY_ATTRIBUTEINSTANCE
					+ " (temporalentity_id, attributeid, data, deletedat, instanceId) VALUES ($1, $2, $3::jsonb, $4, $5)")
					.execute(Tuple.of(request.getFirstId(), request.getAttribName(), deletePayload, now, instanceId))
					.onFailure().recoverWithUni(e -> {
						if (e instanceof PgException pge) {
							if (pge.getSqlState().equals(AppConstants.SQL_NOT_FOUND)) {
								return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound,
										request.getFirstId() + " does not exist"));
							}
						}
						return Uni.createFrom().failure(e);
					}).onItem().transformToUni(rows -> {
						return client
								.preparedQuery("UPDATE " + DBConstants.DBTABLE_TEMPORALENTITY
										+ " SET modifiedat = $1 WHERE id = $2")
								.execute(Tuple.of(now, request.getFirstId()));
					}).onItem().transformToUni(t -> Uni.createFrom().voidItem());
		});
		// });
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
		return DBUtil.getAllRegistries(clientManager, ldService,
				"SELECT cs_id, c_id, e_id, e_id_p, e_type, e_prop, e_rel, ST_AsGeoJSON(i_location), scopes, EXTRACT(MILLISECONDS FROM expires), endpoint, tenant_id, headers, reg_mode, createEntity, updateEntity, appendAttrs, updateAttrs, deleteAttrs, deleteEntity, createBatch, upsertBatch, updateBatch, deleteBatch, upsertTemporal, appendAttrsTemporal, deleteAttrsTemporal, updateAttrsTemporal, deleteAttrInstanceTemporal, deleteTemporal, mergeEntity, replaceEntity, replaceAttrs, mergeBatch, retrieveEntity, queryEntity, queryBatch, retrieveTemporal, queryTemporal, retrieveEntityTypes, retrieveEntityTypeDetails, retrieveEntityTypeInfo, retrieveAttrTypes, retrieveAttrTypeDetails, retrieveAttrTypeInfo, createSubscription, updateSubscription, retrieveSubscription, querySubscription, deleteSubscription, queryEntityMap, createEntityMap, updateEntityMap, deleteEntityMap, retrieveEntityMap  FROM csourceinformation WHERE upsertTemporal OR appendAttrsTemporal OR deleteAttrsTemporal OR updateAttrsTemporal OR deleteAttrInstanceTemporal OR deleteTemporal",
				logger);
	}

	public Uni<Void> setMergePatch(BaseRequest request) {
		Map<String, Object> newPayload = new HashMap<>();

		List<Uni<Void>> unis = new ArrayList<>();
		for (String key : request.getPayload().keySet()) {
			String value = request.getPayload().get(key).toString();
			if (value.contains(NGSIConstants.HAS_VALUE_NULL) || value.contains(NGSIConstants.HAS_OBJECT_NULL)) {
				// separating deleted attr from payload
				DeleteAttributeRequest deleteAttributeRequest = new DeleteAttributeRequest(request.getTenant(),
						request.getFirstId(), key, null, false, false);
				unis.add(setAttributeDeleted(deleteAttributeRequest));
			} else {
				newPayload.put(key, request.getPayload().get(key));
			}
		}
		if (newPayload.size() > 2) {// size > 2 because there is always id and modifiedat present in the payload
			AppendHistoryEntityRequest appendHistoryEntityRequest = new AppendHistoryEntityRequest(request.getTenant(),
					newPayload, request.getFirstId(), false);
			unis.add(appendToHistoryEntity(appendHistoryEntityRequest));
		}
		return Uni.combine().all().unis(unis).collectFailures().discardItems();
	}
}