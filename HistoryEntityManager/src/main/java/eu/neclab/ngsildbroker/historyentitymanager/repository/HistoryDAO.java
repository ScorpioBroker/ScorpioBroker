package eu.neclab.ngsildbroker.historyentitymanager.repository;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.DBConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.AppendHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CreateHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteAttrHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteAttrInstanceHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteAttributeRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.UpdateAttrHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.storage.ClientManager;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.sqlclient.PreparedQuery;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.Tuple;
import io.vertx.pgclient.PgException;

@Singleton
public class HistoryDAO {

	@Inject
	ClientManager clientManager;

	public Uni<Boolean> createHistoryEntity(CreateHistoryEntityRequest request) {
		return clientManager.getClient(request.getTenant(), true).onItem().transformToUni(client -> {
			Map<String, Object> payload = request.getPayload();
			return client.getConnection().onItem().transformToUni(conn -> {
				return conn.preparedQuery("INSERT INTO " + DBConstants.DBTABLE_TEMPORALENTITY
						+ " (id, e_types, createdat, modifiedat) VALUES($1, $2, $3::timestamp, $4::timestamp) "
						+ "ON CONFLICT(id) DO UPDATE SET e_types = ARRAY(SELECT DISTINCT UNNEST(e_types || EXCLUDED.e_types)), modifiedat = EXCLUDED.modifiedat RETURNING (modifiedat = createdat)")
						.execute(Tuple.of(payload.remove(NGSIConstants.JSON_LD_ID),
								payload.remove(NGSIConstants.JSON_LD_TYPE),
								payload.remove(NGSIConstants.NGSI_LD_CREATED_AT),
								payload.remove(NGSIConstants.NGSI_LD_MODIFIED_AT)))
						.onItem().transformToUni(rows -> {
							List<Tuple> batch = Lists.newArrayList();
							for (Entry<String, Object> entry : payload.entrySet()) {
								List<Map<String, Object>> entries = (List<Map<String, Object>>) entry.getValue();
								for (Map<String, Object> attribEntry : entries) {
									batch.add(Tuple.of(request.getId(), entry.getKey(), new JsonObject(attribEntry)));
								}
								//
							}
							return conn
									.preparedQuery("INSERT INTO " + DBConstants.DBTABLE_TEMPORALENTITY_ATTRIBUTEINSTANCE
											+ " (temporalentity_id, attributeid, data) VALUES ($1, $2, $3::jsonb")
									.executeBatch(batch).onItem()
									.transformToUni(t -> Uni.createFrom().item(rows.iterator().next().getBoolean(0)));
						});
			});
		});
	}

	public Uni<RowSet<Row>> deleteHistoryEntity(DeleteHistoryEntityRequest request) {
		return clientManager.getClient(request.getTenant(), true).onItem().transformToUni(client -> {
			String sql = "DELETE * FROM " + DBConstants.DBTABLE_TEMPORALENTITY + " WHERE id = $1";
			return client.preparedQuery(sql).execute(Tuple.of(request.getId())).onFailure().recoverWithUni(e -> {
				if (e instanceof PgException) {
					if (((PgException) e).getCode().equals(AppConstants.SQL_NOT_FOUND)) {
						return Uni.createFrom().failure(
								new ResponseException(ErrorType.NotFound, request.getId() + " does not exist"));
					}
				}
				return Uni.createFrom().failure(e);
			});
		});

	}

	public Uni<Void> appendToHistoryEntity(AppendHistoryEntityRequest request) {
		return clientManager.getClient(request.getTenant(), true).onItem().transformToUni(client -> {
			Map<String, Object> payload = request.getPayload();
			return client.getConnection().onItem().transformToUni(conn -> {
				Uni<RowSet<Row>> query1;
				if (payload.containsKey(NGSIConstants.JSON_LD_TYPE)) {
					query1 = conn.preparedQuery("UPDATE " + DBConstants.DBTABLE_TEMPORALENTITY
							+ " SET e_types = ARRAY(SELECT DISTINCT UNNEST(e_types || $1)), modifiedat = $2 WHERE id=$3")
							.execute(Tuple.of(payload.remove(NGSIConstants.JSON_LD_TYPE),
									payload.remove(NGSIConstants.NGSI_LD_MODIFIED_AT), request.getId()));
				} else {
					query1 = conn
							.preparedQuery(
									"UPDATE " + DBConstants.DBTABLE_TEMPORALENTITY + " SET modifiedat = $1 WHERE id=$2")
							.execute(Tuple.of(payload.remove(NGSIConstants.NGSI_LD_MODIFIED_AT), request.getId()));
				}
				payload.remove(NGSIConstants.JSON_LD_ID);
				return query1.onFailure().recoverWithUni(e -> {
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
						List<Map<String, Object>> entries = (List<Map<String, Object>>) entry.getValue();
						for (Map<String, Object> attribEntry : entries) {
							batch.add(Tuple.of(request.getId(), entry.getKey(), new JsonObject(attribEntry)));
						}
					}
					return conn
							.preparedQuery("INSERT INTO " + DBConstants.DBTABLE_TEMPORALENTITY_ATTRIBUTEINSTANCE
									+ " (temporalentity_id, attributeid, data) VALUES ($1, $2, $3::jsonb")
							.executeBatch(batch).onItem().transformToUni(t -> Uni.createFrom().voidItem());
				});

			});
		});
	}

	public Uni<Void> updateAttrInstanceInHistoryEntity(UpdateAttrHistoryEntityRequest request) {
		return clientManager.getClient(request.getTenant(), true).onItem().transformToUni(client -> {
			Map<String, Object> payload = request.getPayload();
			return client.getConnection().onItem().transformToUni(conn -> {
				return conn
						.preparedQuery("UPDATE " + DBConstants.DBTABLE_TEMPORALENTITY_ATTRIBUTEINSTANCE
								+ " SET data VALUES ($1::jsonb) WHERE attributeid=$2 AND instanceid=$3 AND id=$4")
						.execute(Tuple.of(new JsonObject(payload), request.getAttrId(), request.getInstanceId(),
								request.getId()))
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
									.execute(Tuple.of(payload.get(NGSIConstants.NGSI_LD_MODIFIED_AT), request.getId()))
									.onItem().transformToUni(t -> Uni.createFrom().voidItem());
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
										+ " WHERE attributeid=$1 AND id=$2")
								.execute(Tuple.of(request.getAttribName(), request.getId()));
					} else {
						query1 = conn
								.preparedQuery("DELETE FROM " + DBConstants.DBTABLE_TEMPORALENTITY_ATTRIBUTEINSTANCE
										+ " WHERE attributeid=$1 AND id=$2 AND NOT '"
										+ NGSIConstants.NGSI_LD_DATA_SET_ID + "' ? data")
								.execute(Tuple.of(request.getAttribName(), request.getId()));
					}
				} else {
					query1 = conn
							.preparedQuery("DELETE FROM " + DBConstants.DBTABLE_TEMPORALENTITY_ATTRIBUTEINSTANCE
									+ " WHERE attributeid=$1 AND id=$2 AND '" + NGSIConstants.NGSI_LD_DATA_SET_ID
									+ "' ? data AND data@>>'{" + NGSIConstants.NGSI_LD_DATA_SET_ID + ",0,"
									+ NGSIConstants.JSON_LD_ID + "}'=$3")
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
							.execute(Tuple.of(
									SerializationTools.notifiedAt_formatter.format(LocalDateTime.ofInstant(
											Instant.ofEpochMilli(request.getSendTimestamp()), ZoneId.of("Z"))),
									request.getId()))
							.onItem().transformToUni(t -> Uni.createFrom().voidItem());
				});
			});
		});
	}

	public Uni<Void> deleteAttrInstanceInHistoryEntity(DeleteAttrInstanceHistoryEntityRequest request) {
		return clientManager.getClient(request.getTenant(), true).onItem().transformToUni(client -> {
			return client.getConnection().onItem().transformToUni(conn -> {
				return conn
						.preparedQuery("DELETE FROM " + DBConstants.DBTABLE_TEMPORALENTITY_ATTRIBUTEINSTANCE
								+ " WHERE attributeid=$2 AND instanceid=$3 AND id=$4")
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
											SerializationTools.notifiedAt_formatter.format(LocalDateTime.ofInstant(
													Instant.ofEpochMilli(request.getSendTimestamp()), ZoneId.of("Z"))),
											request.getId()))
									.onItem().transformToUni(t -> Uni.createFrom().voidItem());
						});
			});
		});
	}

	public Uni<Void> setEntityDeleted(BaseRequest request) {
		return clientManager.getClient(request.getTenant(), false).onItem().transformToUni(client -> {
			return client.preparedQuery("UPDATE temporalentity SET deletedat='$1'::TIMESTAMP")
					// You would think that we do the conversion in the db but somehow postgres
					// can't easily convert utc into a timestamp without a timezone
					.execute(Tuple.of(SerializationTools.notifiedAt_formatter.format(
							LocalDateTime.ofInstant(Instant.ofEpochMilli(request.getSendTimestamp()), ZoneId.of("Z")))))
					.onItem().transformToUni(t -> Uni.createFrom().voidItem());
		});

	}

	public Uni<Void> setAttributeDeleted(DeleteAttributeRequest request) {
		return clientManager.getClient(request.getTenant(), true).onItem().transformToUni(client -> {
			return client.getConnection().onItem().transformToUni(conn -> {
				String now = SerializationTools.notifiedAt_formatter.format(
						LocalDateTime.ofInstant(Instant.ofEpochMilli(request.getSendTimestamp()), ZoneId.of("Z")));
				String instanceId = UUID.randomUUID().toString();
				JsonObject deletePayload = getAttribDeletedPayload(now, instanceId, request.getDatasetId());
				return conn.preparedQuery("INSERT INTO " + DBConstants.DBTABLE_TEMPORALENTITY_ATTRIBUTEINSTANCE
						+ " (temporalentity_id, attributeid, data, deletedat, instanceId) VALUES ($1, $2, $3::jsonb, $4::TIMESTAMP, $5")
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

}