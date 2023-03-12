package eu.neclab.ngsildbroker.historyentitymanager.repository;

import java.io.IOException;
import java.text.ParseException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import javax.inject.Inject;
import javax.inject.Singleton;

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
import eu.neclab.ngsildbroker.commons.datatypes.requests.CreateHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteAttrHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteAttrInstanceHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteAttributeRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.UpdateAttrHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.storage.ClientManager;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.MultiMap;
import io.vertx.mutiny.sqlclient.PreparedQuery;
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

	GeoJSONReader geoReader = new GeoJSONReader(JtsSpatialContext.GEO, new SpatialContextFactory());

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

	public Uni<Table<String, String, RegistrationEntry>> getAllRegistries() {
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
							Table<String, String, RegistrationEntry> result = HashBasedTable.create();
							for (Object obj : list) {
								Tuple2<String, RowSet<Row>> tuple = (Tuple2<String, RowSet<Row>>) obj;
								String tenant = tuple.getItem1();
								RowIterator<Row> it2 = tuple.getItem2().iterator();
								while (it2.hasNext()) {
									Row row = it2.next();
									result.put(tenant, row.getString(1), getRegistrationEntry(row, tenant));
								}
							}
							return result;
						});
					});
		});

	}

	private RegistrationEntry getRegistrationEntry(Row row, String tenant) {
//		cs_id bigint,
//		c_id text,  
//		e_id text,
//		e_id_p text,
//		e_type text,
//		e_prop text,
//		e_rel text,
//		i_location GEOMETRY(Geometry, 4326),
//		scopes text[],
//		expires timestamp without time zone,
//		endpoint text,
//		tenant_id text,
//		headers jsonb,
//		reg_mode smallint,
//		createEntity boolean, 13 14 15 16 17 18 19 20
//		updateEntity boolean,
//		appendAttrs boolean,
//		updateAttrs boolean,
//		deleteAttrs boolean,
//		deleteEntity boolean,
//		createBatch boolean,
//		upsertBatch boolean,
//		updateBatch boolean,
//		deleteBatch boolean,
//		upsertTemporal boolean,
//		appendAttrsTemporal boolean,
//		deleteAttrsTemporal boolean,
//		updateAttrsTemporal boolean,
//		deleteAttrInstanceTemporal boolean,
//		deleteTemporal boolean,
//		mergeEntity boolean,
//		replaceEntity boolean,
//		replaceAttrs boolean,
//		mergeBatch boolean,
//		retrieveEntity boolean,
//		queryEntity boolean,
//		queryBatch boolean,
//		retrieveTemporal boolean,
//		queryTemporal boolean,
//		retrieveEntityTypes boolean,
//		retrieveEntityTypeDetails boolean,
//		retrieveEntityTypeInfo boolean,
//		retrieveAttrTypes boolean,
//		retrieveAttrTypeDetails boolean,
//		retrieveAttrTypeInfo boolean,
//		createSubscription boolean,
//		updateSubscription boolean,
//		retrieveSubscription boolean,
//		querySubscription boolean,
//		deleteSubscription boolean,

		// 1 2 3 4 5 6 7geo, 8 scopes 9endpoint 10 tenant 11 headers 12 regMode 13 - ops

		Shape geoJson = null;
		String geoString = row.getString(7);
		if (geoString != null) {
			try {
				geoJson = geoReader.read(geoString);
			} catch (InvalidShapeException | IOException | ParseException e) {
				logger.error("Failed to load registrations for the entity mananger", e);
			}
		}
		return new RegistrationEntry(row.getString(1), row.getString(2), row.getString(3), row.getString(4),
				row.getString(5), row.getString(6), geoJson, row.getArrayOfStrings(8), row.getLong(9),
				row.getInteger(12), row.getBoolean(13), row.getBoolean(14), row.getBoolean(15), row.getBoolean(16),
				row.getBoolean(17), row.getBoolean(18), row.getBoolean(19), row.getBoolean(20), row.getBoolean(21),
				row.getBoolean(22), row.getBoolean(23), row.getBoolean(24), row.getBoolean(25), row.getBoolean(26),
				row.getBoolean(27), row.getBoolean(28), row.getBoolean(29), row.getBoolean(30), row.getBoolean(31),
				row.getBoolean(32), row.getBoolean(33), row.getBoolean(34), row.getBoolean(35), row.getBoolean(36),
				row.getBoolean(37), row.getBoolean(38), row.getBoolean(39), row.getBoolean(40), row.getBoolean(41),
				row.getBoolean(42), row.getBoolean(43), row.getBoolean(44), row.getBoolean(45), row.getBoolean(46),
				row.getBoolean(47), row.getBoolean(48),
				new RemoteHost(row.getString(9), row.getString(10),
						MultiMap.newInstance(
								HttpUtils.getHeadersForRemoteCall(row.getJsonArray(11), row.getString(10))),
						row.getString(1), false, false));

	}

}