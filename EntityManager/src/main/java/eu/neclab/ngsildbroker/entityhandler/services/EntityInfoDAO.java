package eu.neclab.ngsildbroker.entityhandler.services;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.commons.lang3.StringUtils;
import org.locationtech.spatial4j.context.SpatialContextFactory;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.exception.InvalidShapeException;
import org.locationtech.spatial4j.io.GeoJSONReader;
import org.locationtech.spatial4j.shape.Shape;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.RegistrationEntry;
import eu.neclab.ngsildbroker.commons.datatypes.RemoteHost;
import eu.neclab.ngsildbroker.commons.datatypes.requests.AppendEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BatchCreateRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CreateEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteAttributeRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.UpdateEntityRequest;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.storage.ClientManager;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
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
public class EntityInfoDAO {

	private static Logger logger = LoggerFactory.getLogger(EntityInfoDAO.class);

	@Inject
	ClientManager clientManager;

	GeoJSONReader geoReader = new GeoJSONReader(JtsSpatialContext.GEO, new SpatialContextFactory());

	public Uni<Map<String, Object>> batchCreateEntity(BatchCreateRequest request) {
		return clientManager.getClient(request.getTenant(), true).onItem().transformToUni(client -> {
			return client.preparedQuery(
					"SELECT * FROM NGSILD_CREATEBATCH($1)")
					.execute(Tuple.of(new JsonArray(request.getRequestPayload()))).onItem().transform(rows -> {
						return rows.iterator().next().getJsonObject(0).getMap();
					});
		});
	}

	public Uni<Void> partialUpdateAttribute(UpdateEntityRequest request) {
		return clientManager.getClient(request.getTenant(), false).onItem().transformToUni(client -> {
			Object objPayload =  request.getPayload()
					.get(request.getAttrName());
			Tuple tuple;
			if(objPayload instanceof List<?> payloads){
				tuple = Tuple.of(request.getAttrName(), new JsonArray(payloads) , request.getId());
			}else {
				List<Object> payloads =new ArrayList<>();
				payloads.add(objPayload);
				tuple = Tuple.of(request.getAttrName(), new JsonArray(payloads) , request.getId());
			}
			String sql = "UPDATE ENTITY SET ENTITY = NGSILD_PARTIALUPDATE(ENTITY, $1, $2) WHERE id=$3 AND ENTITY ? $1 RETURNING ENTITY";
			return client.preparedQuery(sql)
					.execute(tuple)
					.onFailure().retry().atMost(3).onItem().transformToUni(rows -> {
						if (rows.size() == 0) {
							return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound));
						} else {
							return Uni.createFrom().voidItem();
						}
					});
		});
	}

	public Uni<Map<String, Object>> deleteAttribute(DeleteAttributeRequest request) {
		return clientManager.getClient(request.getTenant(), false).onItem().transformToUni(client -> {
			String sql;
			Tuple tuple;
			sql = "UPDATE ENTITY SET ENTITY=";
			if (request.deleteAll()) {
				sql = "ENTITY - $1 WHERE id=$2 AND ENTITY ? $1";
				tuple = Tuple.of(request.getAttribName(), request.getId());
			} else if (request.getDatasetId() != null) {
				sql = "NGSILD_DELETEATTRIB(ENTITY, $1, $2) WHERE id=$3 AND ENTITY @> '{\"$1\": [{\""
						+ NGSIConstants.NGSI_LD_DATA_SET_ID + "\": \"$2\"}]}'";
				tuple = Tuple.of(request.getAttribName(), request.getDatasetId(), request.getId());
			} else {
				sql = "NGSILD_DELETEATTRIB(ENTITY, $1, null) WHERE id=$2 AND ENTITY ? $1 AND EXISTS (SELECT jsonb_array_elements FROM jsonb_array_elements(ENTITY->$1) WHERE NOT jsonb_array_elements ? '"
						+ NGSIConstants.NGSI_LD_DATA_SET_ID + "')";
				tuple = Tuple.of(request.getAttribName(), request.getId());
			}
			sql += " RETURNING ENTITY";
			return client.preparedQuery(sql).execute(tuple).onFailure().retry().atMost(3).onItem()
					.transformToUni(rows -> {
						if (rows.size() == 0) {
							return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound));
						}
						return Uni.createFrom().item(rows.iterator().next().getJsonObject(0).getMap());
					});
		});
	}

	public Uni<RowSet<Row>> upsertEntity(CreateEntityRequest request) {
		return clientManager.getClient(request.getTenant(), true).onItem().transformToUni(client -> {
			String sql = "SELECT * FROM NGSILD_UPSERTENTITY($1::jsonb)";
			return client.preparedQuery(sql).execute(Tuple.of(new JsonObject(request.getPayload()))).onFailure().retry()
					.atMost(3).onFailure().recoverWithUni(e -> Uni.createFrom().failure(e));
		});
	}

	public Uni<Void> createEntity(CreateEntityRequest request) {
		return clientManager.getClient(request.getTenant(), true).onItem().transformToUni(client -> {
			String[] types = ((List<String>) request.getPayload().get(NGSIConstants.JSON_LD_TYPE))
					.toArray(new String[0]);
			return client.preparedQuery("INSERT INTO ENTITY(ID,E_TYPES, ENTITY) VALUES ($1, $2, $3)")
					.execute(Tuple.of(request.getId(), types, new JsonObject(request.getPayload()))).onFailure()
					.recoverWithUni(e -> {
						if (e instanceof PgException) {
							if (((PgException) e).getCode().equals(AppConstants.SQL_ALREADY_EXISTS)) {
								return Uni.createFrom().failure(new ResponseException(ErrorType.AlreadyExists,
										request.getId() + " already exists"));
							}
						}
						return Uni.createFrom().failure(e);
					}).onItem().transformToUni(v -> Uni.createFrom().voidItem());
		});
	}

	public Uni<Table<String, String, RegistrationEntry>> getAllRegistries() {
		return clientManager.getClient(AppConstants.INTERNAL_NULL_KEY, false).onItem().transformToUni(client -> {
			return client.preparedQuery("SELECT tenant_id FROM tenant").execute().onItem()
					.transformToUni(tenantRows -> {
						List<Uni<Tuple2<String, RowSet<Row>>>> unis = Lists.newArrayList();
						RowIterator<Row> it = tenantRows.iterator();
						String sql = "SELECT cs_id, c_id, e_id, e_id_p, e_type, e_prop, e_rel, ST_AsGeoJSON(i_location), scopes, EXTRACT(MILLISECONDS FROM expires), endpoint, tenant_id, headers, reg_mode, createEntity, updateEntity, appendAttrs, updateAttrs, deleteAttrs, deleteEntity, createBatch, upsertBatch, updateBatch, deleteBatch, upsertTemporal, appendAttrsTemporal, deleteAttrsTemporal, updateAttrsTemporal, deleteAttrInstanceTemporal, deleteTemporal, mergeEntity, replaceEntity, replaceAttrs, mergeBatch, retrieveEntity, queryEntity, queryBatch, retrieveTemporal, queryTemporal, retrieveEntityTypes, retrieveEntityTypeDetails, retrieveEntityTypeInfo, retrieveAttrTypes, retrieveAttrTypeDetails, retrieveAttrTypeInfo, createSubscription, updateSubscription, retrieveSubscription, querySubscription, deleteSubscription FROM csourceinformation WHERE createEntity OR createBatch OR updateEntity OR appendAttrs OR deleteAttrs OR deleteEntity OR upsertBatch OR updateBatch OR deleteBatch";
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

	public Uni<Void> updateEntity(UpdateEntityRequest request) {
		return clientManager.getClient(request.getTenant(), false).onItem().transformToUni(client -> {
			Map<String, Object> payload = request.getPayload();
			payload.remove(NGSIConstants.JSON_LD_ID);
			Object types = payload.remove(NGSIConstants.JSON_LD_TYPE);

			List<String> toBeRemoved = removeAtNoneEntries(payload);
			int dollar = 2;
			Tuple tuple = Tuple.tuple();

			String sql;

			sql = "UPDATE ENTITY SET ";
			if (types != null) {
				sql += "e_types = ARRAY(SELECT DISTINCT UNNEST(e_types || $1)), ENTITY = (jsonb_set(ENTITY, '{@type}', array_to_json(e_types)::jsonb) || $2)";
				dollar = 3;
				tuple.addArrayOfString(((List<String>) types).toArray(new String[0]));
				tuple.addJsonObject(new JsonObject(payload));
			} else {
				sql += " ENTITY = (ENTITY || $1) ";
				tuple.addJsonObject(new JsonObject(payload));
			}
			if (!toBeRemoved.isEmpty()) {
				for (String remove : toBeRemoved) {
					sql += " - '$" + dollar + "'";
					dollar++;
					tuple.addString(remove);
				}
			}
			sql += "WHERE ID = $" + dollar;
			tuple.addString(request.getId());
			return client.preparedQuery(sql).execute(tuple).onFailure()
					.recoverWithUni(e -> Uni.createFrom().failure(new ResponseException(ErrorType.NotFound))).onItem()
					.transformToUni(rows -> {
						if (rows.rowCount() == 0) {
							return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound));
						}
						return Uni.createFrom().voidItem();
					});
		});

	}

	private List<String> removeAtNoneEntries(Map<String, Object> payload) {
		Iterator<Entry<String, Object>> it = payload.entrySet().iterator();
		List<String> result = new ArrayList<>();
		while (it.hasNext()) {
			Entry<String, Object> entry = it.next();
			Object obj = entry.getValue();
			if (obj instanceof List) {
				List<Object> list = ((List<Object>) obj);
				Iterator<Object> it2 = list.iterator();
				Object tmp;
				while (it2.hasNext()) {
					tmp = it2.next();
					if (tmp instanceof Map) {
						Map<String, Object> map = (Map<String, Object>) tmp;
						if (map.containsKey(NGSIConstants.JSON_LD_TYPE)) {
							if ((map.get(NGSIConstants.JSON_LD_TYPE) instanceof List<?> types && types.get(0)
									.equals(NGSIConstants.JSON_LD_NONE))
							|| map.get(NGSIConstants.JSON_LD_TYPE).equals(NGSIConstants.JSON_LD_NONE)) {
								it2.remove();
							}
						} else {
							removeAtNoneEntries(map);
						}
					}
				}
				if (list.isEmpty()) {
					it.remove();
					result.add(entry.getKey());
				}
			}
		}
		return result;
	}

	/**
	 * 
	 * @param request
	 * @param noOverwrite
	 * @return the not added attribs
	 */
	public Uni<Set<String>> appendToEntity2(AppendEntityRequest request, boolean noOverwrite) {
		return clientManager.getClient(request.getTenant(), false).onItem().transformToUni(client -> {
			Map<String, Object> payload = request.getPayload();
			payload.remove(NGSIConstants.JSON_LD_ID);
			Object types = payload.remove(NGSIConstants.JSON_LD_TYPE);
			int dollar = 2;
			Tuple tuple = Tuple.tuple();

			String sql;

			sql = "UPDATE ENTITY SET ";
			if (types != null) {
				sql += "e_types = ARRAY(SELECT DISTINCT UNNEST(e_types || $1)), ";
				if (noOverwrite) {
					sql += "ENTITY = ($2 || jsonb_set(ENTITY, '{@type}', array_to_json(e_types)::jsonb))";
				} else {
					sql += "ENTITY = (jsonb_set(ENTITY, '{@type}', array_to_json(e_types)::jsonb) || $2)";
				}

				dollar = 3;
				tuple.addArrayOfString(((List<String>) types).toArray(new String[0]));
				tuple.addJsonObject(new JsonObject(payload));
			} else {
				if (noOverwrite) {
					sql += " ENTITY = ($1 || ENTITY) ";
				} else {
					sql += " ENTITY = (ENTITY || $1) ";
				}
				tuple.addJsonObject(new JsonObject(payload));
			}

			sql += "WHERE ID = $" + dollar;
			if (noOverwrite) {
				sql += " RETURNING ENTITY";
			}
			tuple.addString(request.getId());
			return client.preparedQuery(sql).execute(tuple).onItem().transform(rows -> {
				if (noOverwrite) {
					// TODO return the not added stuff from noOverwrite
					return new HashSet<>(0);
				} else {
					return new HashSet<>(0);
				}
			});
		});

	}

	public Uni<Map<String, Object>> deleteEntity(DeleteEntityRequest request) {
		return clientManager.getClient(request.getTenant(), false).onItem().transformToUni(client -> {
			return client.preparedQuery("DELETE FROM ENTITY WHERE id=$1 RETURNING ENTITY")
					.execute(Tuple.of(request.getId())).onItem().transformToUni(rows -> {
						if (rows.rowCount() == 0) {
							return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound));
						}
						return Uni.createFrom().item(rows.iterator().next().getJsonObject(0).getMap());
					});
		});
	}

}
