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
import eu.neclab.ngsildbroker.commons.datatypes.requests.BatchRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CreateEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteAttributeRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.UpdateEntityRequest;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.storage.ClientManager;
import eu.neclab.ngsildbroker.commons.tools.DBUtil;
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

	public Uni<Map<String, Object>> batchCreateEntity(BatchRequest request) {
		return clientManager.getClient(request.getTenant(), true).onItem().transformToUni(client -> {
			Tuple tuple = Tuple.of(new JsonArray(request.getRequestPayload()));
			return client.preparedQuery("SELECT * FROM NGSILD_CREATEBATCH($1)").execute(tuple).onItem()
					.transform(rows -> {
						return rows.iterator().next().getJsonObject(0).getMap();
					});
		});
	}

	public Uni<Map<String, Object>> batchUpsertEntity(BatchRequest request) {
		return clientManager.getClient(request.getTenant(), true).onItem().transformToUni(client -> {
			return client.preparedQuery("SELECT * FROM NGSILD_UPSERTBATCH($1)")
					.execute(Tuple.of(new JsonArray(request.getRequestPayload()))).onItem().transform(rows -> {
						return rows.iterator().next().getJsonObject(0).getMap();
					});
		});
	}

	public Uni<Map<String, Object>> batchAppendEntity(BatchRequest request) {
		return clientManager.getClient(request.getTenant(), true).onItem().transformToUni(client -> {
			return client.preparedQuery("SELECT * FROM NGSILD_APPENDBATCH($1)")
					.execute(Tuple.of(new JsonArray(request.getRequestPayload()))).onItem().transform(rows -> {
						return rows.iterator().next().getJsonObject(0).getMap();
					});
		});
	}

	public Uni<Map<String, Object>> batchDeleteEntity(String tenant, List<String> entityIds) {
		return clientManager.getClient(tenant, true).onItem().transformToUni(client -> {
			return client.preparedQuery("SELECT * FROM NGSILD_DELETEBATCH($1)")
					.execute(Tuple.of(new JsonArray(entityIds))).onItem().transform(rows -> {
						return rows.iterator().next().getJsonObject(0).getMap();
					});
		});
	}

	public Uni<Void> partialUpdateAttribute(UpdateEntityRequest request) {
		return clientManager.getClient(request.getTenant(), false).onItem().transformToUni(client -> {
			Object objPayload = request.getPayload().get(request.getAttrName());
			Tuple tuple;
			if (objPayload instanceof List<?> payloads) {
				tuple = Tuple.of(request.getAttrName(), new JsonArray(payloads), request.getId());
			} else {
				List<Object> payloads = new ArrayList<>();
				payloads.add(objPayload);
				tuple = Tuple.of(request.getAttrName(), new JsonArray(payloads), request.getId());
			}
			String sql = "UPDATE ENTITY SET ENTITY = NGSILD_PARTIALUPDATE(ENTITY, $1, $2) WHERE id=$3 AND ENTITY ? $1 RETURNING ENTITY";
			return client.preparedQuery(sql).execute(tuple).onFailure().retry().atMost(3).onItem()
					.transformToUni(rows -> {
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
			String sql = "";
			Tuple tuple;
			sql += "UPDATE ENTITY SET ENTITY=";
			if (request.deleteAll()) {
				sql = "ENTITY - $1 WHERE id=$2 AND ENTITY ? $1";
				tuple = Tuple.of(request.getAttribName(), request.getId());
			} else if (request.getDatasetId() != null) {
				sql += "NGSILD_DELETEATTRIB(ENTITY, $1, $2) WHERE id=$3 AND ENTITY @> '{\"$1\": [{\""
						+ NGSIConstants.NGSI_LD_DATA_SET_ID + "\": \"$2\"}]}'";
				tuple = Tuple.of(request.getAttribName(), request.getDatasetId(), request.getId());
			} else {
				sql += "NGSILD_DELETEATTRIB(ENTITY, $1, null) WHERE id=$2 AND ENTITY ? $1 AND EXISTS (SELECT jsonb_array_elements FROM jsonb_array_elements(ENTITY->$1) WHERE NOT jsonb_array_elements ? '"
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

	public Uni<String> getEndpoint(String entityId, String tenantId) {
		String query = "SELECT endpoint FROM csource, csourceinformation csi WHERE csource.id=csi.id AND csi.e_id='"
				+ entityId + "'";
		return clientManager.getClient(tenantId, false).onItem()
				.transformToUni(client -> client.preparedQuery(query).execute().onItem().transform((rowSet) -> {
					if (rowSet.rowCount() == 0) {
						return null;
					}
					return rowSet.iterator().next().getString("endpoint");
				}).onFailure().recoverWithUni(Uni.createFrom().item("")));
	}

	public Uni<Table<String, String, List<RegistrationEntry>>> getAllRegistries() {
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
							if ((map.get(NGSIConstants.JSON_LD_TYPE) instanceof List<?> types
									&& types.get(0).equals(NGSIConstants.JSON_LD_NONE))
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
