package eu.neclab.ngsildbroker.entityhandler.services;

import com.github.jsonldjava.core.JsonLDService;
import com.github.jsonldjava.core.JsonLdConsts;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.RegistrationEntry;
import eu.neclab.ngsildbroker.commons.datatypes.requests.AppendEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BatchRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CreateEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteAttributeRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.MergePatchRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.ReplaceAttribRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.ReplaceEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.UpdateEntityRequest;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.storage.ClientManager;
import eu.neclab.ngsildbroker.commons.tools.DBUtil;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple3;
import io.smallrye.mutiny.tuples.Tuples;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.Tuple;
import io.vertx.pgclient.PgException;
import org.locationtech.spatial4j.context.SpatialContextFactory;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.io.GeoJSONReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

@Singleton
public class EntityInfoDAO {

	private static Logger logger = LoggerFactory.getLogger(EntityInfoDAO.class);

	@Inject
	ClientManager clientManager;

	@Inject
	JsonLDService ldService;

	GeoJSONReader geoReader = new GeoJSONReader(JtsSpatialContext.GEO, new SpatialContextFactory());

	public Uni<Map<String, Object>> newbatchCreateEntity(BatchRequest request) {
		return clientManager.getClient(request.getTenant(), true).onItem().transformToUni(client -> {
			List<Map<String, Object>> entities = Lists.newArrayList();
			request.getPayload().values().forEach(entityList -> {
				entities.addAll(entityList);
			});
			Tuple tuple = Tuple.of(new JsonArray(entities));
			String sql = """
					with a as (SELECT entity, entity->'@id' as id, ARRAY(SELECT jsonb_array_elements_text(entity->'@type')) as e_types FROM jsonb_array_elements($1) as entity)
					insert into entity(id, e_types, entity) select a.id, a.e_types, a.entity from a ON CONFLICT(id) DO NOTHING RETURNING id, (xmax = 0) AS inserted;
					""";

			return client.preparedQuery(sql).execute(tuple).onItem().transform(rows -> {
				Map<String, Object> result = new HashMap<>(2);
				ArrayList<String> success = new ArrayList<>();
				ArrayList<Map<String, Object>> fails = new ArrayList<>();
				result.put("success", success);
				result.put("failure", fails);
				rows.forEach(row -> {
					if (row.getBoolean(1)) {
						success.add(row.getString(0));
					} else {
						Map<String, String> map = new HashMap<>(1);
						map.put(row.getString(0), AppConstants.SQL_ALREADY_EXISTS);
					}
				});
				return result;
			}).onFailure().recoverWithUni(e -> {
				if (e instanceof PgException pge) {
					logger.error(pge.getDetail());
				}
				logger.error("Failed to store entities in batch create.", e);
				return Uni.createFrom().failure(e);
			});
		});
	}

	public Uni<Map<String, Object>> batchCreateEntity(BatchRequest request) {
		return clientManager.getClient(request.getTenant(), true).onItem().transformToUni(client -> {
			List<Map<String, Object>> entities = Lists.newArrayList();
			request.getPayload().values().forEach(entityList -> {
				entities.addAll(entityList);
			});
			Tuple tuple = Tuple.of(new JsonArray(entities));
			return client.preparedQuery("SELECT * FROM NGSILD_CREATEBATCH($1)").execute(tuple).onItem()
					.transform(rows -> {
						return rows.iterator().next().getJsonObject(0).getMap();
					}).onFailure().recoverWithUni(e -> {
						if (e instanceof PgException pge) {
							logger.error(pge.getDetail());
						}
						logger.error("Failed to store entities in batch create.", e);
						return Uni.createFrom().failure(e);
					});
		});
	}

	public Uni<Map<String, Object>> oldbatchUpsertEntity(BatchRequest request, boolean doReplace) {
		return clientManager.getClient(request.getTenant(), true).onItem().transformToUni(client -> {
			List<Map<String, Object>> entities = Lists.newArrayList();
			request.getPayload().values().forEach(entityList -> {
				entities.addAll(entityList);
			});
			Tuple tuple = Tuple.of(new JsonArray(entities), doReplace);
			return client.preparedQuery("SELECT * FROM NGSILD_UPSERTBATCH($1, $2)").execute(tuple).onItem()
					.transform(rows -> {
						return rows.iterator().next().getJsonObject(0).getMap();
					});
		});
	}

	public Uni<Map<String, Object>> batchUpsertEntity(BatchRequest request, boolean doReplace) {
		return clientManager.getClient(request.getTenant(), true).onItem().transformToUni(client -> {

			List<Map<String, Object>> entities = Lists.newArrayList();
			request.getPayload().values().forEach(entityList -> {
				entities.add(mergeAllEntities(entityList));
			});
			Tuple tuple = Tuple.of(new JsonArray(entities));
			String sql = """
					with a as (SELECT jsonb_array_elements($1) as entity),
					b as (SELECT a.entity->>'@id' as id, a.entity as entity, entity.entity as old_entity from a left join entity on a.entity->>'@id' = entity.id),
					c as (insert into entity(id, e_types, entity) select b.id, ARRAY(SELECT jsonb_array_elements_text(b.entity->'@type')), b.entity from b ON CONFLICT (id) DO UPDATE SET id = EXCLUDED.id,e_types = ARRAY(SELECT DISTINCT UNNEST(entity.e_types || EXCLUDED.e_types)),entity =
					""";
			if (doReplace) {
				sql += "EXCLUDED.entity ";
			} else {
				sql += "ngsild_update_entity(EXCLUDED.entity, entity.entity, true) ";
			}

			sql += "RETURNING id, entity, (xmax = 0) AS inserted) select c.id, c.inserted, c.entity, b.old_entity from c left join b on c.id = b.id";

			logger.debug(sql);
			logger.debug(tuple.deepToString());
			return client.preparedQuery(sql).execute(tuple).onItem().transform(rows -> {
				Map<String, Object> result = new HashMap<>(2);
				ArrayList<Map<String, Object>> success = new ArrayList<>(rows.size());
				result.put("success", success);
				result.put("failure", new ArrayList<Map<String, Object>>(0));
				rows.forEach(row -> {
					Map<String, Object> tmp = new HashMap<>(4);
					tmp.put("id", row.getString(0));
					tmp.put("updated", !row.getBoolean(1));
					JsonObject tmpObj = row.getJsonObject(3);
					if (tmpObj != null) {
						tmp.put("old", tmpObj.getMap());
					} else {
						tmp.put("old", null);
					}
					tmp.put("new", row.getJsonObject(2).getMap());
					success.add(tmp);
				});
				return result;
			});
		});
	}

	private Map<String, Object> mergeAllEntities(List<Map<String, Object>> entityList) {
		if (entityList.size() == 1) {
			return entityList.get(0);
		}
		Map<String, Object> first = new HashMap<>();
		for (int i = 0; i < entityList.size(); i++) {
			first.putAll(entityList.get(i));

		}
		return first;
	}

	public Uni<Map<String, Object>> batchAppendEntity(BatchRequest request) {
		return clientManager.getClient(request.getTenant(), true).onItem().transformToUni(client -> {
			List<Map<String, Object>> entities = Lists.newArrayList();
			request.getPayload().values().forEach(entityList -> {
				entities.addAll(entityList);
			});
			Tuple tuple = Tuple.of(new JsonArray(entities), request.isNoOverwrite());
			return client.preparedQuery("SELECT * FROM NGSILD_APPENDBATCH($1, $2)").execute(tuple).onItem()
					.transform(rows -> {
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

	@SuppressWarnings("unchecked")
	/**
	 * 
	 * @param request
	 * @return old version of the entity
	 */
	public Uni<Map<String, Object>> partialUpdateAttribute(UpdateEntityRequest request) {
		return clientManager.getClient(request.getTenant(), false).onItem().transformToUni(client -> {
			Object objPayload = request.getFirstPayload().get(request.getAttribName());
			Tuple tuple;
			List<Object> payloads = new ArrayList<>();
			if (objPayload instanceof List<?>) {
				payloads = (List<Object>) objPayload;
			} else {
				payloads.add(objPayload);
			}
			((Map<String, Object>) payloads.get(0)).remove(NGSIConstants.NGSI_LD_CREATED_AT);
			tuple = Tuple.of(request.getAttribName(), new JsonArray(payloads), request.getFirstId());
			String sql = """
					WITH old_entity AS (
					    SELECT ENTITY
					    FROM ENTITY
					    WHERE id = $3
					)
					UPDATE ENTITY
					SET ENTITY = NGSILD_PARTIALUPDATE(ENTITY, $1, $2)
					WHERE id = $3 AND ENTITY ? $1
					RETURNING (SELECT ENTITY FROM old_entity) AS old_entry;
					""";
			return client.preparedQuery(sql).execute(tuple).onItem().transformToUni(rows -> {
				if (rows.size() == 0) {
					return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound,
							"Entity " + request.getFirstId() + " was not found"));
				}
				Row first = rows.iterator().next();
				JsonObject result = first.getJsonObject(0);
				if (result == null) {
					return Uni.createFrom().nullItem();
				}
				return Uni.createFrom().item(result.getMap());
			});
		}).onFailure().recoverWithUni(e -> {
			logger.error("Failed to add because of unknown error", e);
			return Uni.createFrom().failure(e);
		});
	}

	/**
	 * 
	 * @param request
	 * @return old version of the entity
	 */
	public Uni<Map<String, Object>> deleteAttribute(DeleteAttributeRequest request) {
		return clientManager.getClient(request.getTenant(), false).onItem().transformToUni(client -> {
			StringBuilder sql = new StringBuilder("""
					WITH old_entity AS (
					    SELECT ENTITY
					    FROM ENTITY
					    WHERE id = $2
					)""");
			Tuple tuple;
			sql.append("UPDATE ENTITY SET ENTITY=");
			if (request.isDeleteAll()) {
				sql.append("ENTITY - $1 WHERE id=$2 AND ENTITY ? $1");
				tuple = Tuple.of(request.getAttribName(), request.getFirstId());
			} else if (request.getDatasetId() != null) {
				sql.append("NGSILD_DELETEATTRIB(ENTITY, $1, $2) WHERE id=$3 AND ENTITY @> '{\"$1\": [{\""
						+ NGSIConstants.NGSI_LD_DATA_SET_ID + "\": \"$2\"}]}'");
				tuple = Tuple.of(request.getAttribName(), request.getDatasetId(), request.getFirstId());
			} else {
				sql.append(
						"NGSILD_DELETEATTRIB(ENTITY, $1, null) WHERE id=$2 AND ENTITY ? $1 AND EXISTS (SELECT jsonb_array_elements FROM jsonb_array_elements(ENTITY->$1) WHERE NOT jsonb_array_elements ? '"
								+ NGSIConstants.NGSI_LD_DATA_SET_ID + "')");
				tuple = Tuple.of(request.getAttribName(), request.getFirstId());
			}
			sql.append(" RETURNING (SELECT ENTITY FROM old_entity) AS old_entity;");
			return client.preparedQuery(sql.toString()).execute(tuple).onFailure().retry().atMost(3).onItem()
					.transformToUni(rows -> {
						if (rows.size() == 0) {
							return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound));
						}
						Row first = rows.iterator().next();
						return Uni.createFrom().item(first.getJsonObject(0).getMap());
					});
		});
	}

	public Uni<RowSet<Row>> upsertEntity(CreateEntityRequest request) {
		return clientManager.getClient(request.getTenant(), true).onItem().transformToUni(client -> {
			String sql = "SELECT * FROM NGSILD_UPSERTENTITY($1::jsonb)";
			return client.preparedQuery(sql).execute(Tuple.of(new JsonObject(request.getFirstPayload()))).onFailure()
					.retry().atMost(3).onFailure().recoverWithUni(e -> Uni.createFrom().failure(e));
		});
	}

	@SuppressWarnings("unchecked")
	public Uni<Void> createEntity(CreateEntityRequest request) {
		return clientManager.getClient(request.getTenant(), true).onItem().transformToUni(client -> {
			String[] types = ((List<String>) request.getFirstPayload().get(NGSIConstants.JSON_LD_TYPE))
					.toArray(new String[0]);
			return client.preparedQuery("INSERT INTO ENTITY(ID,E_TYPES, ENTITY) VALUES ($1, $2, $3)")
					.execute(Tuple.of(request.getFirstId(), types, new JsonObject(request.getFirstPayload())))
					.onFailure().recoverWithUni(e -> {
						if (e instanceof PgException pge) {
							if (pge.getSqlState().equals(AppConstants.SQL_ALREADY_EXISTS)) {
								return Uni.createFrom().failure(new ResponseException(ErrorType.AlreadyExists,
										request.getFirstId() + " already exists"));
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
		return DBUtil.getAllRegistries(clientManager, ldService,
				"SELECT cs_id, c_id, e_id, e_id_p, e_type, e_prop, e_rel, ST_AsGeoJSON(i_location), scopes, EXTRACT(MILLISECONDS FROM expires), endpoint, tenant_id, headers, reg_mode, createEntity, updateEntity, appendAttrs, updateAttrs, deleteAttrs, deleteEntity, createBatch, upsertBatch, updateBatch, deleteBatch, upsertTemporal, appendAttrsTemporal, deleteAttrsTemporal, updateAttrsTemporal, deleteAttrInstanceTemporal, deleteTemporal, mergeEntity, replaceEntity, replaceAttrs, mergeBatch, retrieveEntity, queryEntity, queryBatch, retrieveTemporal, queryTemporal, retrieveEntityTypes, retrieveEntityTypeDetails, retrieveEntityTypeInfo, retrieveAttrTypes, retrieveAttrTypeDetails, retrieveAttrTypeInfo, createSubscription, updateSubscription, retrieveSubscription, querySubscription, deleteSubscription,queryEntityMap, createEntityMap, updateEntityMap, deleteEntityMap, retrieveEntityMap FROM csourceinformation WHERE (createEntity OR createBatch OR updateEntity OR appendAttrs OR deleteAttrs OR deleteEntity OR upsertBatch OR updateBatch OR deleteBatch) AND reg_mode != 0",
				logger);

	}

	@SuppressWarnings("unchecked")
	public Uni<Map<String, Object>> updateEntity(UpdateEntityRequest request) {
		return clientManager.getClient(request.getTenant(), false).onItem().transformToUni(client -> {
			Map<String, Object> payload = request.getFirstPayload();
			payload.remove(NGSIConstants.JSON_LD_ID);
			Object types = payload.remove(NGSIConstants.JSON_LD_TYPE);

			List<String> toBeRemoved = removeAtNoneEntries(payload);
			int dollar = 2;
			Tuple tuple = Tuple.tuple();

			StringBuilder sql = new StringBuilder("""
					WITH old_entity AS (
					    SELECT ENTITY
					    FROM ENTITY
					    WHERE ID = '%s'
					)""".formatted(request.getFirstId()));
			sql.append(
					"""
							,json_data AS (
							    SELECT jsonb_strip_nulls(jsonb_object_agg(
							               key,
							               CASE
							                   WHEN jsonb_typeof(value->0) = 'object' and (value->0)?'https://uri.etsi.org/ngsi-ld/createdAt' THEN
							                       jsonb_set(value, '{0,https://uri.etsi.org/ngsi-ld/createdAt}', old_entity.entity->key->0->'https://uri.etsi.org/ngsi-ld/createdAt', true)
							                   ELSE
							                       value
							               END
							           )) AS modified_data
							    FROM JSONB_EACH($1::jsonb)
							    CROSS JOIN old_entity
							)""");
			tuple.addJsonObject(new JsonObject(payload));
			sql.append(" UPDATE ENTITY SET ");
			if (types != null) {
				sql.append(
						"e_types = ARRAY(SELECT DISTINCT UNNEST(e_types || $2)), ENTITY = (jsonb_set(ENTITY, '{@type}', array_to_json(Array(SELECT DISTINCT UNNEST(e_types || $2))) ::jsonb) || ((select * from json_data)-'https://uri.etsi.org/ngsi-ld/createdAt')::jsonb)");
				dollar = 3;
				tuple.addArrayOfString(((List<String>) types).toArray(new String[0]));
			} else {
				sql.append(" ENTITY = (ENTITY || (select * from json_data)-'https://uri.etsi.org/ngsi-ld/createdAt') ");
			}
			if (!toBeRemoved.isEmpty()) {
				for (String remove : toBeRemoved) {
					sql.append(" - '$");
					sql.append(dollar);
					sql.append('\'');
					dollar++;
					tuple.addString(remove);
				}
			}
			sql.append("WHERE ID = $");
			sql.append(dollar);
			tuple.addString(request.getFirstId());
			sql.append(" RETURNING (SELECT ENTITY FROM old_entity) AS old_entity;");
			return client.preparedQuery(sql.toString()).execute(tuple).onFailure().recoverWithUni(e -> {
				return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound));
			}).onItem().transformToUni(rows -> {
				if (rows.rowCount() == 0) {
					return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound));
				}
				Row first = rows.iterator().next();
				return Uni.createFrom().item(first.getJsonObject(0).getMap());
			});
		});

	}

	private List<String> removeAtNoneEntries(Map<String, Object> payload) {
		Iterator<Entry<String, Object>> it = payload.entrySet().iterator();
		List<String> result = new ArrayList<>();
		while (it.hasNext()) {
			Entry<String, Object> entry = it.next();
			Object obj = entry.getValue();
			if (obj instanceof List<?> list) {

				Iterator<?> it2 = list.iterator();
				Object tmp;
				while (it2.hasNext()) {
					tmp = it2.next();
					if (tmp instanceof Map) {
						@SuppressWarnings("unchecked")
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
	@SuppressWarnings("unchecked")
	public Uni<Tuple3<Map<String, Object>, Map<String, Object>, Set<String>>> appendToEntity2(
			AppendEntityRequest request, boolean noOverwrite) {
		return clientManager.getClient(request.getTenant(), false).onItem().transformToUni(client -> {
			Map<String, Object> payload = request.getFirstPayload();
			payload.remove(NGSIConstants.JSON_LD_ID);
			Object types = payload.remove(NGSIConstants.JSON_LD_TYPE);

			Tuple tuple = Tuple.tuple();
			tuple.addString(request.getFirstId());
			StringBuilder sql = new StringBuilder(
					"WITH a as(SELECT entity FROM entity WHERE id =$1), b as (UPDATE ENTITY SET ");
			if (types != null) {
				sql.append("e_types = ARRAY(SELECT DISTINCT UNNEST(e_types || $2)), ");
				if (noOverwrite) {
					sql.append(
							"ENTITY = (($3::jsonb - 'https://uri.etsi.org/ngsi-ld/createdAt') || jsonb_set(ENTITY, '{@type}', array_to_json(Array(SELECT DISTINCT UNNEST(e_types || $2))) ::jsonb))");
				} else {
					sql.append(
							"ENTITY = (jsonb_set(ENTITY, '{@type}', array_to_json(Array(SELECT DISTINCT UNNEST(e_types || $2))) ::jsonb) || ($3::jsonb - 'https://uri.etsi.org/ngsi-ld/createdAt'))");
				}

				tuple.addArrayOfString(((List<String>) types).toArray(new String[0]));
				tuple.addJsonObject(new JsonObject(payload));
			} else {
				if (noOverwrite) {
					sql.append(" ENTITY = (($2::jsonb - 'https://uri.etsi.org/ngsi-ld/createdAt') || ENTITY) ");
				} else {
					sql.append(" ENTITY = (ENTITY || ($2::jsonb - 'https://uri.etsi.org/ngsi-ld/createdAt')) ");
				}
				tuple.addJsonObject(new JsonObject(payload));
			}

			sql.append("WHERE ID = $1");
			// if (noOverwrite) {
			sql.append(" RETURNING ENTITY) SELECT a.entity as old, b.entity as new FROM a, b;");
			// }

			return client.preparedQuery(sql.toString()).execute(tuple).onItem().transformToUni(rows -> {
				if (rows.size() == 0) {
					return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound));
				}
				Row first = rows.iterator().next();
				if (noOverwrite) {
					// TODO return the not added stuff from noOverwrite
					return Uni.createFrom().item(Tuple3.of(first.getJsonObject(0).getMap(),
							first.getJsonObject(1).getMap(), new HashSet<>(0)));
				} else {
					return Uni.createFrom().item(Tuple3.of(first.getJsonObject(0).getMap(),
							first.getJsonObject(1).getMap(), new HashSet<>(0)));
				}
			});
		});

	}

	public Uni<Map<String, Object>> deleteEntity(DeleteEntityRequest request) {
		return clientManager.getClient(request.getTenant(), false).onItem().transformToUni(client -> {
			return client.preparedQuery("DELETE FROM ENTITY WHERE id=$1 RETURNING ENTITY")
					.execute(Tuple.of(request.getFirstId())).onItem().transformToUni(rows -> {
						if (rows.rowCount() == 0) {
							return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound));
						}
						return Uni.createFrom().item(rows.iterator().next().getJsonObject(0).getMap());
					});
		});
	}

	public Uni<Map<String, Object>> mergePatch(MergePatchRequest request) {
		return clientManager.getClient(request.getTenant(), false).onItem().transformToUni(client -> {
			Map<String, Object> payload = request.getFirstPayload();
			payload.remove(NGSIConstants.NGSI_LD_CREATED_AT);
			if (payload.get(JsonLdConsts.TYPE) == null) {
				payload.remove(JsonLdConsts.TYPE);
			}
			String sql = "SELECT * FROM MERGE_JSON($1,$2);";
			Tuple tuple = Tuple.of(request.getFirstId(), new JsonObject(payload));
			return client.preparedQuery(sql).execute(tuple).onFailure().recoverWithUni(e -> {

				if (e instanceof PgException pge) {

					MicroServiceUtils.logPGE(pge, logger);
					if (pge.getSqlState().equals(AppConstants.SQL_NOT_FOUND)) {
						return Uni.createFrom().failure(
								new ResponseException(ErrorType.NotFound, request.getFirstId() + " not found"));
					}
					if (pge.getSqlState().startsWith("SB")) {
						return Uni.createFrom()
								.failure(new ResponseException(ErrorType.BadRequestData, pge.getErrorMessage()));
					}
				}
				logger.debug("database exception", e);
				return Uni.createFrom().failure(e);
			}).onItem().transformToUni(rows -> {
				if (rows.size() == 0)
					return Uni.createFrom()
							.failure(new ResponseException(ErrorType.NotFound, request.getFirstId() + " not found"));
				Row first = rows.iterator().next();
				JsonObject result = first.getJsonObject(0);
				if (result == null) {
					return Uni.createFrom()
							.failure(new ResponseException(ErrorType.NotFound, request.getFirstId() + " not found"));
				}
				return Uni.createFrom().item(result.getMap());
			});
		});
	}

	/**
	 * 
	 * @param request
	 * @return old version of the entity
	 */
	public Uni<Map<String, Object>> replaceEntity(ReplaceEntityRequest request) {
		@SuppressWarnings("unchecked")
		String[] types = ((List<String>) request.getFirstPayload().get(NGSIConstants.JSON_LD_TYPE))
				.toArray(new String[0]);
		return clientManager.getClient(request.getTenant(), false).onItem().transformToUni(client -> {
			return client
					.preparedQuery(
							"""
									WITH old_entity AS (
									SELECT ENTITY
									FROM ENTITY
									WHERE id = $3),
									json_data AS (
									 SELECT jsonb_strip_nulls(jsonb_object_agg(
									 key,
									 CASE WHEN jsonb_typeof(value->0) = 'object' and (value->0)?'https://uri.etsi.org/ngsi-ld/createdAt' THEN
									 jsonb_set(value, '{0,https://uri.etsi.org/ngsi-ld/createdAt}', old_entity.entity->key->0->'https://uri.etsi.org/ngsi-ld/createdAt', true)
									 ELSE value
									 END )) FROM JSONB_EACH($1::jsonb) CROSS JOIN old_entity )
									update entity set entity = (select * from json_data) || jsonb_build_object('https://uri.etsi.org/ngsi-ld/createdAt' , entity->'https://uri.etsi.org/ngsi-ld/createdAt') , e_types = $2 where id = $3
									RETURNING (SELECT ENTITY FROM old_entity) AS old_entity;""")
					.execute(Tuple.of(new JsonObject(request.getFirstPayload()), types, request.getFirstId())).onItem()
					.transformToUni(rows -> {
						if (rows.rowCount() == 0) {
							return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound));
						}
						Row first = rows.iterator().next();
						return Uni.createFrom().item(first.getJsonObject(0).getMap());
					});
		});
	}

	/**
	 * 
	 * @param request
	 * @return old version of the entity
	 */
	public Uni<Map<String, Object>> replaceAttrib(ReplaceAttribRequest request) {
		return clientManager.getClient(request.getTenant(), false).onItem().transformToUni(client -> {
			return client
					.preparedQuery(
							"""
									WITH old_entity AS (
									  SELECT ENTITY
									  FROM ENTITY
									  WHERE id = $2
									),
									json_data AS (
									SELECT jsonb_strip_nulls(jsonb_object_agg(
									key,
									CASE WHEN jsonb_typeof(value->0) = 'object' and (value->0)?'https://uri.etsi.org/ngsi-ld/createdAt' THEN
									jsonb_set(value, '{0,https://uri.etsi.org/ngsi-ld/createdAt}', old_entity.entity->key->0->'https://uri.etsi.org/ngsi-ld/createdAt', true)
									ELSE value
									END )) FROM JSONB_EACH($1::jsonb) CROSS JOIN old_entity )
									UPDATE entity
									SET entity = entity::jsonb || ((select * from json_data) - 'https://uri.etsi.org/ngsi-ld/createdAt')
									WHERE id = $2
									  AND ENTITY ? $3
									  AND (ENTITY-> $3 )::jsonb->$4 IS NULL
									RETURNING (SELECT ENTITY FROM old_entity) AS old_entity;
									""")
					.execute(Tuple.of(new JsonObject(request.getFirstPayload()), request.getFirstId(),
							request.getAttribName(), request.getDatasetId()))
					.onItem().transformToUni(rows -> {
						if (rows.rowCount() == 0) {
							return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound));
						}
						Row first = rows.iterator().next();
						return Uni.createFrom().item(first.getJsonObject(0).getMap());
					});
		});
	}

	public Uni<Table<String, String, List<RegistrationEntry>>> getAllQueryRegistries() {
		return DBUtil.getAllRegistries(clientManager, ldService,
				"SELECT cs_id, c_id, e_id, e_id_p, e_type, e_prop, e_rel, ST_AsGeoJSON(i_location), scopes, EXTRACT(MILLISECONDS FROM expires), endpoint, tenant_id, headers, reg_mode, createEntity, updateEntity, appendAttrs, updateAttrs, deleteAttrs, deleteEntity, createBatch, upsertBatch, updateBatch, deleteBatch, upsertTemporal, appendAttrsTemporal, deleteAttrsTemporal, updateAttrsTemporal, deleteAttrInstanceTemporal, deleteTemporal, mergeEntity, replaceEntity, replaceAttrs, mergeBatch, retrieveEntity, queryEntity, queryBatch, retrieveTemporal, queryTemporal, retrieveEntityTypes, retrieveEntityTypeDetails, retrieveEntityTypeInfo, retrieveAttrTypes, retrieveAttrTypeDetails, retrieveAttrTypeInfo, createSubscription, updateSubscription, retrieveSubscription, querySubscription, deleteSubscription, queryEntityMap, createEntityMap, updateEntityMap, deleteEntityMap, retrieveEntityMap FROM csourceinformation WHERE queryentity OR querybatch OR retrieveentity OR retrieveentitytypes OR retrieveentitytypedetails OR retrieveentitytypeinfo OR retrieveattrtypes OR retrieveattrtypedetails OR retrieveattrtypeinfo",
				logger);
	}

	public Uni<Void> updateValueField(String tenant, String id, String attribId, String datasetId,
			Map<String, Object> value) {
		return clientManager.getClient(tenant, false).onItem().transformToUni(client -> {
			Tuple t = Tuple.tuple();

			StringBuilder sql = new StringBuilder(
					"WITH JSON_DATA AS	(SELECT JSONB_ARRAY_ELEMENTS(ENTITY -> $1) WITH ORDINALITY AS ELEM FROM ENTITY WHERE ID=$2), ELEMENTS AS (SELECT ELEM.ELEM, ELEM.ORDINALITY - 1 AS INDEX FROM JSON_DATA ELEM WHERE ELEM.ELEM ->> '");
			sql.append(NGSIConstants.NGSI_LD_DATA_SET_ID);
			sql.append("' ");

			t.addString(attribId);
			t.addString(id);
			if (datasetId == null) {
				sql.append("IS NULL");
			} else {
				sql.append("= $3");
				t.addString(datasetId);
			}

			sql.append(") UPDATE ENTITY SET ENTITY=CASE WHEN entity#>>'{$1,@type,0}' = '");
			sql.append(NGSIConstants.NGSI_LD_PROPERTY);
			sql.append("' THEN JSONB_SET(ENTITY, ARRAY[$1,ELEMENTS.INDEX, '");
			sql.append(NGSIConstants.NGSI_LD_HAS_VALUE);
			sql.append("']::text[],$4,false) " + "WHEN entity#>>'{$1,@type,0}' = '");
			sql.append(NGSIConstants.NGSI_LD_RELATIONSHIP);
			sql.append("' THEN JSONB_SET(ENTITY, ARRAY[$1,ELEMENTS.INDEX, '");
			sql.append(NGSIConstants.NGSI_LD_HAS_OBJECT);
			sql.append("']::text[],$4,false)" + "WHEN entity#>>'{$1,@type,0}' = '");
			sql.append(NGSIConstants.NGSI_LD_LISTRELATIONSHIP);
			sql.append("' THEN JSONB_SET(ENTITY, ARRAY[$1,ELEMENTS.INDEX, '");
			sql.append(NGSIConstants.NGSI_LD_HAS_OBJECT_LIST);
			sql.append("']::text[],$4,false)");
			sql.append("WHEN entity#>>'{$1,@type,0}' = '");
			sql.append(NGSIConstants.NGSI_LD_ListProperty);
			sql.append("' THEN JSONB_SET(ENTITY, ARRAY[$1,ELEMENTS.INDEX, '");
			sql.append(NGSIConstants.NGSI_LD_HAS_LIST);
			sql.append("']::text[],$4,false)" + "WHEN entity#>>'{$1,@type,0}' = '");
			sql.append(NGSIConstants.NGSI_LD_LANGPROPERTY);
			sql.append("' THEN JSONB_SET(ENTITY, ARRAY[$1,ELEMENTS.INDEX, '");
			sql.append(NGSIConstants.NGSI_LD_HAS_LANGUAGE_MAP);
			sql.append("']::text[],$4,false)" + "WHEN entity#>>'{$1,@type,0}' = '");
			sql.append(NGSIConstants.NGSI_LD_VocabProperty);
			sql.append("' THEN JSONB_SET(ENTITY, ARRAY[$1,ELEMENTS.INDEX, '");
			sql.append(NGSIConstants.NGSI_LD_HAS_VOCAB);
			sql.append("']::text[],$4,false)" + "WHEN entity#>>'{$1,@type,0}' = '");
			sql.append(NGSIConstants.NGSI_LD_JSON_PROPERTY);
			sql.append("' THEN JSONB_SET(ENTITY, ARRAY[$1,ELEMENTS.INDEX, '");
			sql.append(NGSIConstants.NGSI_LD_HAS_JSON);
			sql.append("']::text[],$4,false)" + "WHEN entity#>>'{$1,@type,0}' = '");
			sql.append(NGSIConstants.NGSI_LD_GEOPROPERTY);
			sql.append("' THEN JSONB_SET(ENTITY, ARRAY[$1,ELEMENTS.INDEX, '");
			sql.append(NGSIConstants.NGSI_LD_HAS_VALUE);
			sql.append("']::text[],$4,false)" + "ELSE ENTITY end FROM ELEMENTS WHERE ENTITY.ID=$2");
			t.addJsonObject(new JsonObject(value));
			return client.preparedQuery(sql.toString()).execute(t).onItem().transformToUni(result -> {

				return Uni.createFrom().voidItem();
			});

		});
	}

	public Uni<Map<String, Object>> mergeBatchEntity(BatchRequest request) {
		return clientManager.getClient(request.getTenant(), true).onItem().transformToUni(client -> {
			List<Map<String, Object>> entities = Lists.newArrayList();
			request.getPayload().values().forEach(entityList -> {
				entities.addAll(entityList);
			});
			Tuple tuple = Tuple.of(new JsonArray(entities));
			return client.preparedQuery("SELECT * FROM MERGE_JSON_BATCH($1)").execute(tuple).onItem()
					.transform(rows -> {
						return rows.iterator().next().getJsonObject(0).getMap();
					});
		});
	}

}
