package eu.neclab.ngsildbroker.entityhandler.services;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import eu.neclab.ngsildbroker.commons.storage.ConnectionManager;
import eu.neclab.ngsildbroker.commons.tools.DBUtil;
import eu.neclab.ngsildbroker.commons.tools.EntityTools;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import io.quarkus.logging.Log;
import io.quarkus.runtime.Startup;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.mutiny.tuples.Tuple3;
import io.smallrye.mutiny.unchecked.Unchecked;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.Tuple;
import io.vertx.pgclient.PgException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
@Startup
public class EntityInfoDAO {

	private static Logger logger = LoggerFactory.getLogger(EntityInfoDAO.class);

	@Inject
	ConnectionManager connectionManager;

	@Inject
	JsonLDService ldService;

	public Uni<Map<String, Object>> batchCreateEntity(BatchRequest request) {

		List<Map<String, Object>> entities = Lists.newArrayList();
		request.getPayload().values().forEach(entityList -> {
			entities.addAll(entityList);
		});
		Tuple3<Boolean, List<Tuple>, List<String>> nullFoundAndTuple = EntityTools
				.removeNGSILDNullToTuplesWithIdSet(entities, true);

		return connectionManager.executeBatchQuery(request.getTenant(),
				"INSERT INTO ENTITY (id, e_types, entity) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING RETURNING id, true;",
				nullFoundAndTuple.getItem2(), true).onItem().transform(rows -> {
					List<String> ids = nullFoundAndTuple.getItem3();
					Map<String, Object> result = new HashMap<>(2);
					ArrayList<String> success = new ArrayList<>();
					ArrayList<Map<String, String>> failure = new ArrayList<>();
					result.put("success", success);
					result.put("failure", failure);
					while (rows != null) {
						rows.forEach(row -> {
							String id = row.getString(0);
							ids.remove(id);
							success.add(id);
						});
						rows = rows.next();
					}
					ids.forEach(id -> {
						failure.add(Map.of(id, AppConstants.SQL_ALREADY_EXISTS));
					});
					return result;
				}).onFailure().recoverWithUni(e -> {
					if (e instanceof PgException pge) {
						logger.error(pge.getDetail());
					}
					logger.error("Failed to store entities in batch create.", e);
					return Uni.createFrom().failure(e);
				});
	}

	public Uni<Map<String, Object>> batchUpsertEntity(BatchRequest request, boolean doReplace) {

		List<Map<String, Object>> entities = Lists.newArrayList();
		request.getPayload().values().forEach(entityList -> {
			entities.add(mergeAllEntities(entityList));
		});
		if (entities.isEmpty()) {
			Map<String, Object> result = new HashMap<>(2);
			result.put("success", new ArrayList<Map<String, Object>>(0));
			result.put("failure", new ArrayList<Map<String, Object>>(0));
			return Uni.createFrom().item(result);
		}
		Tuple2<Boolean, List<Tuple>> nullFoundAndTuple = EntityTools.removeNGSILDNullToTuples(entities, doReplace);
		StringBuilder sql = new StringBuilder(
				"""
						with a as (SELECT ID AS ID, ENTITY AS OLD_ENTITY FROM ENTITY WHERE ID = $1),
						b as(INSERT INTO ENTITY(ID, E_TYPES, ENTITY) VALUES ($1, $2, $3::jsonb) ON CONFLICT (id) DO UPDATE SET e_types = ARRAY(SELECT DISTINCT UNNEST(entity.e_types || EXCLUDED.e_types)),entity =
								""");

		if (doReplace) {
			sql.append("EXCLUDED.entity ");
		} else {
			if (nullFoundAndTuple.getItem1()) {
				sql.append("ngsild_update_entity(entity.entity, $4, true) ");
			} else {
				sql.append("ngsild_update_entity(entity.entity, excluded.entity, true) ");
			}
		}

		sql.append(
				"RETURNING id, entity, (xmax = 0) AS inserted) select b.id, b.inserted, b.entity, a.old_entity from b LEFT JOIN a ON b.id = a.id;");
		// logger.debug(sql.toString());

		return connectionManager
				.executeBatchQuery(request.getTenant(), sql.toString(), nullFoundAndTuple.getItem2(), true).onItem()
				.transform(rows -> {
					Map<String, Object> result = new HashMap<>(2);
					ArrayList<Map<String, Object>> success = new ArrayList<>(rows.size());
					result.put("success", success);
					result.put("failure", new ArrayList<Map<String, Object>>(0));
					while (rows != null) {
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
						rows = rows.next();
					}
					return result;
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

	// public Uni<Map<String, Object>> batchAppendEntity(BatchRequest request) {
	// return clientManager.getClient(request.getTenant(),
	// true).onItem().transformToUni(client -> {
	// List<Tuple> entities = Lists.newArrayList();
	// request.getPayload().values().forEach(entityList -> {
	// entityList.forEach(entity -> entities.add(Tuple.of(entity)));
	// });
	//
	//
	// return client.preparedQuery(
	// "UPDATE ENTITY (id, e_types, entity) VALUES ($1, $2, $3) ON CONFLICT DO
	// NOTHING RETURNING id, true;")
	// .executeBatch(nullFoundAndTuple.getItem2()).onItem().transform(rows -> {
	// Set<String> ids = nullFoundAndTuple.getItem3();
	// Map<String, Object> result = new HashMap<>(2);
	// ArrayList<String> success = new ArrayList<>();
	// ArrayList<Map<String, String>> failure = new ArrayList<>();
	// result.put("success", success);
	// result.put("failure", failure);
	// while (rows != null) {
	// rows.forEach(row -> {
	// String id = row.getString(0);
	// ids.remove(id);
	// success.add(id);
	// });
	// rows = rows.next();
	// }
	// ids.forEach(id -> {
	// failure.add(Map.of(id, AppConstants.SQL_ALREADY_EXISTS));
	// });
	// return result;
	// }).onFailure().recoverWithUni(e -> {
	// if (e instanceof PgException pge) {
	// logger.error(pge.getDetail());
	// }
	// logger.error("Failed to store entities in batch create.", e);
	// return Uni.createFrom().failure(e);
	// });
	// });
	// }

	public Uni<Map<String, Object>> batchAppendEntity(BatchRequest request) {

		List<Map<String, Object>> entities = Lists.newArrayList();
		request.getPayload().values().forEach(entityList -> {
			entities.addAll(entityList);
		});
		Tuple tuple = Tuple.of(new JsonArray(entities), request.isNoOverwrite());
		return connectionManager
				.executeQuery(request.getTenant(), "SELECT * FROM NGSILD_APPENDBATCH($1, $2)", tuple, false).onItem()
				.transform(rows -> {
					return rows.iterator().next().getJsonObject(0).getMap();
				});

	}

	public Uni<Map<String, Object>> batchDeleteEntity(String tenant, List<String> entityIds) {

		return connectionManager.executeQuery(tenant, "SELECT * FROM NGSILD_DELETEBATCH($1)",
				Tuple.of(new JsonArray(entityIds)), true).onItem().transform(rows -> {
					return rows.iterator().next().getJsonObject(0).getMap();
				});

	}

	@SuppressWarnings("unchecked")
	/**
	 * 
	 * @param request
	 * @return old version of the entity
	 */
	public Uni<Map<String, Object>> partialUpdateAttribute(UpdateEntityRequest request) {

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
		return connectionManager.executeQuery(request.getTenant(), sql, tuple, false).onItem().transformToUni(rows -> {
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

	}

	/**
	 * 
	 * @param request
	 * @return old version of the entity
	 */
	public Uni<Map<String, Object>> deleteAttribute(DeleteAttributeRequest request) {

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
			sql.append("NGSILD_DELETEATTRIB(ENTITY, $1, $3) WHERE id=$2 AND ENTITY @> '{\"$1\": [{\""
					+ NGSIConstants.NGSI_LD_DATA_SET_ID + "\": [{\"@id\":\"$3\"}]}]}'");
			tuple = Tuple.of(request.getAttribName(), request.getFirstId(), request.getDatasetId());
		} else {
			sql.append(
					"NGSILD_DELETEATTRIB(ENTITY, $1, null) WHERE id=$2 AND ENTITY ? $1 AND EXISTS (SELECT jsonb_array_elements FROM jsonb_array_elements(ENTITY->$1) WHERE NOT jsonb_array_elements ? '"
							+ NGSIConstants.NGSI_LD_DATA_SET_ID + "')");
			tuple = Tuple.of(request.getAttribName(), request.getFirstId());
		}
		sql.append(" RETURNING (SELECT ENTITY FROM old_entity) AS old_entity;");
		Log.debug(sql.toString());
		Log.debug(tuple.deepToString());
		return connectionManager.executeQuery(request.getTenant(), sql.toString(), tuple, false).onItem()
				.transformToUni(rows -> {
					if (rows.size() == 0) {
						if (request.getDatasetId() == null) {
							return Uni.createFrom().failure(
									new ResponseException(ErrorType.NotFound, "Attribute " + request.getAttribName()
											+ " on Entity " + request.getFirstId() + " was not found."));
						} else {
							return Uni.createFrom()
									.failure(new ResponseException(ErrorType.NotFound,
											"Attribute " + request.getAttribName() + " with datasetId "
													+ request.getDatasetId() + " on Entity " + request.getFirstId()
													+ " was not found."));
						}
					}
					Row first = rows.iterator().next();
					return Uni.createFrom().item(first.getJsonObject(0).getMap());
				});
	}

	public Uni<RowSet<Row>> upsertEntity(CreateEntityRequest request) {

		String sql = "SELECT * FROM NGSILD_UPSERTENTITY($1::jsonb)";
		return connectionManager
				.executeQuery(request.getTenant(), sql, Tuple.of(new JsonObject(request.getFirstPayload())), true)
				.onFailure().recoverWithUni(e -> Uni.createFrom().failure(e));

	}

	@SuppressWarnings("unchecked")
	public Uni<Void> createEntity(CreateEntityRequest request) {

		String[] types = ((List<String>) request.getFirstPayload().get(NGSIConstants.JSON_LD_TYPE))
				.toArray(new String[0]);
		return connectionManager
				.executeQuery(request.getTenant(), "INSERT INTO ENTITY(ID,E_TYPES, ENTITY) VALUES ($1, $2, $3)",
						Tuple.of(request.getFirstId(), types, new JsonObject(request.getFirstPayload())), true)
				.onFailure().recoverWithUni(e -> {
					if (e instanceof PgException pge) {
						if (pge.getSqlState().equals(AppConstants.SQL_ALREADY_EXISTS)) {
							return Uni.createFrom().failure(new ResponseException(ErrorType.AlreadyExists,
									request.getFirstId() + " already exists"));
						}
					}
					return Uni.createFrom().failure(e);
				}).onItem().transformToUni(v -> Uni.createFrom().voidItem());
	}

	public Uni<Tuple2<String, String>> getEndpoint(String entityId, String tenantId) {
		String query = "SELECT endpoint, csource_alias FROM csource, csourceinformation csi WHERE csource.id=csi.id AND csi.e_id='"
				+ entityId + "'";

		return connectionManager.executeQuery(tenantId, query, null, false).onItem().transform((rowSet) -> {
			if (rowSet.rowCount() == 0) {
				return null;
			}
			Row row = rowSet.iterator().next();
			return Tuple2.of(row.getString(0), row.getString(1));
		}).onFailure().recoverWithUni(Uni.createFrom().item(null));
	}

	public Uni<Table<String, String, List<RegistrationEntry>>> getAllRegistries() {
		return DBUtil.getAllRegistries(connectionManager, ldService,
				"SELECT cs_id, c_id, e_id, e_id_p, e_type, e_prop, e_rel, ST_AsGeoJSON(i_location), scopes, EXTRACT(MILLISECONDS FROM expires), endpoint, tenant_id, headers, reg_mode, createEntity, updateEntity, appendAttrs, updateAttrs, deleteAttrs, deleteEntity, createBatch, upsertBatch, updateBatch, deleteBatch, upsertTemporal, appendAttrsTemporal, deleteAttrsTemporal, updateAttrsTemporal, deleteAttrInstanceTemporal, deleteTemporal, mergeEntity, replaceEntity, replaceAttrs, mergeBatch, retrieveEntity, queryEntity, queryBatch, retrieveTemporal, queryTemporal, retrieveEntityTypes, retrieveEntityTypeDetails, retrieveEntityTypeInfo, retrieveAttrTypes, retrieveAttrTypeDetails, retrieveAttrTypeInfo, createSubscription, updateSubscription, retrieveSubscription, querySubscription, deleteSubscription,queryEntityMap, createEntityMap, updateEntityMap, deleteEntityMap, retrieveEntityMap, csource_Alias FROM csourceinformation WHERE (createEntity OR createBatch OR updateEntity OR appendAttrs OR deleteAttrs OR deleteEntity OR upsertBatch OR updateBatch OR deleteBatch) AND reg_mode != 0",
				logger);

	}

	public Uni<Map<String, Object>> updateEntity(UpdateEntityRequest request) {

		String sql = """
				WITH a AS (
				    SELECT ENTITY
				    FROM ENTITY
				    WHERE ID = $1
				)
				UPDATE ENTITY SET entity = ngsild_update_entity(entity, $2, $3) WHERE ID = $1 RETURNING (SELECT ENTITY FROM a) AS old_entity, ENTITY.entity as new_entity;
				""";

		Tuple tuple = Tuple.of(request.getFirstId(), new JsonObject(request.getFirstPayload()),
				!request.isNoOverwrite());
		// logger.debug(sql);
		// logger.debug(tuple.deepToString());
		return connectionManager.executeQuery(request.getTenant(), sql, tuple, false).onFailure().recoverWithUni(e -> {
			e.printStackTrace();
			return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound));
		}).onItem().transformToUni(rows -> {
			if (rows.rowCount() == 0) {
				return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound));
			}
			Row first = rows.iterator().next();
			return Uni.createFrom().item(first.getJsonObject(0).getMap());
		});

	}

	/**
	 * 
	 * @param request
	 * @param noOverwrite
	 * @return the not added attribs
	 */
	public Uni<Tuple3<Map<String, Object>, Map<String, Object>, Set<String>>> appendToEntity2(
			AppendEntityRequest request, boolean noOverwrite) {

		String sql = """
				WITH a AS (
				    SELECT ENTITY
				    FROM ENTITY
				    WHERE ID = $1
				)
				UPDATE ENTITY SET entity = ngsild_update_entity(entity, $2, $3) WHERE ID = $1 RETURNING (SELECT ENTITY FROM a) AS old_entity, ENTITY.entity as new_entity;
				""";

		Tuple tuple = Tuple.of(request.getFirstId(), new JsonObject(request.getFirstPayload()), !noOverwrite);
		// logger.debug(sql);
		// logger.debug(tuple.deepToString());
		return connectionManager.executeQuery(request.getTenant(), sql, tuple, false).onFailure().recoverWithUni(e -> {
			return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound));
		}).onItem().transformToUni(rows -> {
			if (rows.rowCount() == 0) {
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

	}

	public Uni<Map<String, Object>> deleteEntity(DeleteEntityRequest request) {

		return connectionManager.executeQuery(request.getTenant(), "DELETE FROM ENTITY WHERE id=$1 RETURNING ENTITY",
				Tuple.of(request.getFirstId()), false).onItem().transformToUni(rows -> {
					if (rows.rowCount() == 0) {
						return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound));
					}
					return Uni.createFrom().item(rows.iterator().next().getJsonObject(0).getMap());
				});

	}

	public Uni<Map<String, Object>> mergePatch(MergePatchRequest request) {

		Map<String, Object> payload = request.getFirstPayload();
		payload.remove(NGSIConstants.NGSI_LD_CREATED_AT);
		if (payload.get(JsonLdConsts.TYPE) == null) {
			payload.remove(JsonLdConsts.TYPE);
		}
		String sql = "SELECT * FROM MERGE_JSON($1,$2);";
		Tuple tuple = Tuple.of(request.getFirstId(), new JsonObject(payload));
		return connectionManager.executeQuery(request.getTenant(), sql, tuple, false).onFailure().recoverWithUni(e -> {

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
		String sql = """
				WITH old_entity AS (
				SELECT ENTITY, ENTITY -> 'https://uri.etsi.org/ngsi-ld/createdAt' as createdAt, ENTITY -> 'https://uri.etsi.org/ngsi-ld/modifiedAt' as modifiedAt
				FROM ENTITY
				WHERE id = $1)
				UPDATE ENTITY SET ENTITY = jsonb_set($2,'{https://uri.etsi.org/ngsi-ld/createdAt}', olde.createdAt), E_TYPES = $3 FROM (SELECT * FROM old_entity) as olde WHERE id = $1
				RETURNING (SELECT ENTITY FROM old_entity) AS old_entity;""";
		Tuple tuple = Tuple.of(request.getFirstId(), new JsonObject(request.getFirstPayload()), types);

		return connectionManager.executeQuery(request.getTenant(), sql, tuple, false).onItem()
				.transformToUni(rows -> {
					if (rows.rowCount() == 0) {
						return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound));
					}
					Row first = rows.iterator().next();
					return Uni.createFrom().item(first.getJsonObject(0).getMap());
				});
	}

	/**
	 * 
	 * @param request
	 * @return old version of the entity
	 */
	public Uni<Map<String, Object>> replaceAttrib(ReplaceAttribRequest request) {
		String sql = """
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
				""";
		Tuple tuple = Tuple.of(new JsonObject(request.getFirstPayload()), request.getFirstId(),
				request.getAttribName(), request.getDatasetId());
		return connectionManager.executeQuery(request.getTenant(), sql, tuple, false)
				.onItem().transformToUni(rows -> {
					if (rows.rowCount() == 0) {
						return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound));
					}
					Row first = rows.iterator().next();
					return Uni.createFrom().item(first.getJsonObject(0).getMap());
				});

	}

	public Uni<Table<String, String, List<RegistrationEntry>>> getAllQueryRegistries() {
		return DBUtil.getAllRegistries(connectionManager, ldService,
				"SELECT cs_id, c_id, e_id, e_id_p, e_type, e_prop, e_rel, ST_AsGeoJSON(i_location), scopes, EXTRACT(MILLISECONDS FROM expires), endpoint, tenant_id, headers, reg_mode, createEntity, updateEntity, appendAttrs, updateAttrs, deleteAttrs, deleteEntity, createBatch, upsertBatch, updateBatch, deleteBatch, upsertTemporal, appendAttrsTemporal, deleteAttrsTemporal, updateAttrsTemporal, deleteAttrInstanceTemporal, deleteTemporal, mergeEntity, replaceEntity, replaceAttrs, mergeBatch, retrieveEntity, queryEntity, queryBatch, retrieveTemporal, queryTemporal, retrieveEntityTypes, retrieveEntityTypeDetails, retrieveEntityTypeInfo, retrieveAttrTypes, retrieveAttrTypeDetails, retrieveAttrTypeInfo, createSubscription, updateSubscription, retrieveSubscription, querySubscription, deleteSubscription, queryEntityMap, createEntityMap, updateEntityMap, deleteEntityMap, retrieveEntityMap, csource_Alias FROM csourceinformation WHERE queryentity OR querybatch OR retrieveentity OR retrieveentitytypes OR retrieveentitytypedetails OR retrieveentitytypeinfo OR retrieveattrtypes OR retrieveattrtypedetails OR retrieveattrtypeinfo",
				logger);
	}

	public Uni<Void> updateValueField(String tenant, String id, String attribId, String datasetId,
			Map<String, Object> value) {
		Tuple t = Tuple.tuple();

		StringBuilder sql = new StringBuilder(
				"WITH JSON_DATA AS(SELECT VALUE, ORDINALITY FROM ENTITY, JSONB_ARRAY_ELEMENTS(ENTITY -> $1) WITH ORDINALITY WHERE ID=$2), "
						+ "ELEMENTS AS (SELECT VALUE, ORDINALITY - 1 AS INDEX FROM JSON_DATA WHERE VALUE ->> '");
		sql.append(NGSIConstants.NGSI_LD_DATA_SET_ID);
		sql.append("' ");

		t.addString(attribId);
		t.addString(id);
		int dollar;
		if (datasetId == null) {
			sql.append("IS NULL");
			dollar = 3;
		} else {
			sql.append("= $3");
			dollar = 4;
			t.addString(datasetId);
		}

		sql.append(") UPDATE ENTITY SET ENTITY=CASE WHEN entity#>>'{$1,@type,0}' = '");
		sql.append(NGSIConstants.NGSI_LD_PROPERTY);
		sql.append("' THEN JSONB_SET(ENTITY, ARRAY[$1,ELEMENTS.INDEX, '");
		sql.append(NGSIConstants.NGSI_LD_HAS_VALUE);
		sql.append("']::text[],$");
		sql.append(dollar);
		sql.append(",false) " + "WHEN entity#>>'{$1,@type,0}' = '");
		sql.append(NGSIConstants.NGSI_LD_RELATIONSHIP);
		sql.append("' THEN JSONB_SET(ENTITY, ARRAY[$1,ELEMENTS.INDEX, '");
		sql.append(NGSIConstants.NGSI_LD_HAS_OBJECT);
		sql.append("']::text[],$");
		sql.append(dollar);
		sql.append(",false)" + "WHEN entity#>>'{$1,$");
		sql.append(dollar);
		sql.append("ype,0}' = '");
		sql.append(NGSIConstants.NGSI_LD_LISTRELATIONSHIP);
		sql.append("' THEN JSONB_SET(ENTITY, ARRAY[$1,ELEMENTS.INDEX, '");
		sql.append(NGSIConstants.NGSI_LD_HAS_OBJECT_LIST);
		sql.append("']::text[],$");
		sql.append(dollar);
		sql.append(",false)");
		sql.append("WHEN entity#>>'{$1,@type,0}' = '");
		sql.append(NGSIConstants.NGSI_LD_LIST_PROPERTY);
		sql.append("' THEN JSONB_SET(ENTITY, ARRAY[$1,ELEMENTS.INDEX, '");
		sql.append(NGSIConstants.NGSI_LD_HAS_LIST);
		sql.append("']::text[],$");
		sql.append(dollar);
		sql.append(",false)" + "WHEN entity#>>'{$1,@type,0}' = '");
		sql.append(NGSIConstants.NGSI_LD_LANGPROPERTY);
		sql.append("' THEN JSONB_SET(ENTITY, ARRAY[$1,ELEMENTS.INDEX, '");
		sql.append(NGSIConstants.NGSI_LD_HAS_LANGUAGE_MAP);
		sql.append("']::text[],$");
		sql.append(dollar);
		sql.append(",false)" + "WHEN entity#>>'{$1,@type,0}' = '");
		sql.append(NGSIConstants.NGSI_LD_VOCAB_PROPERTY);
		sql.append("' THEN JSONB_SET(ENTITY, ARRAY[$1,ELEMENTS.INDEX, '");
		sql.append(NGSIConstants.NGSI_LD_HAS_VOCAB);
		sql.append("']::text[],$");
		sql.append(dollar);
		sql.append(",false)" + "WHEN entity#>>'{$1,@type,0}' = '");
		sql.append(NGSIConstants.NGSI_LD_JSON_PROPERTY);
		sql.append("' THEN JSONB_SET(ENTITY, ARRAY[$1,ELEMENTS.INDEX, '");
		sql.append(NGSIConstants.NGSI_LD_HAS_JSON);
		sql.append("']::text[],$");
		sql.append(dollar);
		sql.append(",false)" + "WHEN entity#>>'{$1,$");
		sql.append(dollar);
		sql.append(",@type,0}' = '");
		sql.append(NGSIConstants.NGSI_LD_GEOPROPERTY);
		sql.append("' THEN JSONB_SET(ENTITY, ARRAY[$1,ELEMENTS.INDEX, '");
		sql.append(NGSIConstants.NGSI_LD_HAS_VALUE);
		sql.append("']::text[],$");
		sql.append(dollar);
		sql.append(",false)" + "ELSE ENTITY end FROM ELEMENTS WHERE ENTITY.ID=$2");
		t.addJsonObject(new JsonObject(value));

		return connectionManager.executeQuery(tenant, sql.toString(), t, false).onItem().transformToUni(result -> {

			return Uni.createFrom().voidItem();
		});

	}

	public Uni<Map<String, Object>> mergeBatchEntity(BatchRequest request) {
		List<Map<String, Object>> entities = Lists.newArrayList();
		request.getPayload().values().forEach(entityList -> {
			entities.addAll(entityList);
		});
		Tuple tuple = Tuple.of(new JsonArray(entities));
		return connectionManager.executeQuery(request.getTenant(), "SELECT * FROM MERGE_JSON_BATCH($1)", tuple, true)
				.onItem().transform(rows -> rows.iterator().next().getJsonObject(0).getMap());
	}

	/**
	 * Bulk-stores expanded entities via batched JDBC upsert. Drops non-essential
	 * indexes for the duration of the load, then rebuilds them before commit.
	 */
	public Uni<Integer> bulkCopyInsert(String tenant, List<Map<String, Object>> expandedEntities) {
		if (expandedEntities.isEmpty()) {
			return Uni.createFrom().item(0);
		}
		return Uni.createFrom().item(Unchecked.supplier(() -> {
			DataSource ds = connectionManager.getWriteDataSource(tenant);
			Connection conn = ds.getConnection();
			try {
				conn.setAutoCommit(false);

				try (Statement st = conn.createStatement()) {
					st.execute("SET LOCAL session_replication_role = replica");
					st.execute("SET LOCAL synchronous_commit = off");
					st.execute("SET LOCAL maintenance_work_mem = '2GB'");

					st.execute("DROP INDEX IF EXISTS public.I_entity_scopes");
					st.execute("DROP INDEX IF EXISTS public.I_entity_types");
					st.execute("DROP INDEX IF EXISTS public.i_entity_createdat");
					st.execute("DROP INDEX IF EXISTS public.i_entity_data");
					st.execute("DROP INDEX IF EXISTS public.i_entity_location");
					st.execute("DROP INDEX IF EXISTS public.i_entity_modifiedat");
				}

				final String sql = """
						INSERT INTO ENTITY (id, e_types, entity, createdat, modifiedat, location, scopes)
						VALUES (
						    ?,
						    ?,
						    ?::jsonb,
						    ?::timestamp,
						    ?::timestamp,
						    CASE WHEN ?::text IS NULL THEN NULL
						         ELSE ST_SetSRID(ST_GeomFromGeoJSON(?::text), 4326) END,
						    ?
						)
						ON CONFLICT (id) DO UPDATE
						  SET e_types    = EXCLUDED.e_types,
						      entity     = ngsild_update_entity(entity.entity, excluded.entity, true),
						      createdat  = EXCLUDED.createdat,
						      modifiedat = EXCLUDED.modifiedat,
						      location   = EXCLUDED.location,
						      scopes     = EXCLUDED.scopes
						""";

				long t1 = System.nanoTime();
				int affected = 0;

				try (PreparedStatement ps = conn.prepareStatement(sql)) {
					final int batchSize = 1_000_000;
					int inBatch = 0;

					for (Map<String, Object> e : expandedEntities) {
						JsonObject json = new JsonObject(e);

						String id = extractId(json);
						java.sql.Array types = conn.createArrayOf("text", extractTypesArray(json));
						String entityJson = json.encode();
						String createdAt = extractValueAt(json, NGSIConstants.NGSI_LD_CREATED_AT);
						String modifiedAt = extractValueAt(json, NGSIConstants.NGSI_LD_MODIFIED_AT);
						String geoJson = extractGeoJson(json);
						Object[] scopeArr = extractScopesArray(json);
						java.sql.Array scopes = scopeArr == null ? null : conn.createArrayOf("text", scopeArr);

						ps.setString(1, id);
						ps.setArray(2, types);
						ps.setString(3, entityJson);
						ps.setString(4, createdAt);
						ps.setString(5, modifiedAt);
						ps.setString(6, geoJson);
						ps.setString(7, geoJson);
						if (scopes == null) {
							ps.setNull(8, java.sql.Types.ARRAY);
						} else {
							ps.setArray(8, scopes);
						}

						ps.addBatch();
						if (++inBatch == batchSize) {
							for (int c : ps.executeBatch()) {
								affected += (c > 0 ? c : 0);
							}
							inBatch = 0;
						}
					}
					if (inBatch > 0) {
						for (int c : ps.executeBatch()) {
							affected += (c > 0 ? c : 0);
						}
					}
				}

				long t2 = System.nanoTime();

				try (Statement st = conn.createStatement()) {
					st.execute("CREATE INDEX i_entity_createdat  ON public.entity USING btree (createdat)");
					st.execute("CREATE INDEX i_entity_modifiedat ON public.entity USING btree (modifiedat)");
					st.execute("CREATE INDEX I_entity_scopes ON public.entity USING gin  (scopes)");
					st.execute("CREATE INDEX I_entity_types  ON public.entity USING gin  (e_types)");
					st.execute("CREATE INDEX i_entity_location   ON public.entity USING gist (location)");
					st.execute("CREATE INDEX i_entity_data       ON public.entity USING gin  (entity)");
				}
				long t3 = System.nanoTime();

				logger.info("insert={}ms rebuild={}ms", (t2 - t1) / 1_000_000, (t3 - t2) / 1_000_000);

				conn.commit();

				try (Statement st = conn.createStatement()) {
					st.execute("ANALYZE public.entity");
				}
				return affected;
			} catch (Exception e) {
				try {
					conn.rollback();
				} catch (Exception rb) {
					logger.error("Failed to roll back bulk insert transaction", rb);
				}
				logger.error("Failed to bulk insert entities", e);
				throw e;
			} finally {
				try {
					conn.close();
				} catch (Exception ce) {
					logger.error("Failed to close bulk insert connection", ce);
				}
			}
		})).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
	}

	private String extractId(JsonObject json) {
		Object v = json.getValue(NGSIConstants.JSON_LD_ID);
		return v == null ? null : v.toString();
	}

	private String[] extractTypesArray(JsonObject json) {
		Object t = json.getValue(NGSIConstants.JSON_LD_TYPE);
		if (t instanceof JsonArray arr) {
			String[] out = new String[arr.size()];
			for (int i = 0; i < arr.size(); i++) {
				out[i] = arr.getValue(i).toString();
			}
			return out;
		}
		return new String[0];
	}

	private String extractValueAt(JsonObject json, String key) {
		Object arr = json.getValue(key);
		if (arr instanceof JsonArray ja && !ja.isEmpty() && ja.getValue(0) instanceof JsonObject fo) {
			Object val = fo.getValue(NGSIConstants.JSON_LD_VALUE);
			return val == null ? null : val.toString();
		}
		return null;
	}

	private Object[] extractScopesArray(JsonObject json) {
		Object sv = json.getValue(NGSIConstants.NGSI_LD_SCOPE);
		if (!(sv instanceof JsonArray arr)) {
			return null;
		}
		Object[] out = new Object[arr.size()];
		for (int i = 0; i < arr.size(); i++) {
			Object o = arr.getValue(i);
			out[i] = (o instanceof JsonObject elem) ? elem.getValue(NGSIConstants.JSON_LD_VALUE) : null;
		}
		return out;
	}

	private String extractGeoJson(JsonObject json) {
		Object loc = json.getValue(NGSIConstants.NGSI_LD_LOCATION);
		if (!(loc instanceof JsonArray locArr) || locArr.isEmpty()) {
			return null;
		}
		if (!(locArr.getValue(0) instanceof JsonObject locObj)) {
			return null;
		}

		Object locType = locObj.getValue(NGSIConstants.JSON_LD_TYPE);
		boolean isGeo = (locType instanceof JsonArray lta)
				&& lta.stream().anyMatch(x -> NGSIConstants.NGSI_LD_GEOPROPERTY.equals(x));
		if (!isGeo) {
			return null;
		}

		Object hv = locObj.getValue(NGSIConstants.NGSI_LD_HAS_VALUE);
		if (!(hv instanceof JsonArray hvArr) || hvArr.isEmpty()) {
			return null;
		}
		if (!(hvArr.getValue(0) instanceof JsonObject node)) {
			return null;
		}

		Object ntObj = node.getValue(NGSIConstants.JSON_LD_TYPE);
		if (!(ntObj instanceof JsonArray nta) || nta.isEmpty()) {
			return null;
		}
		Object t0 = nta.getValue(0);
		if (t0 == null) {
			return null;
		}
		String fullType = t0.toString();
		String geoType = fullType.length() >= 31 ? fullType.substring(31) : "";

		Object coordsWrap = node.getValue(NGSIConstants.NGSI_LD_COORDINATES);
		Object coordList = null;
		if (coordsWrap instanceof JsonArray cwa && !cwa.isEmpty() && cwa.getValue(0) instanceof JsonObject cw0) {
			coordList = cw0.getValue(NGSIConstants.JSON_LD_LIST);
		}
		Object coordinates = getCoordinates(coordList);

		return new JsonObject().put("type", geoType).put("coordinates", coordinates).encode();
	}

	private Object getCoordinates(Object coordinateList) {
		if (!(coordinateList instanceof JsonArray arr)) {
			return null;
		}
		JsonArray out = new JsonArray();
		for (Object elem : arr) {
			if (elem instanceof JsonObject eo) {
				if (eo.containsKey(NGSIConstants.JSON_LD_LIST)) {
					out.add(getCoordinates(eo.getValue(NGSIConstants.JSON_LD_LIST)));
				} else {
					out.add(eo.getValue(NGSIConstants.JSON_LD_VALUE));
				}
			} else {
				out.add((Object) null);
			}
		}
		return out;
	}

}
