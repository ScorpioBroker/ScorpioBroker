package eu.neclab.ngsildbroker.queryhandler.repository;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

import eu.neclab.ngsildbroker.commons.datatypes.terms.DataSetIdTerm;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import org.locationtech.spatial4j.context.SpatialContextFactory;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.io.GeoJSONReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.jsonldjava.core.Context;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.EntityCache;
import eu.neclab.ngsildbroker.commons.datatypes.EntityMap;
import eu.neclab.ngsildbroker.commons.datatypes.EntityMapEntry;
import eu.neclab.ngsildbroker.commons.datatypes.QueryRemoteHost;
import eu.neclab.ngsildbroker.commons.datatypes.RegistrationEntry;
import eu.neclab.ngsildbroker.commons.datatypes.terms.AttrsQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.CSFQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.GeoQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.LanguageQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.OmitTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.PickTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.QQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.ScopeQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.TypeQueryTerm;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.storage.ClientManager;
import eu.neclab.ngsildbroker.commons.tools.DBUtil;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.mutiny.tuples.Tuple3;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowIterator;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.Tuple;
import io.vertx.pgclient.PgException;

@Singleton
public class QueryDAO {

	@Inject
	ClientManager clientManager;

	GeoJSONReader geoReader = new GeoJSONReader(JtsSpatialContext.GEO, new SpatialContextFactory());

	private static Logger logger = LoggerFactory.getLogger(QueryDAO.class);

	public Uni<Map<String, Object>> getEntity(String entityId, String tenantId, AttrsQueryTerm attrsQuery,
			DataSetIdTerm datasetIdTerm) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {
			Tuple tuple = Tuple.tuple();
			int dollar = 1;
			StringBuilder query = new StringBuilder("SELECT ");
			if (attrsQuery != null) {
				dollar = attrsQuery.toSqlConstructEntity(query, tuple, dollar, datasetIdTerm);
			} else {
				query.append("ENTITY");
			}
			query.append(" FROM ENTITY WHERE ID=$");
			query.append(dollar);
			tuple.addString(entityId);
			return client.preparedQuery(query.toString()).execute(tuple).onItem().transformToUni(t -> {
				if (t.rowCount() == 0) {
					return Uni.createFrom().item(new HashMap<String, Object>());
				}
				Map<String, Object> result = t.iterator().next().getJsonObject(0).getMap();
				if (attrsQuery != null && !attrsQuery.getAttrs().isEmpty() && result.size() <= 4) {
					return Uni.createFrom().failure(new ResponseException(ErrorType.CombinationNotFound));
				}
				return Uni.createFrom().item(t.iterator().next().getJsonObject(0).getMap());
			});
		});

	}

	public Uni<RowSet<Row>> getRemoteSourcesForEntity(String entityId, AttrsQueryTerm attrs, String tenantId) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {

			if (attrs == null) {
				return client.preparedQuery(
						"SELECT C.endpoint, C.tenant_id, c.headers, c.reg_mode, (array_agg(DISTINCT C.e_prop) FILTER (WHERE C.e_prop is not null) || array_agg(DISTINCT C.e_rel) FILTER (WHERE C.e_rel is not null)) AS attrs FROM CSOURCEINFORMATION AS C WHERE C.retrieveEntity AND (C.E_ID=$1 OR C.E_ID is NULL)  AND (c.expires IS NULL OR c.expires >= now() at time zone 'utc') GROUP BY C.endpoint, C.tenant_id, c.headers, c.reg_mode")
						.execute(Tuple.of(entityId));
			} else {
				return client.preparedQuery(
						"SELECT C.endpoint, C.tenant_id, c.headers, c.reg_mode, (array_agg(DISTINCT C.e_prop) FILTER (WHERE C.e_prop is not null) || array_agg(DISTINCT C.e_rel) FILTER (WHERE C.e_rel is not null)) AS attrs FROM CSOURCEINFORMATION AS C WHERE C.retrieveEntity AND (C.E_ID=$1 OR C.E_ID is NULL) AND (C.e_prop is NULL OR C.e_prop IN $2) AND (C.e_rel is NULL OR C.e_rel IN $2) AND (c.expires IS NULL OR c.expires >= now() at time zone 'utc') GROUP BY C.endpoint, C.tenant_id, c.headers, c.reg_mode")
						.execute(Tuple.of(entityId, attrs.getAttrs()));
			}

		});
	}

	public Uni<RowSet<Row>> queryLocalOnly(String tenantId, String[] ids, TypeQueryTerm typeQuery, String idPattern,
			AttrsQueryTerm attrsQuery, QQueryTerm qQuery, GeoQueryTerm geoQuery, ScopeQueryTerm scopeQuery,
			LanguageQueryTerm langQuery, int limit, int offSet, boolean count, DataSetIdTerm dataSetIdTerm, String join,
			int joinLevel, PickTerm pickTerm, OmitTerm omitTerm) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {
			StringBuilder query = new StringBuilder();
			int dollar = 1;
			Tuple tuple = Tuple.tuple();
			String entitySelect = "D0";
			query.append("with D0 as(");
			if (count && limit == 0 && dataSetIdTerm == null) {
				query.append("SELECT COUNT(ENTITY)");
			} else {
				query.append("SELECT ");
				if (join != null && joinLevel > 0) {
					query.append("ID, true AS PARENT, ");
				}
				if (attrsQuery != null) {
					dollar = attrsQuery.toSqlConstructEntity(query, tuple, dollar, dataSetIdTerm);
				} else {
					query.append("ENTITY");
				}
				if (count) {
					query.append(", COUNT(*) as count");
				}
				query.append(" as entity, id");
			}
			query.append(" FROM ENTITY WHERE ");
			if (typeQuery == null && attrsQuery == null && geoQuery == null && qQuery == null && ids == null
					&& idPattern == null && scopeQuery == null) {
				query.append("TRUE ");
			} else {
				boolean sqlAdded = false;
				if (typeQuery != null) {
					dollar = typeQuery.toSql(query, tuple, dollar);
					sqlAdded = true;
				}
				if (attrsQuery != null) {
					if (sqlAdded) {
						query.append(" AND ");
					}
					dollar = attrsQuery.toSql(query, tuple, dollar);
					sqlAdded = true;
				}
				if (geoQuery != null) {
					if (sqlAdded) {
						query.append(" AND ");
					}
					dollar = geoQuery.toSql(query, tuple, dollar);
					sqlAdded = true;
				}

				if (qQuery != null) {
					if (sqlAdded) {
						query.append(" AND ");
					}
					dollar = qQuery.toSql(query, dollar, tuple, false, true);
					sqlAdded = true;
				}
				if (ids != null) {
					if (sqlAdded) {
						query.append(" AND ");
					}
					query.append("id IN (");
					for (String id : ids) {
						query.append('$');
						query.append(dollar);
						query.append(',');
						tuple.addString(id);
						dollar++;
					}

					query.setCharAt(query.length() - 1, ')');
					sqlAdded = true;
				}
				if (idPattern != null) {
					if (sqlAdded) {
						query.append(" AND ");
					}
					query.append("id ~ $");
					query.append(dollar);
					tuple.addString(idPattern);
					dollar++;
					sqlAdded = true;
				}
				if (scopeQuery != null) {
					query.append(" AND ");
					scopeQuery.toSql(query);
				}
			}
			if (!(count && limit == 0 && dataSetIdTerm == null)) {
				query.append(" GROUP BY ENTITY,id");
			}
			if (limit != 0) {
				query.append(" LIMIT ");
				query.append(limit);
				query.append(" OFFSET ");
				query.append(offSet);
			}
			query.append(")");
			int counter = 0;
			if (join != null && joinLevel > 0) {

				query.append(", ");
				for (counter = 0; counter < joinLevel; counter++) {
					query.append('B');
					query.append(counter + 1);
					query.append(" AS (SELECT ");
					query.append('D');
					query.append(counter);
					query.append(".ID AS ID, X.VALUE AS VALUE FROM D");
					query.append(counter);
					query.append(", JSONB_EACH(D");
					query.append(counter);
					query.append(".ENTITY) AS X WHERE JSONB_TYPEOF(X.VALUE) = 'array'), ");
					query.append('C');
					query.append(counter + 1);
					query.append(" AS (SELECT distinct Z ->> '@id' as link FROM B");
					query.append(counter + 1);
					query.append(", JSONB_ARRAY_ELEMENTS(B");
					query.append(counter + 1);
					query.append(
							".VALUE) AS Y, JSONB_ARRAY_ELEMENTS(Y #> '{https://uri.etsi.org/ngsi-ld/hasObject}') AS Z WHERE Y #>> '{@type,0}' = 'https://uri.etsi.org/ngsi-ld/Relationship'), ");
					query.append('D');
					query.append(counter + 1);
					query.append(" as (SELECT E.ID as id, false as parent, E.ENTITY as entity");
					if (count) {
						query.append(", -1 as count");
					}
					query.append(" from C");
					query.append(counter + 1);
					query.append(" join ENTITY as E on C");
					query.append(counter + 1);
					query.append(".link = E.ID), ");
				}
				query.setLength(query.length() - 2);
				query.append(" SELECT * FROM (");
				for (int i = 0; i <= joinLevel; i++) {
					query.append("SELECT * FROM D");
					query.append(i);
					query.append(" UNION ALL ");
				}
				query.setLength(query.length() - " UNION ALL ".length());
				query.append(") as xyz group by id, entity, parent");
				entitySelect = "xyz";
			}

			if (dataSetIdTerm != null) {
				query.append(",");
				dollar = dataSetIdTerm.toSql(query, tuple, dollar, entitySelect);
				if (count && limit == 0) {
					query.append("select count(x.entity) from x");
				} else {
					query.append("select x.entity, (select count(x.entity) from x),x.id from x");
				}
			} else {
				query.append("select a.entity,(select count(*) from a) from a");

			}
			query.append(';');
			String queryString = query.toString();
			logger.debug("SQL REQUEST: " + queryString);
			logger.debug("SQL TUPLE: " + tuple.deepToString());
			return client.preparedQuery(queryString).execute(tuple);
		});
	}

	public Uni<Map<String, Object>> getTypes(String tenantId) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {
			return client.preparedQuery(
					"SELECT DISTINCT myTypes from (SELECT to_jsonb(unnest(e_types)) as myTypes from entity UNION ALL SELECT to_jsonb(e_type) as myTypes from csourceinformation);")
					.execute().onItem().transform(rows -> {
						Map<String, Object> result = Maps.newHashMap();
						result.put(NGSIConstants.JSON_LD_TYPE, Lists.newArrayList(NGSIConstants.NGSI_LD_ENTITY_LIST));
						List<Map<String, String>> typeList = Lists.newArrayList();
						rows.forEach(row -> {
							Map<String, String> tmp = Maps.newHashMap();
							tmp.put(NGSIConstants.JSON_LD_ID, row.getString(0));
							typeList.add(tmp);
						});
						result.put(NGSIConstants.NGSI_LD_TYPE_LIST, typeList);
						result.put(NGSIConstants.JSON_LD_ID, AppConstants.TYPE_LIST_PREFIX + typeList.hashCode());
						return result;
					});
		});
	}

	public Uni<Map<String, Object>> getAttributeList(String tenantId) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {

			return client.preparedQuery(
					"select distinct x from entity, jsonb_object_keys(entity.entity) as x where x not in ('"
							+ NGSIConstants.JSON_LD_ID + "', '" + NGSIConstants.JSON_LD_TYPE + "', '"
							+ NGSIConstants.NGSI_LD_CREATED_AT + "', '" + NGSIConstants.NGSI_LD_MODIFIED_AT + "')")
					.execute().onItem().transform(rows -> {
						Map<String, Object> result = Maps.newHashMap();
						List<Map<String, String>> attribs = new ArrayList<>(rows.size());
						rows.forEach(row -> {
							attribs.add(Map.of(NGSIConstants.JSON_LD_ID, row.getString(0)));
						});
						result.put(NGSIConstants.JSON_LD_ID, AppConstants.ATTRIBUTE_LIST_PREFIX + attribs.hashCode());
						result.put(NGSIConstants.JSON_LD_TYPE, List.of(NGSIConstants.NGSI_LD_ATTRIBUTE_LIST_TYPE));
						result.put(NGSIConstants.NGSI_LD_ATTRIBUTE_LIST_ATTRIBUTE_KEY, attribs);
						return result;
					});
		});
	}

	public Uni<List<Map<String, Object>>> getAttributesDetail(String tenantId) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {

			return client.preparedQuery(
					"select distinct x, jsonb_agg(jsonb_build_object('@id',y)) from entity, jsonb_object_keys(entity.entity) as x, unnest(entity.e_types) as y where x not in ('"
							+ NGSIConstants.JSON_LD_ID + "', '" + NGSIConstants.JSON_LD_TYPE + "', '"
							+ NGSIConstants.NGSI_LD_CREATED_AT + "', '" + NGSIConstants.NGSI_LD_MODIFIED_AT
							+ "') group by x")
					.execute().onItem().transform(rows -> {
						List<Map<String, Object>> result = new ArrayList<>(rows.size());
						rows.forEach(row -> {
							String attrib = row.getString(0);
							List<Map<String, Object>> types = row.getJsonArray(1).getList();
							Map<String, Object> tmp = Maps.newHashMap();
							tmp.put(NGSIConstants.JSON_LD_ID, attrib);
							tmp.put(NGSIConstants.JSON_LD_TYPE, Lists.newArrayList(NGSIConstants.NGSI_LD_ATTRIBUTE));
							Map<String, Object> tmp2 = Maps.newHashMap();
							tmp2.put(NGSIConstants.JSON_LD_ID, attrib);
							tmp.put(NGSIConstants.NGSI_LD_ATTRIBUTE_NAME, Lists.newArrayList(tmp2));
							tmp.put(NGSIConstants.NGSI_LD_TYPE_NAMES, types.stream().distinct().toList());
							result.add(tmp);
						});
						return result;
					});
		});
	}

	public Uni<Map<String, Object>> getAttributeDetail(String tenantId, String attribId) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {
			Tuple tuple = Tuple.tuple();
			tuple.addString(attribId);
			return client.preparedQuery(
					"Select jsonb_agg(distinct jsonb_build_object('@id', x#>>'{@type,0}')), jsonb_agg(distinct jsonb_build_object('@id', y)), count(entity) from entity, jsonb_array_elements(entity.entity-> $1) as x, jsonb_array_elements(entity->'@type') as y where entity ? $1")
					.execute(tuple).onItem().transformToUni(rows -> {
						if (rows.size() == 0) {
							return Uni.createFrom().item(new HashMap<>(0));
						}
						Row row = rows.iterator().next();
						Long count = row.getLong(2);
						if (count == 0) {
							return Uni.createFrom().item(new HashMap<>(0));
						}
						List types = row.getJsonArray(1).getList();
						List attribTypes = row.getJsonArray(0).getList();

						Map<String, Object> result = Maps.newHashMap();
						result.put(NGSIConstants.JSON_LD_ID, attribId);
						result.put(NGSIConstants.JSON_LD_TYPE, Lists.newArrayList(NGSIConstants.NGSI_LD_ATTRIBUTE));
						Map<String, Object> tmp = Maps.newHashMap();
						tmp.put(NGSIConstants.JSON_LD_ID, attribId);
						result.put(NGSIConstants.NGSI_LD_ATTRIBUTE_NAME, Lists.newArrayList(tmp));
						result.put(NGSIConstants.NGSI_LD_TYPE_NAMES, types);
						result.put(NGSIConstants.NGSI_LD_ATTRIBUTE_TYPES, attribTypes);
						result.put(NGSIConstants.NGSI_LD_ATTRIBUTE_COUNT_SHORT, Lists.newArrayList(count));
						return Uni.createFrom().item(result);
					});
		});
	}

	public Uni<List<Map<String, Object>>> getTypesWithDetails(String tenantId) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {
			return client.preparedQuery("SELECT DISTINCT myTypes, jsonb_agg(jsonb_build_object('"
					+ NGSIConstants.JSON_LD_ID
					+ "', myAttr)) from entity, jsonb_array_elements(ENTITY -> '@type') as myTypes, jsonb_object_keys((ENTITY - ARRAY['"
					+ NGSIConstants.JSON_LD_TYPE + "', '" + NGSIConstants.JSON_LD_ID + "', '"
					+ NGSIConstants.NGSI_LD_CREATED_AT + "','" + NGSIConstants.NGSI_LD_MODIFIED_AT
					+ "'])) as myAttr group by myTypes").execute().onItem().transform(rows -> {
						List<Map<String, Object>> result = Lists.newArrayList();
						rows.forEach(row -> {
							Map<String, Object> resultEntry = Maps.newHashMap();
							resultEntry.put(NGSIConstants.JSON_LD_ID, row.getString(0));
							resultEntry.put(NGSIConstants.JSON_LD_TYPE,
									Lists.newArrayList(NGSIConstants.NGSI_LD_ENTITY_TYPE));
							Map<String, String> tmp = Maps.newHashMap();
							tmp.put(NGSIConstants.JSON_LD_ID, row.getString(0));
							resultEntry.put(NGSIConstants.NGSI_LD_TYPE_NAME, Lists.newArrayList(tmp));
							resultEntry.put(NGSIConstants.NGSI_LD_ATTRIBUTE_NAMES,
									row.getJsonArray(1).getList().stream().distinct().toList());
							result.add(resultEntry);
						});
						return result;
					});
		});
	}

	public Uni<Map<String, Object>> getType(String tenantId, String type) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {
			Tuple tuple = Tuple.tuple();
			tuple.addArrayOfString(new String[] { type });
			return client.preparedQuery(
					"with a as (select x as id, entity -> x as data from entity, jsonb_object_keys(entity.entity) as x where e_types && $1::text[] and x not in ('"
							+ NGSIConstants.JSON_LD_ID + "', '" + NGSIConstants.JSON_LD_TYPE + "', '"
							+ NGSIConstants.NGSI_LD_CREATED_AT + "', '" + NGSIConstants.NGSI_LD_MODIFIED_AT
							+ "')), b as (SELECT count(entity.id) as mycount FROM entity where e_types && $1::text[]) "
							+ "select b.mycount, a.id, jsonb_agg(distinct jsonb_build_object('"
							+ NGSIConstants.JSON_LD_ID + "', x#>'{" + NGSIConstants.JSON_LD_TYPE
							+ ",0}')) from b, a, jsonb_array_elements(a.data) as x group by a.id, b.mycount;"

			).execute(tuple).onItem().transform(rows -> {
				Map<String, Object> result = Maps.newHashMap();
				if (rows.size() == 0) {
					return result;
				}
				long count = 0;
				RowIterator<Row> it = rows.iterator();
				List<Map<String, Object>> attrDetails = Lists.newArrayList();
				while (it.hasNext()) {
					Row row = it.next();
					count = row.getLong(0);
					Map<String, Object> attribDetail = Maps.newHashMap();
					Map<String, String> tmp = Maps.newHashMap();
					tmp.put(NGSIConstants.JSON_LD_ID, row.getString(1));
					attribDetail.put(NGSIConstants.JSON_LD_ID, row.getString(1));
					attribDetail.put(NGSIConstants.TYPE, NGSIConstants.ATTRIBUTE);
					attribDetail.put(NGSIConstants.NGSI_LD_ATTRIBUTE_NAME, Lists.newArrayList(tmp));
					attribDetail.put(NGSIConstants.NGSI_LD_ATTRIBUTE_TYPES, row.getJsonArray(2).getList());
					attrDetails.add(attribDetail);
				}
				result.put(NGSIConstants.ATTRIBUTE_DETAILS, attrDetails);
				Map<String, Long> countMap = Maps.newHashMap();
				countMap.put(NGSIConstants.JSON_LD_VALUE, count);
				result.put(NGSIConstants.NGSI_LD_ENTITY_COUNT, countMap);
				result.put(NGSIConstants.JSON_LD_ID, type);
				result.put(NGSIConstants.JSON_LD_TYPE, Lists.newArrayList(NGSIConstants.NGSI_LD_ENTITY_TYPE_INFO));
				Map<String, String> tmp = Maps.newHashMap();
				tmp.put(NGSIConstants.JSON_LD_ID, type);
				result.put(NGSIConstants.NGSI_LD_TYPE_NAME, Lists.newArrayList(tmp));
				return result;
			});
		});
	}

	public Uni<RowSet<Row>> getRemoteSourcesForTypes(String tenantId) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {
			return client.preparedQuery(
					"SELECT C.endpoint, C.tenant_id, c.headers, c.reg_mode FROM CSOURCEINFORMATION AS C WHERE C.retrieveEntityTypes=true")
					.execute();
		});
	}

	public Uni<String[]> getRemoteTypesForRegWithoutTypesSupport(String tenantId) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {
			return client.preparedQuery(
					"SELECT case when array_agg(distinct C.e_type) is null then array[]::text[] else array_agg(distinct C.e_type) end FROM CSOURCEINFORMATION AS C WHERE C.retrieveAttrTypes=false AND C.e_type is not null")
					.execute().onItem().transform(rows -> {
						return rows.iterator().next().getArrayOfStrings(0);
					});
		});
	}

	public Uni<RowSet<Row>> getRemoteSourcesForTypesWithDetails(String tenantId) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {
			return client.preparedQuery(
					"SELECT C.endpoint, C.tenant_id, c.headers, c.reg_mode FROM CSOURCEINFORMATION AS C WHERE C.retrieveEntityTypeDetails=true")
					.execute();
		});
	}

	public Uni<Map<String, Set<String>>> getRemoteTypesWithDetailsForRegWithoutTypeSupport(String tenantId) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {
			return client.preparedQuery(
					"SELECT C.e_type, C.e_prop, c.e_rel FROM CSOURCEINFORMATION AS C WHERE C.retrieveEntityTypeDetails=false AND C.e_type is not null")
					.execute().onItem().transform(rows -> {
						Map<String, Set<String>> result = Maps.newHashMap();
						rows.forEach(row -> {
							String type = row.getString(0);
							String prop = row.getString(1);
							String rel = row.getString(2);
							Set<String> attribs = result.get(type);
							if (attribs == null) {
								attribs = Sets.newHashSet();
								result.put(type, attribs);
							}
							if (prop != null) {
								attribs.add(prop);
							}
							if (rel != null) {
								attribs.add(prop);
							}

						});
						return result;
					});
		});
	}

	public Uni<RowSet<Row>> getRemoteSourcesForType(String tenantId, String type) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {
			return client.preparedQuery(
					"SELECT C.endpoint, C.tenant_id, c.headers, c.reg_mode FROM CSOURCEINFORMATION AS C WHERE C.retrieveEntityTypeInfo=true AND (C.e_type is NULL OR C.e_type=$1)")
					.execute(Tuple.of(type));
		});
	}

	public Uni<Map<String, Set<String>>> getRemoteTypeInfoForRegWithOutTypeSupport(String tenantId, String type) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {
			return client.preparedQuery(
					"SELECT C.e_prop, C.e_rel FROM CSOURCEINFORMATION AS C WHERE C.retrieveEntityTypeInfo=false AND (C.e_type=$1) AND (C.e_prop is not null OR C.e_rel is not null)")
					.execute(Tuple.of(type)).onItem().transform(rows -> {
						if (rows.size() == 0) {
							return Maps.newHashMap();
						}
						Map<String, Set<String>> result = Maps.newHashMap();
						rows.forEach(row -> {
							String prop = row.getString(0);
							String rel = row.getString(1);
							String attribName = prop == null ? rel : prop;
							String attribType = prop == null ? NGSIConstants.NGSI_LD_PROPERTY
									: NGSIConstants.NGSI_LD_RELATIONSHIP;
							Set<String> types = result.get(attribName);
							if (types == null) {
								types = Sets.newHashSet();
								result.put(attribName, types);
							}
							types.add(attribType);
						});
						return result;

					});
		});
	}

	public Uni<RowSet<Row>> getRemoteSourcesForAttribs(String tenantId) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {
			return client.preparedQuery(
					"SELECT C.endpoint, C.tenant_id, c.headers, c.reg_mode FROM CSOURCEINFORMATION AS C WHERE C.retrieveAttrTypes=true")
					.execute();
		});
	}

	public Uni<Set<String>> getRemoteAttribsForRegWithoutAttribSupport(String tenantId) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {
			return client.preparedQuery(
					"SELECT C.e_prop, C.e_rel FROM CSOURCEINFORMATION AS C WHERE C.retrieveAttrTypes=true AND (C.e_prop is not null OR C.e_rel is not null)")
					.execute().onItem().transform(rows -> {
						Set<String> result = Sets.newHashSet();
						rows.forEach(row -> {
							String prop = row.getString(0);
							String rel = row.getString(1);
							if (prop != null) {
								result.add(prop);
							}
							if (rel != null) {
								result.add(rel);
							}
						});
						return result;
					});
		});
	}

	public Uni<RowSet<Row>> getRemoteSourcesForAttribsWithDetails(String tenantId) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {
			return client.preparedQuery(
					"SELECT C.endpoint, C.tenant_id, c.headers, c.reg_mode FROM CSOURCEINFORMATION AS C WHERE C.retrieveAttrTypeDetails=true")
					.execute();
		});
	}

	public Uni<Map<String, Set<String>>> getRemoteAttribsWithDetailsForRegWithoutAttribSupport(String tenantId) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {
			return client.preparedQuery(
					"SELECT C.e_type, C.e_prop, C.e_rel  FROM CSOURCEINFORMATION AS C WHERE C.retrieveAttrTypeDetails=false AND (c.e_prop is not null OR c.e_rel is not null)")
					.execute().onItem().transform(rows -> {
						Map<String, Set<String>> result = Maps.newHashMap();
						rows.forEach(row -> {
							String type = row.getString(0);
							String prop = row.getString(1);
							String rel = row.getString(2);
							String attr = prop == null ? rel : prop;
							Set<String> types = result.get(attr);
							if (types == null) {
								types = Sets.newHashSet();
								result.put(attr, types);
							}
							if (type != null) {
								types.add(type);
							}
						});
						return result;
					});
		});
	}

	public Uni<RowSet<Row>> getRemoteSourcesForAttrib(String tenantId, String attrib) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {
			return client.preparedQuery(
					"SELECT C.endpoint, C.tenant_id, c.headers, c.reg_mode FROM CSOURCEINFORMATION AS C WHERE C.retrieveAttrTypeInfo=true AND (C.e_prop is NULL OR C.e_prop=$1) AND (C.e_rel is NULL OR C.e_rel=$1)")
					.execute(Tuple.of(attrib));
		});
	}

	public Uni<Tuple2<Set<String>, Set<String>>> getRemoteAttribForRegWithoutAttribSupport(String tenantId,
			String attrib) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {
			return client.preparedQuery(
					"SELECT C.e_type, (C.e_prop is null) FROM CSOURCEINFORMATION AS C WHERE C.retrieveAttrTypeInfo=false AND C.e_prop=$1 OR C.e_rel=$1")
					.execute(Tuple.of(attrib)).onItem().transform(rows -> {
						Set<String> attribTypes = Sets.newHashSet();
						Set<String> entityTypes = Sets.newHashSet();
						rows.forEach(row -> {
							String type = row.getString(0);
							boolean isRel = row.getBoolean(1);
							if (isRel) {
								attribTypes.add(NGSIConstants.NGSI_LD_RELATIONSHIP);
							} else {
								attribTypes.add(NGSIConstants.NGSI_LD_PROPERTY);
							}
							if (type != null) {
								entityTypes.add(type);
							}
						});
						return Tuple2.of(attribTypes, entityTypes);
					});
		});
	}

	public Uni<RowSet<Row>> getRemoteSourcesForQuery(String tenantId, Set<String> id, TypeQueryTerm typeQuery,
			String idPattern, AttrsQueryTerm attrsQuery, QQueryTerm qQuery, CSFQueryTerm csf, GeoQueryTerm geoQuery,
			ScopeQueryTerm scopeQuery) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {
			Set<String> attribs = Sets.newHashSet();
			if (attrsQuery != null) {
				attribs.addAll(attrsQuery.getAttrs());
			}
			if (qQuery != null && attrsQuery == null && typeQuery == null) {
				Set<String> result = Sets.newHashSet();
				getAttribsFromQuery(qQuery, result);
				attribs.addAll(result);
			}
			Set<String> types = Sets.newHashSet();
			if (typeQuery != null) {
				getTypesFromQuery(typeQuery, types);
			}
			List<Object> prepareObjects = Lists.newArrayList();
			StringBuilder queryFront = new StringBuilder(
					"SELECT C.endpoint C.tenant_id, c.headers, c.reg_mode, c.queryEntity, c.queryBatch");
			StringBuilder wherePart = new StringBuilder(" WHERE ");
			int dollarCount = 1;
			if (!types.isEmpty()) {
				queryFront.append(", array_agg(DISTINCT C.e_type) FILTER (WHERE C.e_type is not null) as entityType");
				wherePart.append("(C.e_type is NULL OR C.e_type in $");
				wherePart.append(dollarCount);
				wherePart.append(") AND ");
				prepareObjects.add(types);
				dollarCount++;
			} else {
				queryFront.append(", null as entityType");
			}
			if (id != null) {
				queryFront.append(", array_agg(DISTINCT C.e_id) FILTER (WHERE C.e_id is not null) as entityId");
				wherePart.append("(C.e_id is NULL OR C.e_id in $");
				wherePart.append(dollarCount);
				wherePart.append(") AND (C.e_id_p is NULL OR C.e_id_p ~ ANY($");
				wherePart.append(dollarCount);
				wherePart.append(")) AND ");
				prepareObjects.add(id);
				dollarCount++;
			} else {
				queryFront.append(", null as entityId");
			}
			if (idPattern != null) {
				queryFront.append(", array_agg(DISTINCT C.e_id) FILTER (WHERE C.e_id is not null) as entityId");
				wherePart.append("(C.e_id is NULL OR $");
				wherePart.append(dollarCount);
				wherePart.append(" ~ C.e_id) AND (C.e_id_p is NULL OR $");
				wherePart.append(dollarCount);
				wherePart.append(" ~ C.e_id_p) AND ");
				prepareObjects.add(id);
				dollarCount++;
			} else {
				queryFront.append(", null as entityId");
			}
			if (attribs.isEmpty()) {
				queryFront.append(
						", (array_agg(DISTINCT C.e_prop) FILTER (WHERE C.e_prop is not null) || array_agg(DISTINCT C.e_rel) FILTER (WHERE C.e_rel is not null)) AS attrs");
				wherePart.append("(C.e_prop is NULL OR C.e_prop IN $");
				wherePart.append(dollarCount);
				wherePart.append(") AND (C.e_rel is NULL OR C.e_rel IN $");
				wherePart.append(dollarCount);
				wherePart.append(")) AND ");
			} else {
				queryFront.append(", null as attrs");
			}
			if (geoQuery != null) {
				// TODO user intersect between search area and registration area
			} else {
				queryFront.append(", null as geoq");
			}
			if (scopeQuery != null) {
				// TODO array check between scopes from query and reg query
			} else {
				queryFront.append(", null as scopeq");
			}
			if (csf != null) {
				// TODO talk with martin to understand csf better
			}
			// remove last " AND "
			wherePart.setLength(wherePart.length() - 5);
			queryFront.append(" FROM CSOURCEINFORMATION AS C");
			queryFront.append(wherePart);

			return client.preparedQuery(queryFront.toString()).execute(Tuple.from(prepareObjects));
		});

	}

	private void getTypesFromQuery(TypeQueryTerm typeQuery, Set<String> result) {
		String type = typeQuery.getType();
		if (type != null) {
			result.add(type);
		}
		TypeQueryTerm child = typeQuery.getFirstChild();
		if (child != null) {
			getTypesFromQuery(child, result);
		}

		while (typeQuery.hasNext()) {
			getTypesFromQuery(typeQuery.getNext(), result);
		}
	}

	private void getAttribsFromQuery(QQueryTerm qQuery, Set<String> result) {
		String attrib = qQuery.getAttribute();
		if (attrib != null) {
			result.add(attrib);
		}
		QQueryTerm child = qQuery.getFirstChild();
		if (child != null) {
			getAttribsFromQuery(child, result);
		}

		while (qQuery.hasNext()) {
			getAttribsFromQuery(qQuery.getNext(), result);
		}
	}

	public Uni<Table<String, String, List<RegistrationEntry>>> getAllRegistries() {
		return clientManager.getClient(AppConstants.INTERNAL_NULL_KEY, false).onItem().transformToUni(client -> {
			return client.preparedQuery("SELECT tenant_id FROM tenant").execute().onItem()
					.transformToUni(tenantRows -> {
						List<Uni<Tuple2<String, RowSet<Row>>>> unis = Lists.newArrayList();
						RowIterator<Row> it = tenantRows.iterator();
						String sql = "SELECT cs_id, c_id, e_id, e_id_p, e_type, e_prop, e_rel, ST_AsGeoJSON(i_location), scopes, EXTRACT(MILLISECONDS FROM expires), endpoint, tenant_id, headers, reg_mode, createEntity, updateEntity, appendAttrs, updateAttrs, deleteAttrs, deleteEntity, createBatch, upsertBatch, updateBatch, deleteBatch, upsertTemporal, appendAttrsTemporal, deleteAttrsTemporal, updateAttrsTemporal, deleteAttrInstanceTemporal, deleteTemporal, mergeEntity, replaceEntity, replaceAttrs, mergeBatch, retrieveEntity, queryEntity, queryBatch, retrieveTemporal, queryTemporal, retrieveEntityTypes, retrieveEntityTypeDetails, retrieveEntityTypeInfo, retrieveAttrTypes, retrieveAttrTypeDetails, retrieveAttrTypeInfo, createSubscription, updateSubscription, retrieveSubscription, querySubscription, deleteSubscription FROM csourceinformation WHERE queryentity OR querybatch OR retrieveentity OR retrieveentitytypes OR retrieveentitytypedetails OR retrieveentitytypeinfo OR retrieveattrtypes OR retrieveattrtypedetails OR retrieveattrtypeinfo";
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

	/*
	 * public Uni<Map<String, Tuple2<Map<String, Object>, String>>>
	 * queryForEntityIds(String tenant, String[] ids, TypeQueryTerm typeQuery,
	 * String idPattern, AttrsQueryTerm attrsQuery, QQueryTerm qQuery, GeoQueryTerm
	 * geoQuery, ScopeQueryTerm scopeQuery, Context context, int limit, int offset,
	 * DataSetIdTerm dataSetIdTerm, String join, int joinLevel, boolean isDist) {
	 * return clientManager.getClient(tenant, false).onItem().transformToUni(client
	 * -> { StringBuilder query = new StringBuilder(); int dollar = 1; Tuple tuple =
	 * Tuple.tuple(); boolean doJoin = (join != null && joinLevel > 0);
	 * query.append("WITH a as (SELECT ID");
	 * 
	 * query.append(" FROM ENTITY WHERE "); boolean sqlAdded = false; if (typeQuery
	 * != null) { dollar = typeQuery.toSql(query, tuple, dollar); sqlAdded = true; }
	 * if (attrsQuery != null) { if (sqlAdded) { query.append(" AND "); } dollar =
	 * attrsQuery.toSql(query, tuple, dollar); sqlAdded = true; } if (geoQuery !=
	 * null) { if (sqlAdded) { query.append(" AND "); } dollar =
	 * geoQuery.toSql(query, tuple, dollar); sqlAdded = true; }
	 * 
	 * if (qQuery != null) { if (sqlAdded) { query.append(" AND "); } dollar =
	 * qQuery.toSql(query, dollar, tuple, isDist, false); sqlAdded = true; } if (ids
	 * != null) { if (sqlAdded) { query.append(" AND "); } query.append("id IN (");
	 * for (String id : ids) { query.append('$'); query.append(dollar);
	 * query.append(','); tuple.addString(id); dollar++; }
	 * 
	 * query.setCharAt(query.length() - 1, ')'); sqlAdded = true; } if (idPattern !=
	 * null) { if (sqlAdded) { query.append(" AND "); } query.append("id ~ $");
	 * query.append(dollar); tuple.addString(idPattern); dollar++; sqlAdded = true;
	 * } if (scopeQuery != null) { query.append(" AND "); scopeQuery.toSql(query); }
	 * query.append(" ORDER BY createdAt), b as (SELECT a.ID FROM a limit $");
	 * query.append(dollar); tuple.addInteger(limit); dollar++;
	 * query.append(" offset $"); query.append(dollar); tuple.addInteger(offset);
	 * dollar++; query.append("), c as (SELECT ENTITY.ID, "); if (attrsQuery !=
	 * null) { dollar = attrsQuery.toSqlConstructEntity(query, tuple, dollar,
	 * dataSetIdTerm); } else { query.append("ENTITY.ENTITY"); } query.
	 * append(" as ENTITY, NULL as PARENT FROM b left join ENTITY on b.ID = ENTITY.ID)  "
	 * ); String currentEntitySet = "c"; if (dataSetIdTerm != null) {
	 * query.append(", "); dollar = dataSetIdTerm.toSql(query, tuple, dollar,
	 * currentEntitySet); currentEntitySet = "x"; } if (doJoin) { int counter;
	 * query.append(", D0 as (SELECT * FROM "); query.append(currentEntitySet);
	 * query.append(")"); for (counter = 0; counter < joinLevel; counter++) {
	 * query.append('B'); query.append(counter + 1); query.append(" AS (SELECT ");
	 * query.append('D'); query.append(counter);
	 * query.append(".ID AS ID, X.VALUE AS VALUE FROM D"); query.append(counter);
	 * query.append(", JSONB_EACH(D"); query.append(counter);
	 * query.append(".ENTITY) AS X WHERE JSONB_TYPEOF(X.VALUE) = 'array'), ");
	 * query.append('C'); query.append(counter + 1);
	 * query.append(" AS (SELECT distinct Z ->> '@id' as link FROM B");
	 * query.append(counter + 1); query.append(", JSONB_ARRAY_ELEMENTS(B");
	 * query.append(counter + 1); query.append(
	 * ".VALUE) AS Y, JSONB_ARRAY_ELEMENTS(Y #> '{https://uri.etsi.org/ngsi-ld/hasObject}') AS Z WHERE Y #>> '{@type,0}' = 'https://uri.etsi.org/ngsi-ld/Relationship') AND Y ? 'https://uri.etsi.org/ngsi-ld/hasObjectType', "
	 * ); query.append('D'); query.append(counter + 1);
	 * query.append(" as (SELECT E.ID as id, E.ENTITY as entity, C");
	 * query.append(counter + 1); query.append(".link as parent");
	 * 
	 * query.append(" from C"); query.append(counter + 1);
	 * query.append(" join ENTITY as E on C"); query.append(counter + 1);
	 * query.append(".link = E.ID), "); } query.setLength(query.length() - 2);
	 * query.append(" SELECT * FROM ("); for (int i = 0; i <= joinLevel; i++) {
	 * query.append("SELECT * FROM D"); query.append(i);
	 * query.append(" UNION ALL "); } query.setLength(query.length() -
	 * " UNION ALL ".length());
	 * query.append(") as xyz group by id, entity, parent"); currentEntitySet =
	 * "xyz"; }
	 * 
	 * query.append(" SELECT a.ID, "); query.append(currentEntitySet);
	 * query.append(".ENTITY, "); query.append(currentEntitySet);
	 * query.append(".parent FROM a left outer join ");
	 * query.append(currentEntitySet); query.append(" on a.ID = ");
	 * query.append(currentEntitySet); query.append(".ID");
	 * 
	 * String queryString = query.toString(); logger.debug("SQL REQUEST: " +
	 * queryString); logger.debug("SQL TUPLE: " + tuple.deepToString()); return
	 * client.preparedQuery(queryString).execute(tuple).onItem().transform(rows -> {
	 * Map<String, Tuple2<Map<String, Object>, String>> result = Maps.newHashMap();
	 * rows.forEach(row -> { String id = row.getString(0); JsonObject entity =
	 * row.getJsonObject(1); String parent = row.getString(2); Map<String, Object>
	 * entityMap = null; if (entity != null) { entityMap = entity.getMap(); }
	 * 
	 * result.put(id, Tuple2.of(entityMap, parent));
	 * 
	 * }); return result; }); }).onFailure().recoverWithUni(e -> { if (e instanceof
	 * PgException pge) { logger.debug(pge.getPosition()); if
	 * (pge.getSqlState().equals(AppConstants.INVALID_REGULAR_EXPRESSION)) { return
	 * Uni.createFrom() .failure(new ResponseException(ErrorType.BadRequestData,
	 * "Invalid regular expression")); } if
	 * (pge.getSqlState().equals(AppConstants.INVALID_GEO_QUERY)) { return
	 * Uni.createFrom().failure(new ResponseException(ErrorType.BadRequestData,
	 * "Invalid geo query. " + pge.getErrorMessage())); } } return
	 * Uni.createFrom().failure(e); }); }
	 */
	public Uni<Void> storeEntityMap(String tenant, String qToken, EntityMap entityMap, boolean deleteOldEntries) {

		if (entityMap.getEntityList().isEmpty()) {
			return Uni.createFrom().voidItem();
		}
		List<Tuple> batch = Lists.newArrayList();
//			"q_token" text NOT NULL,
//		    "entity_id" text,
//			"remote_hosts" jsonb,
//			"order_field" numeric NOT NULL,		
//			"fullentities_dist" boolean;
//			"regempty" boolean;
//			"noregentry" boolean;
		long count = 0;
		for (EntityMapEntry entityId2RemoteHosts : entityMap.getEntityList()) {
			Tuple tuple = Tuple.tuple();
			tuple.addString(qToken);
			tuple.addString(entityId2RemoteHosts.getEntityId());
			JsonArray remoteHosts = new JsonArray();
			for (QueryRemoteHost remoteHost : entityId2RemoteHosts.getRemoteHosts()) {
				remoteHosts.add(remoteHost.toJson());
			}
			tuple.addJsonArray(remoteHosts);
			tuple.addLong(count);
			tuple.addBoolean(entityMap.isOnlyFullEntitiesDistributed());
			tuple.addBoolean(entityMap.isRegEmpty());
			tuple.addBoolean(entityMap.isNoRootLevelRegEntry());
			count++;
			batch.add(tuple);
		}
		return clientManager.getClient(tenant, false).onItem().transformToUni(client -> {
			String sql;
			Uni<Void> delete;
			if (deleteOldEntries) {
				delete = client.preparedQuery("DELETE FROM entitymap WHERE id=$1").execute(Tuple.of(qToken)).onItem()
						.transformToUni(rows -> Uni.createFrom().voidItem());
			} else {
				delete = Uni.createFrom().voidItem();
			}
			return delete.onItem().transformToUni(
					v -> client.preparedQuery("INSERT INTO entitymap VALUES ($1, $2, $3, $4, $5, $6, $7)")
							.executeBatch(batch).onItem().transformToUni(r -> {
								return client.preparedQuery(
										"INSERT INTO entitymap_management VALUES ($1, now(), $2) ON CONFLICT(q_token) DO UPDATE SET last_access=now(), linked_maps=$2")
										.execute(Tuple.of(qToken, new JsonObject(entityMap.getLinkedMaps()))).onItem()
										.transformToUni(r2 -> Uni.createFrom().voidItem());
							}));
		});
	}

	public Uni<List<Map<String, Object>>> getEntities(String tenant, Map<Set<String>, Set<String>> types2EntityIds,
			QQueryTerm qQuery) {
		Tuple tuple = Tuple.tuple();
		StringBuilder query = new StringBuilder("SELECT id, entity FROM entity WHERE (");
		int dollarCount = 1;
		for (Entry<Set<String>, Set<String>> entry : types2EntityIds.entrySet()) {
			Set<String> types = entry.getValue();
			Set<String> entityIds = entry.getKey();
			query.append("(id = ANY(");
			for (String id : entityIds) {
				query.append('$');
				query.append(dollarCount);
				query.append(',');
				dollarCount++;
				tuple.addString(id);
			}
			query.setCharAt(query.length() - 1, ')');
			query.append(" AND e_types && $");
			query.append(dollarCount);
			dollarCount++;
			tuple.addArrayOfString(types.toArray(new String[0]));
			query.append("::text[]) AND ");

		}
		query.setLength(query.length() - 5);
		query.append(")");
		if (qQuery != null) {
			query.append(" AND ");
			dollarCount = qQuery.toSql(query, dollarCount, tuple, false, true);
		}
		return clientManager.getClient(tenant, false).onItem().transformToUni(client -> {
			return client.preparedQuery(query.toString()).execute(tuple).onItem().transform(rows -> {
				List<Map<String, Object>> result = Lists.newArrayList();
				rows.forEach(row -> {
					result.add(row.getJsonObject(1).getMap());
				});

				return result;
			});
		});
	}

	public Uni<EntityMap> getEntityMap(String tenant, String qToken) {
		return clientManager.getClient(tenant, false).onItem().transformToUni(client -> {
			Tuple tuple;
			String sql;
			sql = "SELECT entity_id, remote_hosts, isDist, regEmpty, noRegEntry FROM entitymap WHERE q_token = $1 order by order_field;";
			tuple = Tuple.of(qToken);
			return client.preparedQuery(sql).execute(tuple).onItem().transformToUni(rows -> {
				if (rows.rowCount() == 0) {
					return Uni.createFrom()
							.failure(new ResponseException(ErrorType.InvalidRequest, "EntityMapId is invalid."));
				}
				Row first = rows.iterator().next();
				EntityMap entityMap = new EntityMap(qToken, first.getBoolean(2), first.getBoolean(3),
						first.getBoolean(4));
				rows.forEach(row -> {
					String entityId = row.getString(0);
					entityMap.getEntry(entityId).getRemoteHosts()
							.addAll(QueryRemoteHost.getRemoteHostsFromJson(row.getJsonArray(1).getList()));

				});
				return Uni.createFrom().item(entityMap);
			});
		});

	}

	public Uni<Tuple3<Long, EntityMap, Map<String, Map<String, Object>>>> getEntityMapAndEntitiesAndUpdateExpires(
			String tenant, String[] ids, TypeQueryTerm typeQuery, String qToken, AttrsQueryTerm attrsQuery,
			QQueryTerm qQuery, GeoQueryTerm geoQuery, ScopeQueryTerm scopeQuery, Context context, int limit, int offSet,
			DataSetIdTerm dataSetIdTerm, String join, int joinLevel, String qToken2) {
		return clientManager.getClient(tenant, false).onItem().transformToUni(client -> {
			Tuple tuple;
			String sql;

			sql = "WITH a AS (UPDATE entitymap_management SET last_access=now() WHERE q_token=$1), b as (SELECT entity_id, remote_hosts, local FROM entitymap WHERE q_token = $1 order by order_field LIMIT $2 OFFSET $3) select b.entity_id, b.remote_hosts, entity, b.isDist, b.regEmpty, b.noRegEntry FROM b left join ENTITY ON b.entity_id = ENTITY.id";
			tuple = Tuple.of(qToken, limit, offSet);

			return client.preparedQuery(sql).execute(tuple).onItem().transformToUni(rows -> {
				if (rows.rowCount() == 0) {
					return Uni.createFrom().failure(new ResponseException(ErrorType.InvalidRequest,
							"Token is invalid. Please rerequest without a token to retrieve a new token"));
				}
				Row first = rows.iterator().next();
				EntityMap entityIdList = new EntityMap(qToken, first.getBoolean(3), first.getBoolean(4),
						first.getBoolean(5));
				Map<String, Map<String, Object>> localEntities = Maps.newHashMap();
				rows.forEach(row -> {
					String entityId = row.getString(0);
					entityIdList.getEntry(entityId).getRemoteHosts()
							.addAll(QueryRemoteHost.getRemoteHostsFromJson(row.getJsonArray(1).getList()));
					JsonObject localEntity = row.getJsonObject(2);
					if (localEntity != null) {
						localEntities.put(entityId, localEntity.getMap());
					}

				});
				return Uni.createFrom().item(Tuple3.of(0l, entityIdList, localEntities));
			});
		});

	}

	public Uni<Void> runEntityMapCleanup(String cleanUpInterval) {
		return clientManager.getClient(AppConstants.INTERNAL_NULL_KEY, false).onItem().transformToUni(client -> {
			return client.preparedQuery("select tenant_id from tenant").execute().onItem().transformToUni(rows -> {
				List<Uni<Void>> cleanUpUnis = Lists.newArrayList();
				String sql = "WITH a as (SELECT q_token FROM entitymap_management WHERE entitymap_management.last_access < NOW() - INTERVAL '"
						+ cleanUpInterval + "'),"
						+ " b as (DELETE FROM entitymap WHERE entitymap.q_token IN (SELECT q_token FROM a)) "
						+ "DELETE FROM entitymap_management WHERE entitymap_management.q_token IN (SELECT q_token FROM a)";
				cleanUpUnis.add(
						client.preparedQuery(sql).execute().onItem().transformToUni(r -> Uni.createFrom().voidItem()));
				rows.forEach(row -> {
					cleanUpUnis.add(
							clientManager.getClient(row.getString(0), false).onItem().transformToUni(tenantClient -> {
								return tenantClient.preparedQuery(sql).execute().onItem()
										.transformToUni(r -> Uni.createFrom().voidItem());
							}));
				});
				return Uni.combine().all().unis(cleanUpUnis).discardItems();
			});
		});
	}

	/*
	 * public Uni<Map<String, Tuple2<Map<String, Object>, Boolean>>>
	 * queryForEntityIds(String tenant, String[] ids, TypeQueryTerm typeQuery,
	 * String idPattern, AttrsQueryTerm attrsQuery, int limit, int offset, String
	 * join, int joinLevel) { return clientManager.getClient(tenant,
	 * false).onItem().transformToUni(client -> { StringBuilder query = new
	 * StringBuilder(); int dollar = 1; Tuple tuple = Tuple.tuple();
	 * 
	 * query.append("WITH a as (SELECT ID");
	 * 
	 * query.append(" FROM ENTITY WHERE "); boolean sqlAdded = false; if (typeQuery
	 * != null) { dollar = typeQuery.toBroadSql(query, tuple, dollar); sqlAdded =
	 * true; } if (attrsQuery != null) { if (sqlAdded) { query.append(" AND "); }
	 * dollar = attrsQuery.toSql(query, tuple, dollar); sqlAdded = true; }
	 * 
	 * if (ids != null) { if (sqlAdded) { query.append(" AND "); }
	 * query.append("id IN ("); for (String id : ids) { query.append('$');
	 * query.append(dollar); query.append(','); tuple.addString(id); dollar++; }
	 * 
	 * query.setCharAt(query.length() - 1, ')'); sqlAdded = true; } if (idPattern !=
	 * null) { if (sqlAdded) { query.append(" AND "); } query.append("id ~ $");
	 * query.append(dollar); tuple.addString(idPattern); dollar++; sqlAdded = true;
	 * }
	 * 
	 * query.append(" ORDER BY createdAt), b as (SELECT a.ID FROM a limit $");
	 * query.append(dollar); tuple.addInteger(limit); dollar++;
	 * query.append(" offset $"); query.append(dollar); tuple.addInteger(offset);
	 * dollar++; query.append("), c as (SELECT ENTITY.ID, "); if (attrsQuery !=
	 * null) { dollar = attrsQuery.toSqlConstructEntity(query, tuple, dollar, null);
	 * } else { query.append("ENTITY.ENTITY"); }
	 * query.append(" as ENTITY FROM b left join ENTITY on b.ID = ENTITY.ID)  ");
	 * query.append("SELECT a.ID, c.ENTITY FROM a left outer join c on a.ID = c.ID"
	 * );
	 * 
	 * String queryString = query.toString(); logger.debug("SQL REQUEST: " +
	 * queryString); logger.debug("SQL TUPLE: " + tuple.deepToString()); return
	 * client.preparedQuery(queryString).execute(tuple).onItem().transform(rows -> {
	 * Map<String, Tuple2<Map<String, Object>, Boolean>> result = Maps.newHashMap();
	 * rows.forEach(row -> { String id = row.getString(0); JsonObject entity =
	 * row.getJsonObject(1); Boolean parent = row.getBoolean(2); Map<String, Object>
	 * entityMap = null; if (entity != null) { entityMap = entity.getMap(); } if
	 * (parent == null) { parent = true; } result.put(id, Tuple2.of(entityMap,
	 * parent));
	 * 
	 * }); return result; }); }).onFailure().recoverWithUni(e -> { if (e instanceof
	 * PgException pge) { logger.debug(pge.getPosition()); if
	 * (pge.getSqlState().equals(AppConstants.INVALID_REGULAR_EXPRESSION)) { return
	 * Uni.createFrom() .failure(new ResponseException(ErrorType.BadRequestData,
	 * "Invalid regular expression")); } if
	 * (pge.getSqlState().equals(AppConstants.INVALID_GEO_QUERY)) { return
	 * Uni.createFrom().failure(new ResponseException(ErrorType.BadRequestData,
	 * "Invalid geo query. " + pge.getErrorMessage())); } } return
	 * Uni.createFrom().failure(e); }); }
	 */
	public Uni<Tuple2<EntityCache, EntityMap>> queryForEntityIdsAndEntitiesRegEmpty(String tenant, String[] ids,
			TypeQueryTerm typeQuery, String idPattern, AttrsQueryTerm attrsQuery, QQueryTerm qQuery,
			GeoQueryTerm geoQuery, ScopeQueryTerm scopeQuery, Context context, int limit, int offset,
			DataSetIdTerm dataSetIdTerm, String join, int joinLevel, String qToken, PickTerm pickTerm,
			OmitTerm omitTerm) {
		return clientManager.getClient(tenant, false).onItem().transformToUni(client -> {
			StringBuilder query = new StringBuilder();
			int dollar = 1;
			Tuple tuple = Tuple.tuple();
			boolean doJoin = (join != null && joinLevel > 0);
			query.append("WITH a as (SELECT ID");

			query.append(" FROM ENTITY WHERE ");
			boolean sqlAdded = false;
			if (typeQuery != null) {
				dollar = typeQuery.toSql(query, tuple, dollar);
				sqlAdded = true;
			}
			if (attrsQuery != null) {
				if (sqlAdded) {
					query.append(" AND ");
				}
				dollar = attrsQuery.toSql(query, tuple, dollar);
				sqlAdded = true;
			}
			if (pickTerm != null) {
				if (sqlAdded) {
					query.append(" AND ");
				}
				dollar = pickTerm.toSql(query, tuple, dollar);
				sqlAdded = true;
			}
			if (omitTerm != null) {
				if (sqlAdded) {
					query.append(" AND ");
				}
				dollar = omitTerm.toSql(query, tuple, dollar);
				sqlAdded = true;
			}
			if (geoQuery != null) {
				if (sqlAdded) {
					query.append(" AND ");
				}
				dollar = geoQuery.toSql(query, tuple, dollar);
				sqlAdded = true;
			}

			if (qQuery != null) {
				if (sqlAdded) {
					query.append(" AND ");
				}
				dollar = qQuery.toSql(query, dollar, tuple, false, false);
				sqlAdded = true;
			}
			if (ids != null) {
				if (sqlAdded) {
					query.append(" AND ");
				}
				query.append("id IN (");
				for (String id : ids) {
					query.append('$');
					query.append(dollar);
					query.append(',');
					tuple.addString(id);
					dollar++;
				}

				query.setCharAt(query.length() - 1, ')');
				sqlAdded = true;
			}
			if (idPattern != null) {
				if (sqlAdded) {
					query.append(" AND ");
				}
				query.append("id ~ $");
				query.append(dollar);
				tuple.addString(idPattern);
				dollar++;
				sqlAdded = true;
			}
			if (scopeQuery != null) {
				query.append(" AND ");
				scopeQuery.toSql(query);
			}
			query.append(" ORDER BY createdAt), b as (SELECT a.ID FROM a limit $");
			query.append(dollar);
			tuple.addInteger(limit);
			dollar++;
			query.append(" offset $");
			query.append(dollar);
			tuple.addInteger(offset);
			dollar++;
			query.append("), c as (SELECT ENTITY.ID, ");
			if (attrsQuery != null) {
				dollar = attrsQuery.toSqlConstructEntity(query, tuple, dollar, dataSetIdTerm);
			} else if (pickTerm != null) {
				dollar = pickTerm.toSqlConstructEntity(query, tuple, dollar, "entityAttrs", dataSetIdTerm);
			} else if (omitTerm != null) {
				dollar = omitTerm.toSqlConstructEntity(query, tuple, dollar, "entityAttrs", dataSetIdTerm);
			} else if (dataSetIdTerm != null) {
				dollar = dataSetIdTerm.toSqlConstructEntity(query, tuple, "entityAttrs", dollar);
			} else {
				query.append("ENTITY.ENTITY");
			}
			query.append(
					" as ENTITY, NULL as PARENT, ENTITY.E_TYPES AS E_TYPES FROM b left join ENTITY on b.ID = ENTITY.ID");
			if (attrsQuery == null && (dataSetIdTerm != null || omitTerm != null || pickTerm != null)) {
				query.append(", JSONB_EACH(ENTITY.ENTITY) AS entityAttrs GROUP BY ENTITY.ID");
			}
			query.append(")  ");
			String currentEntitySet = "c";
			if (doJoin) {
				int counter;
				query.append(", D0 as (SELECT * FROM ");
				query.append(currentEntitySet);
				query.append(")");
				for (counter = 0; counter < joinLevel; counter++) {
					query.append('B');
					query.append(counter + 1);
					query.append(" AS (SELECT ");
					query.append('D');
					query.append(counter);
					query.append(".ID AS ID, X.VALUE AS VALUE FROM D");
					query.append(counter);
					query.append(", JSONB_EACH(D");
					query.append(counter);
					query.append(".ENTITY) AS X WHERE JSONB_TYPEOF(X.VALUE) = 'array'), ");
					query.append('C');
					query.append(counter + 1);
					query.append(" AS (SELECT distinct Z ->> '@id' as link FROM B");
					query.append(counter + 1);
					query.append(", JSONB_ARRAY_ELEMENTS(B");
					query.append(counter + 1);
					query.append(
							".VALUE) AS Y, JSONB_ARRAY_ELEMENTS(Y #> '{https://uri.etsi.org/ngsi-ld/hasObject}') AS Z WHERE Y #>> '{@type,0}' = 'https://uri.etsi.org/ngsi-ld/Relationship') AND Y ? 'https://uri.etsi.org/ngsi-ld/hasObjectType', ");
					query.append('D');
					query.append(counter + 1);
					query.append(" as (SELECT E.ID as id, E.ENTITY as entity, C");
					query.append(counter + 1);
					query.append(".link as parent, E.E_TYPES as E_TYPES");

					query.append(" from C");
					query.append(counter + 1);
					query.append(" join ENTITY as E on C");
					query.append(counter + 1);
					query.append(".link = E.ID), ");
				}
				query.setLength(query.length() - 2);
				query.append(" SELECT * FROM (");
				for (int i = 0; i <= joinLevel; i++) {
					query.append("SELECT * FROM D");
					query.append(i);
					query.append(" UNION ALL ");
				}
				query.setLength(query.length() - " UNION ALL ".length());
				query.append(") as xyz group by id, entity, parent");
				currentEntitySet = "xyz";
			}

			query.append(" SELECT a.ID, ");
			query.append(currentEntitySet);
			query.append(".ENTITY, ");
			query.append(currentEntitySet);
			query.append(".parent, ");
			query.append(currentEntitySet);
			query.append(".E_TYPES");
			query.append(" FROM a left outer join ");
			query.append(currentEntitySet);
			query.append(" on a.ID = ");
			query.append(currentEntitySet);
			query.append(".ID");

			String queryString = query.toString();
			logger.debug("SQL REQUEST: " + queryString);
			logger.debug("SQL TUPLE: " + tuple.deepToString());
			return client.preparedQuery(queryString).execute(tuple).onItem().transform(rows -> {
				EntityCache resultEntities = new EntityCache();
				EntityMap resultEntityMap = new EntityMap(qToken, true, true, true);

				rows.forEach(row -> {
					String id = row.getString(0);
					JsonObject entityObj = row.getJsonObject(1);
					String parent = row.getString(2);
					String[] types = row.getArrayOfStrings(3);
					if (parent == null) {
						resultEntityMap.getEntry(id).addRemoteHost(AppConstants.DB_REMOTE_HOST);
					}

					if (entityObj != null) {
						Map<String, Object> entity = entityObj.getMap();
						for (String type : types) {
							resultEntities.setEntityIntoEntityCache(type, id, entity,
									AppConstants.DEFAULT_REMOTE_HOST_MAP);
						}

					}
				});
				return Tuple2.of(resultEntities, resultEntityMap);
			});
		}).onFailure().recoverWithUni(e -> {
			if (e instanceof PgException pge) {
				logger.debug(pge.getPosition());
				if (pge.getSqlState().equals(AppConstants.INVALID_REGULAR_EXPRESSION)) {
					return Uni.createFrom()
							.failure(new ResponseException(ErrorType.BadRequestData, "Invalid regular expression"));
				}
				if (pge.getSqlState().equals(AppConstants.INVALID_GEO_QUERY)) {
					return Uni.createFrom().failure(new ResponseException(ErrorType.BadRequestData,
							"Invalid geo query. " + pge.getErrorMessage()));
				}
			}
			return Uni.createFrom().failure(e);
		});
	}

	public Uni<Tuple2<EntityCache, EntityMap>> queryForEntityIdsAndEntitiesRegNotEmpty(String tenant, String[] ids,
			TypeQueryTerm typeQuery, String idPattern, AttrsQueryTerm attrsQuery, QQueryTerm qQuery,
			GeoQueryTerm geoQuery, ScopeQueryTerm scopeQuery, int limit, int offset, String qToken,
			boolean onlyFullEntitiesDistributed) {
		return clientManager.getClient(tenant, false).onItem().transformToUni(client -> {
			StringBuilder query = new StringBuilder();
			int dollar = 1;
			Tuple tuple = Tuple.tuple();

			query.append("WITH a as (SELECT ID");

			query.append(" FROM ENTITY WHERE ");
			boolean sqlAdded = false;
			if (typeQuery != null) {
				dollar = typeQuery.toSql(query, tuple, dollar);
				sqlAdded = true;
			}
			if (attrsQuery != null) {
				if (sqlAdded) {
					query.append(" AND ");
				}
				dollar = attrsQuery.toSql(query, tuple, dollar);
				sqlAdded = true;
			}
			if (geoQuery != null) {
				if (sqlAdded) {
					query.append(" AND ");
				}
				dollar = geoQuery.toSql(query, tuple, dollar);
				sqlAdded = true;
			}

			if (qQuery != null) {
				if (sqlAdded) {
					query.append(" AND ");
				}
				dollar = qQuery.toSql(query, dollar, tuple, true, false);
				sqlAdded = true;
			}
			if (ids != null) {
				if (sqlAdded) {
					query.append(" AND ");
				}
				query.append("id IN (");
				for (String id : ids) {
					query.append('$');
					query.append(dollar);
					query.append(',');
					tuple.addString(id);
					dollar++;
				}

				query.setCharAt(query.length() - 1, ')');
				sqlAdded = true;
			}
			if (idPattern != null) {
				if (sqlAdded) {
					query.append(" AND ");
				}
				query.append("id ~ $");
				query.append(dollar);
				tuple.addString(idPattern);
				dollar++;
				sqlAdded = true;
			}
			if (scopeQuery != null) {
				query.append(" AND ");
				scopeQuery.toSql(query);
			}
			query.append(" ORDER BY createdAt), b as (SELECT a.ID FROM a limit $");
			query.append(dollar);
			tuple.addInteger(limit);
			dollar++;
			query.append(" offset $");
			query.append(dollar);
			tuple.addInteger(offset);
			dollar++;
			query.append("), c as (SELECT ENTITY.ID, ");

			query.append("ENTITY.ENTITY");

			query.append(" as ENTITY, NULL as PARENT FROM b left join ENTITY on b.ID = ENTITY.ID)  ");
			String currentEntitySet = "c";

			query.append(" SELECT a.ID, ");
			query.append(currentEntitySet);
			query.append(".ENTITY, ");
			query.append(currentEntitySet);
			query.append(".parent FROM a left outer join ");
			query.append(currentEntitySet);
			query.append(" on a.ID = ");
			query.append(currentEntitySet);
			query.append(".ID");

			String queryString = query.toString();
			logger.debug("SQL REQUEST: " + queryString);
			logger.debug("SQL TUPLE: " + tuple.deepToString());
			return client.preparedQuery(queryString).execute(tuple).onItem().transform(rows -> {
				EntityCache resultEntities = new EntityCache();
				EntityMap resultEntityMap = new EntityMap(qToken, onlyFullEntitiesDistributed, false, true);

				rows.forEach(row -> {
					String id = row.getString(0);
					JsonObject entityObj = row.getJsonObject(1);
					String parent = row.getString(2);
					if (parent == null) {
						resultEntityMap.getEntry(id).addRemoteHost(AppConstants.DB_REMOTE_HOST);
					}

					if (entityObj != null) {
						Map<String, Object> entity = entityObj.getMap();
						List<String> types = (List<String>) entity.get(NGSIConstants.JSON_LD_TYPE);
						for (String type : types) {
							resultEntities.setEntityIntoEntityCache(type, id, entity,
									AppConstants.DEFAULT_REMOTE_HOST_MAP);
						}

					}
				});
				return Tuple2.of(resultEntities, resultEntityMap);
			});
		}).onFailure().recoverWithUni(e -> {
			if (e instanceof PgException pge) {
				logger.debug(pge.getPosition());
				if (pge.getSqlState().equals(AppConstants.INVALID_REGULAR_EXPRESSION)) {
					return Uni.createFrom()
							.failure(new ResponseException(ErrorType.BadRequestData, "Invalid regular expression"));
				}
				if (pge.getSqlState().equals(AppConstants.INVALID_GEO_QUERY)) {
					return Uni.createFrom().failure(new ResponseException(ErrorType.BadRequestData,
							"Invalid geo query. " + pge.getErrorMessage()));
				}
			}
			return Uni.createFrom().failure(e);
		});
	}

	public Uni<Tuple2<EntityCache, EntityMap>> queryForEntityIdsAndEntitiesRegNotEmptyExpectDistEntities(String tenant,
			String[] ids, TypeQueryTerm typeQuery, String idPattern, AttrsQueryTerm attrsQuery, QQueryTerm qQuery,
			GeoQueryTerm geoQuery, String join, int joinLevel, String qToken) {
		return clientManager.getClient(tenant, false).onItem().transformToUni(client -> {
			return null;
		});
	}

	public Uni<Tuple2<Object, QueryRemoteHost>> queryForEntities(String tenant, Set<String> idsForDBCall) {
		return clientManager.getClient(tenant, false).onItem().transformToUni(client -> {
			return client.preparedQuery("SELECT ENTITY FROM ENTITY WHERE id=ANY($1);")
					.execute(Tuple.of(idsForDBCall.toArray(new String[0]))).onItem().transform(rows -> {
						List<Map<String, Object>> resultEntities = new ArrayList<>(rows.size());
						rows.forEach(row -> {
							resultEntities.add(row.getJsonObject(0).getMap());
						});
						return Tuple2.of(resultEntities, AppConstants.DB_REMOTE_HOST);
					});
		});
	}

	public Uni<Tuple2<EntityCache, EntityMap>> queryForEntitiesAndEntityMapNoRegEntry(String tenant,
			AttrsQueryTerm attrsQuery, int limit, int offset, DataSetIdTerm dataSetIdTerm, String join, int joinLevel,
			String qToken, PickTerm pickTerm, OmitTerm omitTerm) {
		return clientManager.getClient(tenant, false).onItem().transformToUni(client -> {
			Tuple tuple = Tuple.tuple();
			boolean doJoin = (join != null && joinLevel > 0);
			StringBuilder query = new StringBuilder(
					"WITH A AS (UPDATE ENTITYMAP_MANAGEMENT SET LAST_ACCESS = NOW() WHERE Q_TOKEN = $1), B AS (SELECT ENTITY_ID, REMOTE_HOSTS, FULLENTITIES_DIST, REGEMPTY, NOREGENTRY FROM ENTITYMAP WHERE Q_TOKEN = $1 ORDER BY ORDER_FIELD), c as (SELECT ENTITY.ID, ");

			tuple.addString(qToken);
			int dollar = 2;

			if (attrsQuery != null) {
				dollar = attrsQuery.toSqlConstructEntity(query, tuple, dollar, dataSetIdTerm);
			} else if (pickTerm != null) {
				dollar = pickTerm.toSqlConstructEntity(query, tuple, dollar, "entityAttrs", dataSetIdTerm);
			} else if (omitTerm != null) {
				dollar = omitTerm.toSqlConstructEntity(query, tuple, dollar, "entityAttrs", dataSetIdTerm);
			} else if (dataSetIdTerm != null) {
				dollar = dataSetIdTerm.toSqlConstructEntity(query, tuple, "entityAttrs", dollar);
			} else {
				query.append("ENTITY.ENTITY");
			}
			query.append(
					" as ENTITY, NULL as PARENT, ENTITY.E_TYPES AS E_TYPES FROM b left join ENTITY on b.ENTITY_ID = ENTITY.ID");
			if (attrsQuery == null && (dataSetIdTerm != null || omitTerm != null || pickTerm != null)) {
				query.append(", JSONB_EACH(ENTITY.ENTITY) AS entityAttrs GROUP BY ENTITY.ID");
			}
			query.append(")  ");
			String currentEntitySet = "c";
			if (doJoin) {
				int counter;
				query.append(", D0 as (SELECT * FROM ");
				query.append(currentEntitySet);
				query.append(")");
				for (counter = 0; counter < joinLevel; counter++) {
					query.append('B');
					query.append(counter + 1);
					query.append(" AS (SELECT ");
					query.append('D');
					query.append(counter);
					query.append(".ID AS ID, X.VALUE AS VALUE FROM D");
					query.append(counter);
					query.append(", JSONB_EACH(D");
					query.append(counter);
					query.append(".ENTITY) AS X WHERE JSONB_TYPEOF(X.VALUE) = 'array'), ");
					query.append('C');
					query.append(counter + 1);
					query.append(" AS (SELECT distinct Z ->> '@id' as link FROM B");
					query.append(counter + 1);
					query.append(", JSONB_ARRAY_ELEMENTS(B");
					query.append(counter + 1);
					query.append(
							".VALUE) AS Y, JSONB_ARRAY_ELEMENTS(Y #> '{https://uri.etsi.org/ngsi-ld/hasObject}') AS Z WHERE Y #>> '{@type,0}' = 'https://uri.etsi.org/ngsi-ld/Relationship') AND Y ? 'https://uri.etsi.org/ngsi-ld/hasObjectType', ");
					query.append('D');
					query.append(counter + 1);
					query.append(" as (SELECT E.ID as id, E.ENTITY as entity, C");
					query.append(counter + 1);
					query.append(".link as parent, E.E_TYPES as E_TYPES");

					query.append(" from C");
					query.append(counter + 1);
					query.append(" join ENTITY as E on C");
					query.append(counter + 1);
					query.append(".link = E.ID), ");
				}
				query.setLength(query.length() - 2);
				query.append(" SELECT * FROM (");
				for (int i = 0; i <= joinLevel; i++) {
					query.append("SELECT * FROM D");
					query.append(i);
					query.append(" UNION ALL ");
				}
				query.setLength(query.length() - " UNION ALL ".length());
				query.append(") as xyz group by id, entity, parent");
				currentEntitySet = "xyz";
			}

			query.append(" SELECT b.ENTITY_ID, ");
			query.append(currentEntitySet);
			query.append(".ENTITY, ");
			query.append(currentEntitySet);
			query.append(".parent, ");
			query.append(currentEntitySet);
			query.append(".E_TYPES");
			query.append(" FROM b left outer join ");
			query.append(currentEntitySet);
			query.append(" on b.ENTITY_ID = ");
			query.append(currentEntitySet);
			query.append(".ID");

			String queryString = query.toString();
			logger.debug("SQL REQUEST: " + queryString);
			logger.debug("SQL TUPLE: " + tuple.deepToString());
			return client.preparedQuery(queryString).execute(tuple).onItem().transformToUni(rows -> {
				if (rows.rowCount() == 0) {
					return Uni.createFrom().failure(new ResponseException(ErrorType.InvalidRequest,
							"Token is invalid. Please rerequest without a token to retrieve a new token"));
				}
				EntityCache resultEntities = new EntityCache();
				EntityMap resultEntityMap = new EntityMap(qToken, true, true, true);

				rows.forEach(row -> {
					String id = row.getString(0);
					JsonObject entityObj = row.getJsonObject(1);
					String parent = row.getString(2);
					String[] types = row.getArrayOfStrings(3);
					if (parent == null) {
						resultEntityMap.getEntry(id).addRemoteHost(AppConstants.DB_REMOTE_HOST);
					}

					if (entityObj != null) {
						Map<String, Object> entity = entityObj.getMap();
						for (String type : types) {
							resultEntities.setEntityIntoEntityCache(type, id, entity,
									AppConstants.DEFAULT_REMOTE_HOST_MAP);
						}

					}
				});
				return Uni.createFrom().item(Tuple2.of(resultEntities, resultEntityMap));
			});
		}).onFailure().recoverWithUni(e -> {
			if (e instanceof PgException pge) {
				logger.debug(pge.getPosition());
				if (pge.getSqlState().equals(AppConstants.INVALID_REGULAR_EXPRESSION)) {
					return Uni.createFrom()
							.failure(new ResponseException(ErrorType.BadRequestData, "Invalid regular expression"));
				}
				if (pge.getSqlState().equals(AppConstants.INVALID_GEO_QUERY)) {
					return Uni.createFrom().failure(new ResponseException(ErrorType.BadRequestData,
							"Invalid geo query. " + pge.getErrorMessage()));
				}
			}
			return Uni.createFrom().failure(e);
		});

	}

	public Uni<Tuple2<EntityCache, EntityMap>> queryForEntitiesAndEntityMap(String tenant, AttrsQueryTerm attrsQuery,
			int limit, int offset, DataSetIdTerm dataSetIdTerm, String join, int joinLevel, String qToken,
			PickTerm pickTerm, OmitTerm omitTerm) {
		return clientManager.getClient(tenant, false).onItem().transformToUni(client -> {
			Tuple tuple = Tuple.tuple();
			StringBuilder sql = new StringBuilder(
					"WITH A AS (UPDATE ENTITYMAP_MANAGEMENT SET LAST_ACCESS = NOW() WHERE Q_TOKEN = $1), B AS (SELECT ENTITY_ID, REMOTE_HOSTS, FULLENTITIES_DIST, REGEMPTY, NOREGENTRY FROM ENTITYMAP WHERE Q_TOKEN = $1 ORDER BY ORDER_FIELD), C AS (SELECT B.ENTITY_ID, ");
			tuple.addString(qToken);
			int dollar = 2;
			boolean doJoin = (join != null && joinLevel > 0);
			if (attrsQuery != null) {
				attrsQuery.toSqlConstructEntity(sql, tuple, dollar, dataSetIdTerm);
			} else if (dataSetIdTerm != null) {
				dollar = dataSetIdTerm.toSqlConstructEntity(sql, tuple, "entityAttrs", dollar);
			} else {

				sql.append("ENTITY ");
			}
			sql.append("FROM B LEFT JOIN ENTITY ON B.ENTITY_ID = ENTITY.ID");
			if (attrsQuery == null && dataSetIdTerm != null) {
				sql.append(", jsonb_each(ENTITY) as entityAttrs");
			}
			sql.append(" LIMIT $");
			sql.append(dollar);
			dollar++;
			sql.append(" OFFSET $");
			sql.append(dollar);
			dollar++;
			sql.append(")");
			if (doJoin) {
				sql.append(", D AS");
			}
			sql.append(
					" (SELECT B.ENTITY_ID, B.REMOTE_HOSTS,B.FULLENTITIES_DIST, B.REGEMPTY, B.NOREGENTRY, C.ENTITY FROM B LEFT OUTER JOIN C ON B.ENTITY_ID=C.ENTITY_ID)");
			if (join != null && joinLevel > 0) {
				String currentEntitySet = "D";
				sql.append(", E AS (SELECT CASE WHEN D.REGEMPTY THEN ");
				int counter;
				sql.append(", D0 as (SELECT * FROM ");
				sql.append(currentEntitySet);
				sql.append(")");
				for (counter = 0; counter < joinLevel; counter++) {
					sql.append('B');
					sql.append(counter + 1);
					sql.append(" AS (SELECT ");
					sql.append('D');
					sql.append(counter);
					sql.append(".ID AS ID, X.VALUE AS VALUE FROM D");
					sql.append(counter);
					sql.append(", JSONB_EACH(D");
					sql.append(counter);
					sql.append(".ENTITY) AS X WHERE JSONB_TYPEOF(X.VALUE) = 'array'), ");
					sql.append('C');
					sql.append(counter + 1);
					sql.append(" AS (SELECT distinct Z ->> '@id' as link FROM B");
					sql.append(counter + 1);
					sql.append(", JSONB_ARRAY_ELEMENTS(B");
					sql.append(counter + 1);
					sql.append(
							".VALUE) AS Y, JSONB_ARRAY_ELEMENTS(Y #> '{https://uri.etsi.org/ngsi-ld/hasObject}') AS Z WHERE Y #>> '{@type,0}' = 'https://uri.etsi.org/ngsi-ld/Relationship') AND Y ? 'https://uri.etsi.org/ngsi-ld/hasObjectType', ");
					sql.append('D');
					sql.append(counter + 1);
					sql.append(" as (SELECT E.ID as id, E.ENTITY as entity, C");
					sql.append(counter + 1);
					sql.append(".link as parent");

					sql.append(" from C");
					sql.append(counter + 1);
					sql.append(" join ENTITY as E on C");
					sql.append(counter + 1);
					sql.append(".link = E.ID), ");
				}
				sql.setLength(sql.length() - 2);
				sql.append(" SELECT * FROM (");
				for (int i = 0; i <= joinLevel; i++) {
					sql.append("SELECT * FROM D");
					sql.append(i);
					sql.append(" UNION ALL ");
				}
				sql.setLength(sql.length() - " UNION ALL ".length());
				sql.append(") as xyz group by id, entity, parent");
				currentEntitySet = "xyz";
				sql.append(" ELSE D.* END FROM D) SELECT * FROM E");
			} else {
				sql.append("SELECT * FROM D;");
			}

			String sqlString = sql.toString();
			logger.debug(sqlString);

			return client.preparedQuery(sqlString).execute(tuple).onItem().transformToUni(rows -> {
				if (rows.rowCount() == 0) {
					return Uni.createFrom().failure(new ResponseException(ErrorType.InvalidRequest,
							"Token is invalid. Please rerequest without a token to retrieve a new token"));
				}
				Row first = rows.iterator().next();
				EntityMap entityMap = new EntityMap(qToken, first.getBoolean(3), first.getBoolean(4),
						first.getBoolean(5));
				EntityCache resultCache = new EntityCache();
				rows.forEach(row -> {
					String entityId = row.getString(0);

					entityMap.getEntry(entityId).getRemoteHosts()
							.addAll(QueryRemoteHost.getRemoteHostsFromJson(row.getJsonArray(1).getList()));
					JsonObject localEntity = row.getJsonObject(2);
					if (localEntity != null) {
						// do not handover entitymap here it gets filled before since we want all ids in
						// the map anyway
						resultCache.putEntityIntoEntityCacheAndEntityMap(localEntity.getMap(),
								AppConstants.DB_REMOTE_HOST, null);
					}

				});
				return Uni.createFrom().item(Tuple2.of(resultCache, entityMap));
			});
		});

	}

}
