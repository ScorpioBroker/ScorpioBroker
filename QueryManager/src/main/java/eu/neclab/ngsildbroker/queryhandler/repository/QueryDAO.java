package eu.neclab.ngsildbroker.queryhandler.repository;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import eu.neclab.ngsildbroker.commons.datatypes.EntityMap;
import eu.neclab.ngsildbroker.commons.datatypes.EntityMapEntry;
import eu.neclab.ngsildbroker.commons.datatypes.QueryRemoteHost;
import eu.neclab.ngsildbroker.commons.datatypes.RegistrationEntry;
import eu.neclab.ngsildbroker.commons.datatypes.terms.AttrsQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.CSFQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.GeoQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.LanguageQueryTerm;
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

	public Uni<Map<String, Object>> getEntity(String entityId, String tenantId, AttrsQueryTerm attrsQuery) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {
			Tuple tuple = Tuple.tuple();
			int dollar = 1;
			StringBuilder query = new StringBuilder("SELECT ");
			if (attrsQuery != null) {
				dollar = attrsQuery.toSqlConstructEntity(query, tuple, dollar);
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
			LanguageQueryTerm langQuery, int limit, int offSet, boolean count, DataSetIdTerm dataSetIdTerm) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {
			StringBuilder query = new StringBuilder();
			int dollar = 1;
			Tuple tuple = Tuple.tuple();
			query.append("with a as(");
			if (count && limit == 0 && dataSetIdTerm == null) {
				query.append("SELECT COUNT(ENTITY)");
			} else {
				query.append("SELECT ");
				if (attrsQuery != null) {
					dollar = attrsQuery.toSqlConstructEntity(query, tuple, dollar);
				} else {
					query.append(" ENTITY ");
				}
				query.append(" as entity, id");
			}
			query.append(" FROM ENTITY WHERE ");
			if(typeQuery == null && attrsQuery == null && geoQuery == null && qQuery == null
					&& ids == null && idPattern == null && scopeQuery == null){
				query.append("TRUE ");
			}
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
				dollar = qQuery.toSql(query, dollar, tuple);
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
			if (!(count && limit == 0 && dataSetIdTerm==null)) {
				query.append(" GROUP BY ENTITY,id");
			}
			if (limit != 0) {
				query.append(" LIMIT ");
				query.append(limit);
				query.append(" OFFSET ");
				query.append(offSet);
			}
			query.append(")");
			if(dataSetIdTerm!=null){
				query.append(",");
				dollar = dataSetIdTerm.toSql(query,tuple,dollar,"a");
				if(count && limit==0){
					query.append("select count(x.entity) from x");
				}else {
					query.append("select x.entity, (select count(x.entity) from x),x.id from x");
				}
			}
			else{
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
			return client
					.preparedQuery("SELECT DISTINCT myTypes from (SELECT to_jsonb(unnest(e_types)) as myTypes from entity UNION ALL SELECT to_jsonb(e_type) as myTypes from csourceinformation) as NA;")
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

	public Uni<Tuple2<Map<String, Map<String, Object>>, List<String>>> queryForEntityIds(String tenant, String[] ids,
																						 TypeQueryTerm typeQuery, String idPattern, AttrsQueryTerm attrsQuery, QQueryTerm qQuery,
																						 GeoQueryTerm geoQuery, ScopeQueryTerm scopeQuery, Context context, int limit, int offset, DataSetIdTerm dataSetIdTerm) {
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
				dollar = qQuery.toSql(query, dollar, tuple);
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
				dollar = attrsQuery.toSqlConstructEntity(query, tuple, dollar);
			} else {
					query.append("ENTITY.ENTITY");
			}
			query.append(" as ENTITY FROM b left join ENTITY on b.ID = ENTITY.ID)  ");
			if(dataSetIdTerm!=null){
				query.append(", ");
				dollar = dataSetIdTerm.toSql(query,tuple,dollar,"c");
				query.append("SELECT a.ID, x.ENTITY FROM a left outer join x on a.ID = x.ID");
			}
			else{
				query.append("SELECT a.ID, c.ENTITY FROM a left outer join c on a.ID = c.ID");
			}
			String queryString = query.toString();
			logger.debug("SQL REQUEST: " + queryString);
			logger.debug("SQL TUPLE: " + tuple.deepToString());
			return client.preparedQuery(queryString).execute(tuple).onItem().transform(rows -> {
				List<String> entityIds = Lists.newArrayList();
				Map<String, Map<String, Object>> entities = Maps.newHashMap();
				;
				rows.forEach(row -> {
					String id = row.getString(0);
					JsonObject entity = row.getJsonObject(1);
					entityIds.add(id);
					if (entity != null) {
						entities.put(id, entity.getMap());
					}

				});
				return Tuple2.of(entities, entityIds);
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

	public Uni<Void> storeEntityMap(String tenant, String qToken, EntityMap entityMap) {

		if (entityMap.getEntityList().isEmpty()) {
			return Uni.createFrom().voidItem();
		}
		List<Tuple> batch = Lists.newArrayList();
//			"q_token" text NOT NULL,
//		    "entity_id" text,
//			"remote_hosts" jsonb,
//			"order_field" numeric NOT NULL
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
			count++;
			batch.add(tuple);
		}
		return clientManager.getClient(tenant, false).onItem().transformToUni(client -> {
			return client.preparedQuery("INSERT INTO entitymap VALUES ($1, $2, $3, $4)").executeBatch(batch).onItem()
					.transformToUni(r -> {
						return client.preparedQuery(
								"INSERT INTO entitymap_management VALUES ($1, now()) ON CONFLICT(q_token) DO UPDATE SET last_access=now()")
								.execute(Tuple.of(qToken)).onItem().transformToUni(r2 -> Uni.createFrom().voidItem());
					});
		});
	}

	public Uni<Map<String, Map<String, Object>>> getEntities(String tenant, List<String> entityIds,
			AttrsQueryTerm attrsQuery) {
		Tuple tuple = Tuple.tuple();
		StringBuilder query = new StringBuilder("SELECT id, entity");
		int dollarCount = 1;
		if (attrsQuery != null) {
			if (attrsQuery != null) {
				query.append("-$1");
				tuple.addArrayOfString(attrsQuery.getAttrs().toArray(new String[0]));
				dollarCount++;
			}
		}
		query.append(" FROM entity WHERE id in (");

		for (String id : entityIds) {
			query.append('$');
			query.append(dollarCount);
			query.append(',');
			dollarCount++;
			tuple.addString(id);
		}
		query.setCharAt(query.length() - 1, ')');
		return clientManager.getClient(tenant, false).onItem().transformToUni(client -> {
			return client.preparedQuery(query.toString()).execute(tuple).onItem().transform(rows -> {
				Map<String, Map<String, Object>> result = Maps.newHashMap();
				rows.forEach(row -> {
					result.put(row.getString(0), row.getJsonObject(1).getMap());
				});

				return result;
			});
		});
	}

	public Uni<Tuple3<Long, EntityMap, Map<String, Map<String, Object>>>> getEntityMap(String tenant, String qToken,
			int limit, int offSet, boolean count) {
		return clientManager.getClient(tenant, false).onItem().transformToUni(client -> {
			return client.preparedQuery(
					"WITH a AS (UPDATE entitymap_management SET last_access=now() WHERE q_token=$1), b as (SELECT entity_id, remote_hosts FROM entitymap WHERE q_token = $1 order by order_field LIMIT $2 OFFSET $3) select b.entity_id, b.remote_hosts, entity FROM b left join ENTITY ON b.entity_id = ENTITY.id")
					.execute(Tuple.of(qToken, limit, offSet)).onItem().transformToUni(rows -> {
						if (rows.rowCount() == 0) {
							return Uni.createFrom().failure(new ResponseException(ErrorType.InvalidRequest,
									"Token is invalid. Please rerequest without a token to retrieve a new token"));
						}
						EntityMap entityIdList = new EntityMap();
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

}
