package eu.neclab.ngsildbroker.queryhandler.repository;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import com.google.common.collect.Sets;
import com.google.common.collect.Table;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.RegistrationEntry;
import eu.neclab.ngsildbroker.commons.datatypes.RemoteHost;
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
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.mutiny.tuples.Tuple4;
import io.vertx.mutiny.core.MultiMap;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowIterator;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.Tuple;

@Singleton
public class QueryDAO {

	@Inject
	ClientManager clientManager;

	GeoJSONReader geoReader = new GeoJSONReader(JtsSpatialContext.GEO, new SpatialContextFactory());

	private static Logger logger = LoggerFactory.getLogger(QueryDAO.class);

	public Uni<Map<String, Object>> getEntity(String entityId, String tenantId, AttrsQueryTerm attrsQuery) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {
			String sql = "";
			sql += "SELECT ENTITY";

			Tuple tuple = Tuple.tuple();
			if (attrsQuery != null) {
				sql += "-$1 FROM ENTITY WHERE ID=$2";
				tuple.addArrayOfString(attrsQuery.getAttrs().toArray(new String[0]));
			} else {
				sql += " FROM ENTITY WHERE ID=$1";
			}
			tuple.addString(entityId);
			return client.preparedQuery(sql).execute(tuple).onItem().transformToUni(t -> {
				if (t.rowCount() == 0) {
					return Uni.createFrom().item(new HashMap<String, Object>());
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

	public Uni<RowSet<Row>> queryLocalOnly(String tenantId, Set<String> ids, TypeQueryTerm typeQuery, String idPattern,
			AttrsQueryTerm attrsQuery, QQueryTerm qQuery, GeoQueryTerm geoQuery, ScopeQueryTerm scopeQuery,
			LanguageQueryTerm langQuery, int limit, int offSet, boolean count) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {
			StringBuilder query = new StringBuilder();
			int dollar = 1;
			Tuple tuple = Tuple.tuple();
			if (count && limit == 0) {
				query.append("SELECT COUNT(ENTITY) ");
			} else if (count) {
				query.append("SELECT ENTITY");
				if (attrsQuery != null) {
					query.append("-$");
					query.append(dollar);
					dollar++;
					tuple.addArrayOfString(attrsQuery.getAttrs().toArray(new String[0]));
				}
				query.append(", COUNT(*)");
			} else {
				query.append("SELECT ENTITY");
				if (attrsQuery != null) {
					query.append("-$");
					query.append(dollar);
					dollar++;
					tuple.addArrayOfString(attrsQuery.getAttrs().toArray(new String[0]));
				}
			}
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

			query.append(" LIMIT ");
			query.append(limit);
			query.append(" OFFSET ");
			query.append(offSet);
			query.append(';');
			System.out.println(query.toString());
			System.out.println(tuple.deepToString());
			return client.preparedQuery(query.toString()).execute(tuple);
		});
	}

	public Uni<Map<String, Object>> getTypes(String tenantId) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {
			return client
					.preparedQuery(
							"SELECT DISTINCT myTypes from entity, jsonb_array_elements(ENTITY -> '@type') as myTypes")
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
							tmp.put(NGSIConstants.NGSI_LD_TYPE_NAMES, types);
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
						tmp = Maps.newHashMap();
						tmp.put(NGSIConstants.JSON_LD_VALUE, count);
						result.put(NGSIConstants.NGSI_LD_ATTRIBUTE_COUNT, Lists.newArrayList(tmp));
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
							resultEntry.put(NGSIConstants.NGSI_LD_ATTRIBUTE_NAMES, row.getJsonArray(1).getList());
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
							+ "')), b as (SELECT count(entity.id) as mycount FROM entity) select b.mycount, a.id, jsonb_agg(distinct jsonb_build_object('"
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
					attribDetail.put(NGSIConstants.NGSI_LD_ATTRIBUTE_NAME, Lists.newArrayList(tmp));
					attribDetail.put(NGSIConstants.NGSI_LD_ATTRIBUTE_TYPES, row.getJsonArray(2).getList());
					attrDetails.add(attribDetail);
				}
				result.put(NGSIConstants.NGSI_LD_ATTRIBUTE_DETAILS, attrDetails);
				Map<String, Long> countMap = Maps.newHashMap();
				countMap.put(NGSIConstants.VALUE, count);
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
					"SELECT array_agg(distinct C.e_type) FROM CSOURCEINFORMATION AS C WHERE C.retrieveAttrTypes=false")
					.execute().onItem().transform(rows -> {
						if (rows.size() == 0) {
							return new String[0];
						}
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

	public Uni<RowSet<Row>> getRemoteSourcesForAttribsWithDetails(String tenantId) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {
			return client.preparedQuery(
					"SELECT C.endpoint, C.tenant_id, c.headers, c.reg_mode FROM CSOURCEINFORMATION AS C WHERE C.retrieveAttrTypeDetails=true")
					.execute();
		});
	}

	public Uni<RowSet<Row>> getRemoteSourcesForAttrib(String tenantId, String attrib) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {
			return client.preparedQuery(
					"SELECT C.endpoint, C.tenant_id, c.headers, c.reg_mode FROM CSOURCEINFORMATION AS C WHERE C.retrieveAttrTypeInfo=true AND (C.e_prop is NULL OR C.e_prop=$1) AND (C.e_rel is NULL OR C.e_rel=$1)")
					.execute(Tuple.of(attrib));
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
									result.put(tenant, row.getString(1),
											DBUtil.getRegistrationEntry(row, tenant, logger));
								}
							}
							return result;
						});
					});
		});

	}

}
