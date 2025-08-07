package eu.neclab.ngsildbroker.queryhandler.repository;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import eu.neclab.ngsildbroker.commons.datatypes.terms.DataSetIdTerm;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.locationtech.spatial4j.context.SpatialContextFactory;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.io.GeoJSONReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLDService;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.EntityCache;
import eu.neclab.ngsildbroker.commons.datatypes.EntityMap;
import eu.neclab.ngsildbroker.commons.datatypes.QueryRemoteHost;
import eu.neclab.ngsildbroker.commons.datatypes.RegistrationEntry;
import eu.neclab.ngsildbroker.commons.datatypes.RemoteHost;
import eu.neclab.ngsildbroker.commons.datatypes.terms.AttrsQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.CSFQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.GeoQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.OmitTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.PickTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.QQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.ScopeQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.TypeQueryTerm;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.storage.ClientManager;
import eu.neclab.ngsildbroker.commons.tools.DBUtil;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.mutiny.tuples.Tuple3;
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

	@Inject
	JsonLDService ldService;

	@Inject
	ObjectMapper objectMapper;

	@ConfigProperty(name = "scorpio.entitymap.cleanup.ttl")
	String entityMapTTL;

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

	public Uni<Map<String, Set<String>>> getTypes(String tenantId) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {
			return client.preparedQuery(
					"select distinct a from (select unnest(e_types) as a from entity) as b;")
					.execute().onItem().transform(rows -> {
						Map<String, Set<String>> result = new HashMap<>(rows.size());
						rows.forEach(row -> {
							result.put(row.getString(0), new HashSet<>(0));
						});
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
							@SuppressWarnings("unchecked")
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
						List<?> types = row.getJsonArray(1).getList();
						List<?> attribTypes = row.getJsonArray(0).getList();

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

	public Uni<List<Map<String, Object>>> getTypesWithDetails(String tenantId, boolean bbox) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {
			String sql;
			if (!bbox) {
				sql = "SELECT DISTINCT myTypes, jsonb_agg(jsonb_build_object('" + NGSIConstants.JSON_LD_ID
						+ "', myAttr)) from entity, jsonb_array_elements(ENTITY -> '@type') as myTypes, jsonb_object_keys((ENTITY - ARRAY['"
						+ NGSIConstants.JSON_LD_TYPE + "', '" + NGSIConstants.JSON_LD_ID + "', '"
						+ NGSIConstants.NGSI_LD_CREATED_AT + "','" + NGSIConstants.NGSI_LD_MODIFIED_AT
						+ "'])) as myAttr group by myTypes";
			} else {
				sql = "SELECT DISTINCT myTypes, jsonb_agg(jsonb_build_object('" + NGSIConstants.JSON_LD_ID
						+ "', myAttr)), ST_XMin(ST_SetSRID(ST_Extent(ENTITY.LOCATION), 4326)) AS xmin,"
						+ "ST_XMax(ST_SetSRID(ST_Extent(ENTITY.LOCATION), 4326)) AS xmax,"
						+ "ST_YMin(ST_SetSRID(ST_Extent(ENTITY.LOCATION), 4326)) AS ymin,"
						+ "ST_YMax(ST_SetSRID(ST_Extent(ENTITY.LOCATION), 4326)) AS ymax from entity, jsonb_array_elements(ENTITY -> '@type') as myTypes, jsonb_object_keys((ENTITY - ARRAY['"
						+ NGSIConstants.JSON_LD_TYPE + "', '" + NGSIConstants.JSON_LD_ID + "', '"
						+ NGSIConstants.NGSI_LD_CREATED_AT + "','" + NGSIConstants.NGSI_LD_MODIFIED_AT
						+ "'])) as myAttr group by myTypes";
			}
			return client.preparedQuery(sql).execute().onItem().transform(rows -> {
				List<Map<String, Object>> result = Lists.newArrayList();
				rows.forEach(row -> {
					Map<String, Object> resultEntry = Maps.newHashMap();
					resultEntry.put(NGSIConstants.JSON_LD_ID, row.getString(0));
					resultEntry.put(NGSIConstants.JSON_LD_TYPE, Lists.newArrayList(NGSIConstants.NGSI_LD_ENTITY_TYPE));
					Map<String, String> tmp = Maps.newHashMap();
					tmp.put(NGSIConstants.JSON_LD_ID, row.getString(0));
					resultEntry.put(NGSIConstants.NGSI_LD_TYPE_NAME, Lists.newArrayList(tmp));
					resultEntry.put(NGSIConstants.NGSI_LD_ATTRIBUTE_NAMES,
							row.getJsonArray(1).getList().stream().distinct().toList());
					if (bbox) {
						Double xmin = row.getDouble(2);
						Double xmax = row.getDouble(3);
						Double ymin = row.getDouble(4);
						Double ymax = row.getDouble(5);
						if (xmin != null && xmax != null && ymax != null && ymin != null) {
							List<Map<String, List<Map<String, Double>>>> bboxEntry = List
									.of(Map.of(NGSIConstants.JSON_LD_LIST,
											List.of(Map.of(NGSIConstants.JSON_LD_VALUE, xmin),
													Map.of(NGSIConstants.JSON_LD_VALUE, ymin),
													Map.of(NGSIConstants.JSON_LD_VALUE, xmax),
													Map.of(NGSIConstants.JSON_LD_VALUE, ymax))));
							resultEntry.put(NGSIConstants.BBOX, bboxEntry);

						}
					}
					result.add(resultEntry);
				});
				return result;
			});
		});
	}

	public Uni<Map<String, Object>> getType(String tenantId, String type, boolean bbox) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {
			Tuple tuple = Tuple.tuple();
			tuple.addArrayOfString(new String[] { type });
			String sql;
			if (bbox) {
				sql = "with a as (select x as id, entity -> x as data from entity, jsonb_object_keys(entity.entity) as x where e_types && $1::text[] and x not in ('"
						+ NGSIConstants.JSON_LD_ID + "', '" + NGSIConstants.JSON_LD_TYPE + "', '"
						+ NGSIConstants.NGSI_LD_CREATED_AT + "', '" + NGSIConstants.NGSI_LD_MODIFIED_AT
						+ "')), b as (SELECT count(entity.id) as mycount, ST_XMin(ST_SetSRID(ST_Extent(ENTITY.LOCATION), 4326)) AS xmin,"
						+ "ST_XMax(ST_SetSRID(ST_Extent(ENTITY.LOCATION), 4326)) AS xmax,"
						+ "ST_YMin(ST_SetSRID(ST_Extent(ENTITY.LOCATION), 4326)) AS ymin,"
						+ "ST_YMax(ST_SetSRID(ST_Extent(ENTITY.LOCATION), 4326)) AS ymax FROM entity where e_types && $1::text[]) "
						+ "select b.mycount, a.id, jsonb_agg(distinct jsonb_build_object('" + NGSIConstants.JSON_LD_ID
						+ "', x#>'{" + NGSIConstants.JSON_LD_TYPE
						+ ",0}')), b.xmin, b.xmax, b.ymin, b.ymax from b, a, jsonb_array_elements(a.data) as x group by a.id, b.mycount, b.xmin, b.xmax, b.ymin, b.ymax;";
			} else {
				sql = "with a as (select x as id, entity -> x as data from entity, jsonb_object_keys(entity.entity) as x where e_types && $1::text[] and x not in ('"
						+ NGSIConstants.JSON_LD_ID + "', '" + NGSIConstants.JSON_LD_TYPE + "', '"
						+ NGSIConstants.NGSI_LD_CREATED_AT + "', '" + NGSIConstants.NGSI_LD_MODIFIED_AT
						+ "')), b as (SELECT count(entity.id) as mycount FROM entity where e_types && $1::text[]) "
						+ "select b.mycount, a.id, jsonb_agg(distinct jsonb_build_object('" + NGSIConstants.JSON_LD_ID
						+ "', x#>'{" + NGSIConstants.JSON_LD_TYPE
						+ ",0}')) from b, a, jsonb_array_elements(a.data) as x group by a.id, b.mycount;";
			}
			return client.preparedQuery(sql).execute(tuple).onItem().transform(rows -> {
				Map<String, Object> result = Maps.newHashMap();
				if (rows.size() == 0) {
					return result;
				}
				long count = 0;
				RowIterator<Row> it = rows.iterator();
				List<Map<String, Object>> attrDetails = Lists.newArrayList();
				Double xmin = null;
				Double xmax = null;
				Double ymin = null;
				Double ymax = null;
				while (it.hasNext()) {
					Row row = it.next();
					count = row.getLong(0);
					if (bbox) {
						xmin = row.getDouble(3);
						xmax = row.getDouble(4);
						ymin = row.getDouble(5);
						ymax = row.getDouble(6);
					}
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
				if (bbox) {
					if (xmin != null && xmax != null && ymax != null && ymin != null) {
						List<Map<String, List<Map<String, Double>>>> bboxEntry = List
								.of(Map.of(NGSIConstants.JSON_LD_LIST,
										List.of(Map.of(NGSIConstants.JSON_LD_VALUE, xmin),
												Map.of(NGSIConstants.JSON_LD_VALUE, ymin),
												Map.of(NGSIConstants.JSON_LD_VALUE, xmax),
												Map.of(NGSIConstants.JSON_LD_VALUE, ymax))));
						result.put(NGSIConstants.BBOX, bboxEntry);

					}
				}
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

	public Uni<Map<RemoteHost, Map<String, Set<String>>>> getRemoteTypesAndEndpoints(
			String tenantId) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {
			return client.preparedQuery(
					"SELECT C.c_id, C.endpoint, C.tenant_id, C.e_type, ARRAY_AGG(C.e_prop) || ARRAY_AGG(C.e_rel), C.retrieveEntityTypeDetails, C.retrieveEntityTypes, C.csource_alias FROM CSOURCEINFORMATION AS C GROUP BY C.endpoint, C.e_type, C.tenant_id, C.retrieveEntityTypeDetails, C.retrieveEntityTypes, c.c_id, c.csource_alias")
					.execute().onItem().transform(rows -> {
						Map<RemoteHost, Map<String, Set<String>>> result = Maps
								.newHashMap();
						rows.forEach(row -> {
							String cId = row.getString(0);
							String host = row.getString(1);
							String tenant_id = row.getString(2);
							String type = row.getString(3);
							String[] attrs = row.getArrayOfStrings(4);
							boolean retrieveEntityTypeDetails = row.getBoolean(5);
							boolean retrieveEntityTypes = row.getBoolean(6);
							String sourceAlias = row.getString(7);
							if (tenant_id == null) {
								tenant_id = AppConstants.INTERNAL_NULL_KEY;
							}
							RemoteHost rHost = new RemoteHost(host, tenant_id, null, cId, retrieveEntityTypes,
									retrieveEntityTypeDetails, -1, false, false, sourceAlias);

							Map<String, Set<String>> hostEntry = result
									.get(rHost);
							if (hostEntry == null) {
								hostEntry = Maps.newHashMap();
								result.put(rHost, hostEntry);
							}

							if (type == null) {
								type = "none";
							}
							hostEntry.put(type, Sets.newHashSet(attrs));

						});
						return result;
					});

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
		return DBUtil.getAllRegistries(clientManager, ldService,
				"SELECT cs_id, c_id, e_id, e_id_p, e_type, e_prop, e_rel, ST_AsGeoJSON(i_location), scopes, EXTRACT(MILLISECONDS FROM expires), endpoint, tenant_id, headers, reg_mode, createEntity, updateEntity, appendAttrs, updateAttrs, deleteAttrs, deleteEntity, createBatch, upsertBatch, updateBatch, deleteBatch, upsertTemporal, appendAttrsTemporal, deleteAttrsTemporal, updateAttrsTemporal, deleteAttrInstanceTemporal, deleteTemporal, mergeEntity, replaceEntity, replaceAttrs, mergeBatch, retrieveEntity, queryEntity, queryBatch, retrieveTemporal, queryTemporal, retrieveEntityTypes, retrieveEntityTypeDetails, retrieveEntityTypeInfo, retrieveAttrTypes, retrieveAttrTypeDetails, retrieveAttrTypeInfo, createSubscription, updateSubscription, retrieveSubscription, querySubscription, deleteSubscription, queryEntityMap, createEntityMap, updateEntityMap, deleteEntityMap, retrieveEntityMap, csource_Alias FROM csourceinformation WHERE queryentity OR querybatch OR retrieveentity OR retrieveentitytypes OR retrieveentitytypedetails OR retrieveentitytypeinfo OR retrieveattrtypes OR retrieveattrtypedetails OR retrieveattrtypeinfo",
				logger);

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
	public Uni<Void> storeEntityMap(String tenant, String qToken, EntityMap entityMap) {

		return clientManager.getClient(tenant, false).onItem().transformToUni(client -> {
			return client.getConnection().onItem().transformToUni(conn -> {
				// map_id text NOT NULL,
				// query_checksum text NOT NULL,
				// entity_id text NOT NULL,
				// remote_query text,
				// csourceid text NOT NULL,
				// last_access timestamp without time zone NOT NULL,
				// expires_at timestamp without time zone NOT NULL
				logger.debug("storeEntityMap");
				String deleteSql = "DELETE FROM entitymap WHERE map_id = $1";
				return conn.preparedQuery(deleteSql).execute(Tuple.of(qToken)).onItem().transformToUni(ignored -> {
					String sql = "INSERT INTO entitymap VALUES ($1, $2, $3, $4, $5, now() + interval '" + entityMapTTL
							+ "', now())";
					String mapId = entityMap.getId();
					String checksum = entityMap.getQueryCheckSum();
					List<Tuple> batch = Lists.newArrayList();
					for (Entry<String, Set<String>> entry : entityMap.getEntityId2CSourceIds().entrySet()) {
						for (String cId : entry.getValue()) {
							String remoteHost;
							QueryRemoteHost tmp = entityMap.getRemoteHost(cId);
							if (tmp == null) {
								remoteHost = null;
							} else {
								try {
									remoteHost = objectMapper.writeValueAsString(tmp);
								} catch (JsonProcessingException e) {
									continue;
								}
							}
							batch.add(Tuple.of(mapId, checksum, entry.getKey(), remoteHost, cId));
						}
					}
					return conn.preparedQuery(sql).executeBatch(batch).onItem().transformToUni(ignoredToo -> {
						return conn.close();
					});

				});
			});
		});
	}

	public Uni<List<Map<String, Object>>> getEntities(String tenant, Map<Set<String>, Set<String>> types2EntityIds,
			QQueryTerm qQuery) {
		Tuple tuple = Tuple.tuple();
		StringBuilder query = new StringBuilder("SELECT id, entity FROM entity WHERE (");
		int dollarCount = 1;
		for (Entry<Set<String>, Set<String>> entry : types2EntityIds.entrySet()) {
			Set<String> types = entry.getKey();
			Set<String> entityIds = entry.getValue();
			query.append("(id = ANY(");
			query.append('$');
			query.append(dollarCount);
			dollarCount++;
			tuple.addArrayOfString(entityIds.toArray(new String[0]));
			query.append(')');
			query.append(" AND e_types && $");
			query.append(dollarCount);
			dollarCount++;
			tuple.addArrayOfString(types.toArray(new String[0]));
			query.append(") OR ");

		}
		query.setLength(query.length() - 4);
		query.append(")");
		if (qQuery != null) {
			query.append(" AND ");
			dollarCount = qQuery.toSql(query, dollarCount, tuple, false, true);
		}
		String sql = query.toString();
		// logger.debug("SQL Request: " + sql);
		// logger.debug("Tuple: " + tuple.deepToString());
		try {
			logger.debug(JsonUtils.toPrettyString(types2EntityIds));
		} catch (JsonGenerationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return clientManager.getClient(tenant, false).onItem().transformToUni(client -> {
			return client.preparedQuery(sql).execute(tuple).onItem().transform(rows -> {
				List<Map<String, Object>> result = Lists.newArrayList();
				rows.forEach(row -> {
					result.add(row.getJsonObject(1).getMap());
				});

				return result;
			});
		});
	}

	public Uni<Map<String, Object>> getEntityMap(String tenant, String qToken) {
		logger.debug("getEntityMap");
		return clientManager.getClient(tenant, false).onItem().transformToUni(client -> {

			String sql = "SELECT * from entitymap WHERE map_id=$1";
			return client.preparedQuery(sql).execute(Tuple.of(qToken)).onItem().transformToUni(rows -> {
				if (rows.rowCount() == 0) {
					return Uni.createFrom()
							.failure(new ResponseException(ErrorType.InvalidRequest, "EntityMapId is invalid."));
				}
				Row first = rows.iterator().next();
				Map<String, Object> tmp = first.getJsonObject(0).getMap();
				tmp.put(NGSIConstants.ID, qToken);
				tmp.put(NGSIConstants.EXPIRES_AT, first.getLocalDateTime(1));
				return Uni.createFrom().item(tmp);
			});
		});

	}

	public Uni<Void> runEntityMapCleanup(String cleanUpInterval) {
		logger.debug("runEntityMapCleanup");
		return clientManager.getClient(AppConstants.INTERNAL_NULL_KEY,
				false).onItem().transformToUni(client -> {
					return client.preparedQuery("select tenant_id from tenant").execute().onItem()
							.transformToUni(rows -> {
								List<Uni<Void>> cleanUpUnis = Lists.newArrayList();
								String sql = "DELETE FROM entitymap WHERE last_access < NOW() - INTERVAL '" +
										cleanUpInterval + "'";
								cleanUpUnis.add(
										client.preparedQuery(sql).execute().onItem()
												.transformToUni(r -> Uni.createFrom().voidItem()));
								rows.forEach(row -> {
									cleanUpUnis.add(
											clientManager.getClient(row.getString(0),
													false).onItem().transformToUni(tenantClient -> {
														return tenantClient.preparedQuery(sql).execute().onItem()
																.transformToUni(r -> Uni.createFrom().voidItem());
													}));
								});
								return Uni.combine().all().unis(cleanUpUnis).discardItems();
							});
				});
		// return Uni.createFrom().voidItem();

	}

	private void generateJoinQuery(StringBuilder query, StringBuilder followUp, int joinLevel, boolean localOnly) {
		query.append(", ");
		followUp.append(", ");
		int counter;
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
			query.append(" AS (SELECT DISTINCT (CASE WHEN Y #>> '{");
			query.append(NGSIConstants.JSON_LD_TYPE);
			query.append(",0}' = '");
			query.append(NGSIConstants.NGSI_LD_RELATIONSHIP);
			query.append("' THEN Z ->> '");
			query.append(NGSIConstants.JSON_LD_ID);
			query.append("' WHEN Y #>> '{");
			query.append(NGSIConstants.JSON_LD_TYPE);
			query.append(",0}' = '");
			query.append(NGSIConstants.NGSI_LD_LISTRELATIONSHIP);
			query.append("' THEN Z #>> '{");
			query.append(NGSIConstants.NGSI_LD_HAS_OBJECT);
			query.append(",0,");
			query.append(NGSIConstants.JSON_LD_ID);
			query.append("}' ELSE NULL END) AS LINK");
			if (!localOnly) {
				query.append(", ARRAY_AGG(E_TYPES ->> '");
				query.append(NGSIConstants.JSON_LD_ID);
				query.append("') AS ET");
			} else {
				query.append(", null AS ET");
			}
			query.append(" FROM B");
			query.append(counter + 1);
			query.append(", JSONB_ARRAY_ELEMENTS(B");
			query.append(counter + 1);
			query.append(".VALUE) AS Y, JSONB_ARRAY_ELEMENTS(CASE WHEN Y #>> '{");
			query.append(NGSIConstants.JSON_LD_TYPE);
			query.append(",0}' = '");
			query.append(NGSIConstants.NGSI_LD_RELATIONSHIP);
			query.append("' THEN Y #> '{");
			query.append(NGSIConstants.NGSI_LD_HAS_OBJECT);
			query.append("}' WHEN Y #>> '{");
			query.append(NGSIConstants.JSON_LD_TYPE);
			query.append(",0}' = '");
			query.append(NGSIConstants.NGSI_LD_LISTRELATIONSHIP);
			query.append("' THEN Y #> '{");
			query.append(NGSIConstants.NGSI_LD_HAS_OBJECT_LIST);
			query.append(",0,");
			query.append(NGSIConstants.JSON_LD_LIST);
			query.append("}' ELSE null END) AS Z");
			// if (!localOnly) {
			query.append(", JSONB_ARRAY_ELEMENTS(Y -> '");
			query.append(NGSIConstants.NGSI_LD_OBJECT_TYPE);
			query.append("') AS E_TYPES");
			// } else {
			// query.append(", null AS E_TYPES");
			// }
			query.append(" WHERE Y #>> '{");
			query.append(NGSIConstants.JSON_LD_TYPE);
			query.append(",0}' = ANY('{");
			query.append(NGSIConstants.NGSI_LD_RELATIONSHIP);
			query.append(",");
			query.append(NGSIConstants.NGSI_LD_LISTRELATIONSHIP);
			query.append("}')");
			if (!localOnly) {
				query.append(" AND Y ? '");
				query.append(NGSIConstants.NGSI_LD_OBJECT_TYPE);
				query.append('\'');
			}
			query.append(" GROUP BY y.value, z.value), ");

			query.append('D');
			query.append(counter + 1);
			query.append(
					" as (SELECT E.ID as id, E.ENTITY as entity, FALSE as parent, E.E_TYPES as E_TYPES, null::jsonb");
			query.append(" from C");
			query.append(counter + 1);
			query.append(" LEFT JOIN ENTITY as E on C");
			query.append(counter + 1);
			query.append(".link = E.ID");
			if (!localOnly) {
				query.append(" WHERE C");
				query.append(counter + 1);
				query.append(".ET && E.E_TYPES");
			}
			query.append("), ");

			followUp.append('B');
			followUp.append(counter + 1);
			followUp.append(" AS (SELECT ");
			followUp.append('D');
			followUp.append(counter);
			followUp.append(".ID AS ID, X.VALUE AS VALUE FROM D");
			followUp.append(counter);
			followUp.append(", JSONB_EACH(D");
			followUp.append(counter);
			followUp.append(".ENTITY) AS X WHERE JSONB_TYPEOF(X.VALUE) = ''array''), ");
			followUp.append('C');
			followUp.append(counter + 1);
			followUp.append(" AS (SELECT DISTINCT (CASE WHEN Y #>> ''{");
			followUp.append(NGSIConstants.JSON_LD_TYPE);
			followUp.append(",0}'' = ''");
			followUp.append(NGSIConstants.NGSI_LD_RELATIONSHIP);
			followUp.append("'' THEN Z ->> ''");
			followUp.append(NGSIConstants.JSON_LD_ID);
			followUp.append("'' WHEN Y #>> ''{");
			followUp.append(NGSIConstants.JSON_LD_TYPE);
			followUp.append(",0}'' = ''");
			followUp.append(NGSIConstants.NGSI_LD_LISTRELATIONSHIP);
			followUp.append("'' THEN Z #>> ''{");
			followUp.append(NGSIConstants.NGSI_LD_HAS_OBJECT);
			followUp.append(",0,");
			followUp.append(NGSIConstants.JSON_LD_ID);
			followUp.append("}'' ELSE NULL END) AS LINK");
			// if (!localOnly) {
			followUp.append(", ARRAY_AGG(E_TYPES ->> ''");
			followUp.append(NGSIConstants.JSON_LD_ID);
			followUp.append("'') AS ET");
			// } else {
			// followUp.append(", null AS ET");
			// }
			followUp.append(" FROM B");
			followUp.append(counter + 1);
			followUp.append(", JSONB_ARRAY_ELEMENTS(B");
			followUp.append(counter + 1);
			followUp.append(".VALUE) AS Y, JSONB_ARRAY_ELEMENTS(CASE WHEN Y #>> ''{");
			followUp.append(NGSIConstants.JSON_LD_TYPE);
			followUp.append(",0}'' = ''");
			followUp.append(NGSIConstants.NGSI_LD_RELATIONSHIP);
			followUp.append("'' THEN Y #> ''{");
			followUp.append(NGSIConstants.NGSI_LD_HAS_OBJECT);
			followUp.append("}'' WHEN Y #>> ''{");
			followUp.append(NGSIConstants.JSON_LD_TYPE);
			followUp.append(",0}'' = ''");
			followUp.append(NGSIConstants.NGSI_LD_LISTRELATIONSHIP);
			followUp.append("'' THEN Y #> ''{");
			followUp.append(NGSIConstants.NGSI_LD_HAS_OBJECT_LIST);
			followUp.append(",0,");
			followUp.append(NGSIConstants.JSON_LD_LIST);

			followUp.append("}'' ELSE null END) AS Z");
			if (!localOnly) {
				followUp.append(", JSONB_ARRAY_ELEMENTS(Y -> ''");
				followUp.append(NGSIConstants.NGSI_LD_OBJECT_TYPE);
				followUp.append("'') AS E_TYPES");
			} else {
				followUp.append(", null AS E_TYPES");
			}
			followUp.append(" WHERE Y #>> ''{");
			followUp.append(NGSIConstants.JSON_LD_TYPE);
			followUp.append(",0}'' = ANY(''{");
			followUp.append(NGSIConstants.NGSI_LD_RELATIONSHIP);
			followUp.append(",");
			followUp.append(NGSIConstants.NGSI_LD_LISTRELATIONSHIP);
			followUp.append("}'')");
			if (!localOnly) {
				followUp.append(" AND Y ? ''");
				followUp.append(NGSIConstants.NGSI_LD_OBJECT_TYPE);
				followUp.append("''");
			}
			followUp.append(" GROUP BY y.value, z.value), ");

			followUp.append('D');
			followUp.append(counter + 1);
			followUp.append(" as (SELECT E.ID as id, E.ENTITY as entity, FALSE as parent, E.E_TYPES as E_TYPES, null");
			followUp.append(" from C");
			followUp.append(counter + 1);
			followUp.append(" LEFT JOIN ENTITY as E on C");
			followUp.append(counter + 1);
			followUp.append(".link = E.ID");
			if (!localOnly) {
				followUp.append(" WHERE C");
				followUp.append(counter + 1);
				followUp.append(".ET && E.E_TYPES");
			}
			followUp.append("), ");
		}

		query.append(" JOINENTITIES AS (");
		followUp.append(" JOINENTITIES AS (");
		for (int i = 1; i <= joinLevel; i++) {
			query.append("SELECT * FROM D");
			query.append(i);
			query.append(" UNION ALL ");

			followUp.append("SELECT * FROM D");
			followUp.append(i);
			followUp.append(" UNION ALL ");
		}
		query.setLength(query.length() - " UNION ALL ".length());
		query.append(")");
		followUp.setLength(followUp.length() - " UNION ALL ".length());
		followUp.append(")");

	}

	private void generateJoinQuery(StringBuilder query, int joinLevel, boolean localOnly) {
		query.append(", ");
		int counter;
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
			query.append(" AS (SELECT DISTINCT (CASE WHEN Y #>> '{");
			query.append(NGSIConstants.JSON_LD_TYPE);
			query.append(",0}' = '");
			query.append(NGSIConstants.NGSI_LD_RELATIONSHIP);
			query.append("' THEN Z ->> '");
			query.append(NGSIConstants.JSON_LD_ID);
			query.append("' WHEN Y #>> '{");
			query.append(NGSIConstants.JSON_LD_TYPE);
			query.append(",0}' = '");
			query.append(NGSIConstants.NGSI_LD_LISTRELATIONSHIP);
			query.append("' THEN Z #>> '{");
			query.append(NGSIConstants.NGSI_LD_HAS_OBJECT);
			query.append(",0,");
			query.append(NGSIConstants.JSON_LD_ID);
			query.append("}' ELSE NULL END) AS LINK");
			if (!localOnly) {
				query.append(", ARRAY_AGG(E_TYPES ->> '");
				query.append(NGSIConstants.JSON_LD_ID);
				query.append("') AS ET");
			} else {
				query.append(", null AS ET");
			}
			query.append(" FROM B");
			query.append(counter + 1);
			query.append(", JSONB_ARRAY_ELEMENTS(B");
			query.append(counter + 1);
			query.append(".VALUE) AS Y, JSONB_ARRAY_ELEMENTS(CASE WHEN Y #>> '{");
			query.append(NGSIConstants.JSON_LD_TYPE);
			query.append(",0}' = '");
			query.append(NGSIConstants.NGSI_LD_RELATIONSHIP);
			query.append("' THEN Y #> '{");
			query.append(NGSIConstants.NGSI_LD_HAS_OBJECT);
			query.append("}' WHEN Y #>> '{");
			query.append(NGSIConstants.JSON_LD_TYPE);
			query.append(",0}' = '");
			query.append(NGSIConstants.NGSI_LD_LISTRELATIONSHIP);
			query.append("' THEN Y #> '{");
			query.append(NGSIConstants.NGSI_LD_HAS_OBJECT_LIST);
			query.append(",0,");
			query.append(NGSIConstants.JSON_LD_LIST);
			query.append("}' ELSE null END) AS Z");
			// if (!localOnly) {
			query.append(", JSONB_ARRAY_ELEMENTS(Y -> '");
			query.append(NGSIConstants.NGSI_LD_OBJECT_TYPE);
			query.append("') AS E_TYPES");
			// } else {
			// query.append(", null AS E_TYPES");
			// }
			query.append(" WHERE Y #>> '{");
			query.append(NGSIConstants.JSON_LD_TYPE);
			query.append(",0}' = ANY('{");
			query.append(NGSIConstants.NGSI_LD_RELATIONSHIP);
			query.append(",");
			query.append(NGSIConstants.NGSI_LD_LISTRELATIONSHIP);
			query.append("}')");
			if (!localOnly) {
				query.append(" AND Y ? '");
				query.append(NGSIConstants.NGSI_LD_OBJECT_TYPE);
				query.append('\'');
			}
			query.append(" GROUP BY y.value, z.value), ");

			query.append('D');
			query.append(counter + 1);
			query.append(
					" as (SELECT E.ID as id, E.ENTITY as entity, FALSE as parent, E.E_TYPES as E_TYPES, null::bigint, null, null");
			query.append(" from C");
			query.append(counter + 1);
			query.append(" LEFT JOIN ENTITY as E on C");
			query.append(counter + 1);
			query.append(".link = E.ID");
			if (!localOnly) {
				query.append(" WHERE C");
				query.append(counter + 1);
				query.append(".ET && E.E_TYPES");
			}
			query.append("), ");

		}

		query.append(" JOINENTITIES AS (");

		for (int i = 1; i <= joinLevel; i++) {
			query.append("SELECT * FROM D");
			query.append(i);
			query.append(" UNION ALL ");

		}
		query.setLength(query.length() - " UNION ALL ".length());
		query.append(")");

	}

	private int generateWherePart(StringBuilder query, int dollar, Tuple tuple,
			List<Tuple3<String[], TypeQueryTerm, String>> idsAndTypeAndIdPattern, AttrsQueryTerm attrsQuery,
			QQueryTerm qQuery, GeoQueryTerm geoQuery, ScopeQueryTerm scopeQuery, Context context, int limit, int offset,
			DataSetIdTerm dataSetIdTerm, String join, int joinLevel, String qToken, PickTerm pickTerm,
			OmitTerm omitTerm, String queryChecksum, boolean splitEntities,
			boolean regEmptyOrNoRegEntryAndNoLinkedQuery, boolean noRootLevelRegEntryAndLinkedQuery,
			String typePattern, boolean localOnly) {

		if (typePattern != null) {
			query.append("EXISTS (SELECT TRUE FROM UNNEST(E_TYPES) AS E_TYPE WHERE E_TYPE ~ $");
			query.append(dollar);
			dollar++;
			tuple.addString(typePattern);
			query.append(") AND ");
		}
		boolean sqlAdded = false;
		if (idsAndTypeAndIdPattern != null) {
			sqlAdded = true;
			query.append('(');

			for (Tuple3<String[], TypeQueryTerm, String> t : idsAndTypeAndIdPattern) {
				TypeQueryTerm typeQuery = t.getItem2();
				String[] ids = t.getItem1();
				String idPattern = t.getItem3();
				boolean tSqlAdded = false;
				query.append('(');

				if (typeQuery != null) {
					if (regEmptyOrNoRegEntryAndNoLinkedQuery || noRootLevelRegEntryAndLinkedQuery
							|| !splitEntities) {
						dollar = typeQuery.toSql(query, tuple, dollar);
					} else {
						dollar = typeQuery.toBroadSql(query, tuple, dollar);
					}

					tSqlAdded = true;
				}
				if (ids != null) {
					if (tSqlAdded) {
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
					tSqlAdded = true;
				}
				if (idPattern != null) {
					if (tSqlAdded) {
						query.append(" AND ");
					}
					query.append("id ~ $");
					query.append(dollar);
					tuple.addString(idPattern);
					dollar++;
					tSqlAdded = true;
				}
				query.append(") OR ");

			}
			query.setLength(query.length() - 4);

			query.append(')');

		} else if (splitEntities && !regEmptyOrNoRegEntryAndNoLinkedQuery) {
			if (attrsQuery != null) {
				if (sqlAdded) {
					query.append(" AND ");
				}
				dollar = attrsQuery.toSql(query, tuple, dollar);
				sqlAdded = true;
			} else if (pickTerm != null) {
				if (sqlAdded) {
					query.append(" AND ");
				}
				dollar = pickTerm.toSql(query, tuple, dollar);
				sqlAdded = true;
			} else if (omitTerm != null) {
				if (sqlAdded) {
					query.append(" AND ");
				}
				dollar = omitTerm.toSql(query, tuple, dollar);
				sqlAdded = true;
			} else if (qQuery != null) {
				if (sqlAdded) {
					query.append(" AND ");
				}
				dollar = qQuery.toSql(query, dollar, tuple, splitEntities, localOnly);
				sqlAdded = true;
			} else if (geoQuery != null) {
				if (sqlAdded) {
					query.append(" AND ");
				}
				dollar = geoQuery.toSql(query, tuple, dollar);
				sqlAdded = true;
			}

		}

		if (regEmptyOrNoRegEntryAndNoLinkedQuery || noRootLevelRegEntryAndLinkedQuery || !splitEntities) {
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
				dollar = qQuery.toSql(query, dollar, tuple,
						!regEmptyOrNoRegEntryAndNoLinkedQuery && splitEntities, localOnly);
				sqlAdded = true;
			}
			if (dataSetIdTerm != null) {
				if (sqlAdded) {
					query.append(" AND ");
				}
				dollar = dataSetIdTerm.toSql(query, tuple, dollar, pickTerm, omitTerm,
						attrsQuery);
				sqlAdded = true;
			}
		}

		if (regEmptyOrNoRegEntryAndNoLinkedQuery || noRootLevelRegEntryAndLinkedQuery || !splitEntities) {
			if (scopeQuery != null) {
				query.append(" AND ");
				scopeQuery.toSql(query);
			}
		}
		char[] checkArray = new char[AppConstants.CHAR_ARRAY_WHERE.length];
		query.getChars(query.length() - checkArray.length, query.length(), checkArray, 0);
		if (Arrays.equals(checkArray, AppConstants.CHAR_ARRAY_WHERE)) {
			query.setLength(query.length() - checkArray.length);
		}
		query.append(
				" ORDER BY createdAt");
		return dollar;

	}

	public Uni<Tuple2<EntityCache, EntityMap>> newQuery(String tenant,
			List<Tuple3<String[], TypeQueryTerm, String>> idsAndTypeAndIdPattern, AttrsQueryTerm attrsQuery,
			QQueryTerm qQuery, GeoQueryTerm geoQuery, ScopeQueryTerm scopeQuery, Context context, int limit, int offset,
			DataSetIdTerm dataSetIdTerm, String join, int joinLevel, String qToken, PickTerm pickTerm,
			OmitTerm omitTerm, String queryChecksum, boolean splitEntities,
			boolean regEmptyOrNoRegEntryAndNoLinkedQuery, boolean noRootLevelRegEntryAndLinkedQuery, String typePattern,
			boolean localOnly, boolean forceEntitymapCreation, boolean tokenProvided) {
		return clientManager.getClient(tenant, false).onItem().transformToUni(client -> {
			StringBuilder query = new StringBuilder();
			Tuple tuple = Tuple.tuple();
			int dollar;
			boolean doJoin = (join != null && joinLevel > 0);
			if (!forceEntitymapCreation
					&& (regEmptyOrNoRegEntryAndNoLinkedQuery || noRootLevelRegEntryAndLinkedQuery || localOnly)) {
				query.append(
						"WITH D0 AS (SELECT ID, ENTITY, TRUE as PARENT, ENTITY.E_TYPES AS E_TYPES, count(*) over() as list_size, null, null FROM ENTITY WHERE ");
				dollar = 1;
				dollar = generateWherePart(query, dollar, tuple, idsAndTypeAndIdPattern, attrsQuery, qQuery, geoQuery,
						scopeQuery,
						context, limit, offset, dataSetIdTerm, join, joinLevel, qToken, pickTerm, omitTerm,
						queryChecksum,
						splitEntities, regEmptyOrNoRegEntryAndNoLinkedQuery, noRootLevelRegEntryAndLinkedQuery,
						typePattern,
						localOnly);
				query.append(" limit $");
				query.append(dollar);
				dollar++;
				query.append(" offset $");
				query.append(dollar);
				query.append(')');
				dollar++;
				tuple.addInteger(limit);
				tuple.addInteger(offset);
				;
			} else {
				query.append("WITH a AS (");
				if (tokenProvided) {
					query.append("UPDATE entitymap SET expires_at = now() + interval '");
					query.append(entityMapTTL);
					query.append(
							"' last_access = now() WHERE map_id=$1 RETURNING query_checksum, entity_id as id, remote_query, csourceid), validation AS (SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM a) THEN 1 / 0 WHEN EXISTS (SELECT 1 FROM a WHERE query_checksum != $2) THEN 1 / 0 ELSE 1 END AS result)");
					tuple.addString(qToken);
					tuple.addString(queryChecksum);
					dollar = 3;
				} else {
					dollar = 1;
					query.append("SELECT ID, null as remote_query, '@none' as csourceid FROM ENTITY WHERE ");
					dollar = generateWherePart(query, dollar, tuple, idsAndTypeAndIdPattern, attrsQuery, qQuery,
							geoQuery,
							scopeQuery, context, limit, offset, dataSetIdTerm, join, joinLevel, qToken, pickTerm,
							omitTerm,
							queryChecksum, splitEntities, regEmptyOrNoRegEntryAndNoLinkedQuery,
							noRootLevelRegEntryAndLinkedQuery, typePattern, localOnly);
					query.append(
							"), b as (INSERT INTO entitymap (map_id, query_checksum , entity_id, remote_query, csourceid, last_access, expires_at) SELECT $");
					query.append(dollar);
					dollar++;
					tuple.addString(qToken);
					query.append(", $");
					query.append(dollar);
					dollar++;
					if (queryChecksum == null) {

						System.err.println("query");
					}
					tuple.addString(queryChecksum);
					query.append(", id, null, '@none', now(), now() + interval '");
					query.append(entityMapTTL);
					query.append("' FROM a)");

				}
				query.append(
						",D0 as (SELECT ENTITY.ID, ENTITY.ENTITY, TRUE as PARENT, ENTITY.E_TYPES AS E_TYPES, null::bigint as SIZE, a.remote_query, a.csourceid FROM a left join ENTITY on a.ID = ENTITY.ID");
				query.append(" limit $");
				query.append(dollar);
				dollar++;
				query.append(" offset $");
				query.append(dollar);
				query.append(')');
				dollar++;
				tuple.addInteger(limit);
				tuple.addInteger(offset);
			}

			if (doJoin) {
				generateJoinQuery(query, joinLevel, localOnly);
			}
			if (!forceEntitymapCreation
					&& (regEmptyOrNoRegEntryAndNoLinkedQuery || noRootLevelRegEntryAndLinkedQuery || localOnly)) {
				query.append(" SELECT * FROM D0");
			} else {
				query.append(
						" SELECT a.ID, D0.ENTITY, D0.PARENT, D0.E_TYPES, D0.SIZE, a.remote_query, a.csourceid FROM a left join D0 on a.ID = D0.ID");
			}

			if (doJoin) {
				query.append(" UNION ALL (SELECT * FROM JOINENTITIES)");
			}

			return client.preparedQuery(query.toString()).execute(tuple).onItem().transform(rows -> {
				EntityMap entityMap = new EntityMap(qToken, splitEntities, regEmptyOrNoRegEntryAndNoLinkedQuery,
						noRootLevelRegEntryAndLinkedQuery);
				EntityCache entityCache = new EntityCache();
				entityMap.setQueryCheckSum(queryChecksum);
				RowIterator<Row> it = rows.iterator();
				if (!forceEntitymapCreation
						&& (regEmptyOrNoRegEntryAndNoLinkedQuery || noRootLevelRegEntryAndLinkedQuery || localOnly)) {
					entityMap.setId(AppConstants.ENTITYMAP_IGNORE);
					while (it.hasNext()) {
						Row row = it.next();
						// a.ID, D0.ENTITY, D0.PARENT, D0.E_TYPES, D0.SIZE, a.remote_query, a.csourceid
						String id = row.getString(0);
						JsonObject entityObj = row.getJsonObject(1);
						boolean parent = row.getBoolean(2);
						Integer size = row.getInteger(4);
						if (parent) {
							entityMap.addEntry(id, NGSIConstants.JSON_LD_NONE, null);
						}
						if (entityObj != null) {
							Map<String, Object> entity = entityObj.getMap();
							entityCache.setEntityIntoEntityCache(id, entity, NGSIConstants.JSON_LD_NONE);
							if (parent) {
								if (attrsQuery != null) {
									attrsQuery.calculateEntity(entity);
								} else if (pickTerm != null) {
									pickTerm.calculateEntity(entity, false, null, null, false);
								} else if (omitTerm != null) {
									omitTerm.calculateEntity(entity, false, null, null, false);
								} else if (dataSetIdTerm != null) {
									dataSetIdTerm.calculateEntity(entity);
								}
							}
						}
						if (size != null) {
							entityMap.setManualSize(size);
						}
					}
				} else {
					while (it.hasNext()) {
						Row row = it.next();
						// a.ID, D0.ENTITY, D0.PARENT, D0.E_TYPES, D0.SIZE, a.remote_query, a.csourceid

						String id = row.getString(0);
						JsonObject entityObj = row.getJsonObject(1);
						if (row.getBoolean(2) == null) {
							System.out.println(query.toString());
						}
						boolean parent = row.getBoolean(2);
						String csourceId = row.getString(6);
						String remoteQuery = row.getString(7);

						QueryRemoteHost queryRemoteHost;
						if (remoteQuery == null) {
							queryRemoteHost = null;
						} else {
							try {
								queryRemoteHost = objectMapper.readValue(remoteQuery, QueryRemoteHost.class);
							} catch (JsonProcessingException e) {
								e.printStackTrace();
								continue;
							}
						}
						if (parent) {
							entityMap.addEntry(id, csourceId, queryRemoteHost);
						}
						if (entityObj != null) {
							Map<String, Object> entity = entityObj.getMap();
							entityCache.setEntityIntoEntityCache(id, entity, csourceId);
						}
					}
				}

				return Tuple2.of(entityCache, entityMap);
			}).onFailure().recoverWithUni(e -> {

				if (e instanceof PgException pgE) {
					// abusing division by zero for handling invalid entitymap requests
					if (pgE.getErrorCode() == 22012) {
						return newQuery(tenant, idsAndTypeAndIdPattern, attrsQuery, qQuery, geoQuery, scopeQuery,
								context, limit, offset, dataSetIdTerm, join, joinLevel, qToken, pickTerm, omitTerm,
								queryChecksum, splitEntities, regEmptyOrNoRegEntryAndNoLinkedQuery,
								noRootLevelRegEntryAndLinkedQuery, typePattern, localOnly, forceEntitymapCreation,
								false);
					}
				}
				logger.debug(query.toString());
				logger.debug(tuple.deepToString());

				return Uni.createFrom().failure(e);
			});
		});
	}

	public Uni<Tuple2<List<Map<String, Object>>, QueryRemoteHost>> queryForEntities(String tenant,
			Set<String> idsForDBCall) {
		return clientManager.getClient(tenant, false).onItem().transformToUni(client -> {
			return client.preparedQuery("SELECT ENTITY FROM ENTITY WHERE id=ANY($1);")
					.execute(Tuple.of(idsForDBCall.toArray(new String[0]))).onItem().transform(rows -> {
						List<Map<String, Object>> resultEntities = new ArrayList<>(rows.size());
						rows.forEach(row -> {
							Map<String, Object> entity = row.getJsonObject(0).getMap();
							entity.put(AppConstants.REG_MODE_KEY, 1);
							resultEntities.add(entity);
						});
						return Tuple2.of(resultEntities, AppConstants.DB_REMOTE_HOST);
					});
		});
	}

	public Uni<Void> deleteEntityMap(String tenant, String entityMapId) {
		logger.debug("deleteEntityMap");
		return clientManager.getClient(tenant, false).onItem().transformToUni(client -> {
			return client.preparedQuery("DELETE FROM ENTITYMAP WHERE map_id=$1 RETURNING distinct map_id")
					.execute(Tuple.of(entityMapId))
					.onItem().transformToUni(rows -> {
						if (rows.rowCount() == 0) {
							return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound));
						}
						return Uni.createFrom().voidItem();
					});
		});
	}

	public Uni<Void> updateEntityMap(String tenant, String entityMapId, String expiresAt) {
		return clientManager.getClient(tenant, false).onItem().transformToUni(client -> {
			return client.preparedQuery("UPDATE ENTITYMAP SET EXPIRES_AT=$1 WHERE id=$2 RETURNING distinct map_id")
					.execute(Tuple.of(SerializationTools.localDateTimeFormatter(expiresAt), entityMapId)).onItem()
					.transformToUni(rows -> {
						if (rows.rowCount() == 0) {
							return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound));
						}
						return Uni.createFrom().voidItem();
					});
		});
	}

	public Uni<Map<String, Set<String>>> getTypesWithDetails(String tenantId) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {
			String sql = "SELECT DISTINCT unnest(e_types), jsonb_object_keys(ENTITY) from entity;";
			return client.preparedQuery(
					sql)
					.execute().onItem().transform(rows -> {
						Map<String, Set<String>> result = new HashMap<>();
						Set<String> attrs = Sets.newHashSet();
						RowIterator<Row> it = rows.iterator();
						while (it.hasNext()) {
							Row row = it.next();
							String type = row.getString(0);
							attrs.add(row.getString(1));
							if (type != null) {
								result.put(type, attrs);
								attrs = Sets.newHashSet();
							}
						}
						return result;
					});
		});
	}

	public static void main(String[] args) {
		String sql = "SELECT DISTINCT myTypes, array_agg(myAttr) from entity, jsonb_array_elements(ENTITY -> '@type') as myTypes, jsonb_object_keys((ENTITY - ARRAY['"
				+ NGSIConstants.JSON_LD_TYPE + "', '" + NGSIConstants.JSON_LD_ID + "', '"
				+ NGSIConstants.NGSI_LD_CREATED_AT + "','" + NGSIConstants.NGSI_LD_MODIFIED_AT
				+ "'])) as myAttr group by myTypes";
		System.out.println(sql);
	}

}
