package eu.neclab.ngsildbroker.historyquerymanager.repository;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.jsonldjava.core.JsonLdConsts;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.RegistrationEntry;
import eu.neclab.ngsildbroker.commons.datatypes.results.QueryResult;
import eu.neclab.ngsildbroker.commons.datatypes.terms.AggrTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.AttrsQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.GeoQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.QQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.ScopeQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.TemporalQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.TypeQueryTerm;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.storage.ClientManager;
import eu.neclab.ngsildbroker.commons.tools.DBUtil;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowIterator;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.Tuple;

@Singleton
public class HistoryDAO {

	private static Logger logger = LoggerFactory.getLogger(HistoryDAO.class);

	private final String TIMESTAMP_FORMAT = "'YYYY-MM-DDThh24:MI:SS.usZ'";

	@Inject
	ClientManager clientManager;

	/**
	 * 
	 * @param tenant     * @param entityId
	 * @param attrsQuery
	 * @param aggrQuery
	 * @param tempQuery
	 * @param lang
	 * @param lastN
	 * @return a single row with a single column containing the constructed entity.
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public Uni<Map<String, Object>> retrieveEntity(String tenant, String entityId, AttrsQueryTerm attrsQuery,
			AggrTerm aggrQuery, TemporalQueryTerm tempQuery, String lang, int lastN) {
		Tuple2<String, Tuple> t;
		try {
			t = getSqlInput(new String[] { entityId }, null, null, attrsQuery, null, tempQuery, aggrQuery, null, null,
					lastN, 1, 0, false);
		} catch (ResponseException e) {
			return Uni.createFrom().failure(e);
		}
		String sqlString = t.getItem1();
		Tuple tuple = t.getItem2();
		return clientManager.getClient(tenant, false).onItem().transformToUni(client -> {
			return client.preparedQuery(sqlString).execute(tuple).onItem().transform(rows -> {
				if (rows.size() == 0) {
					return new HashMap<>(0);
				}
				JsonObject json = rows.iterator().next().getJsonObject(0);
				if (json == null) {
					return new HashMap<>(0);
				}
				if (aggrQuery != null && (aggrQuery.getAggrFunctions().contains(NGSIConstants.AGGR_METH_MAX)
						|| aggrQuery.getAggrFunctions().contains(NGSIConstants.AGGR_METH_MIN))) {
					for (Entry<String, Object> entry : json.getMap().entrySet()) {
						if (NGSIConstants.ENTITY_BASE_PROPS.contains(entry.getKey())) {
							continue;
						}
						List<Map<String, List<Map<String, List>>>> tmp = (List<Map<String, List<Map<String, List>>>>) entry
								.getValue();
						for (Map<String, List<Map<String, List>>> listEntry : tmp) {

							List<Map<String, List>> maxes = listEntry.get(NGSIConstants.NGSI_LD_MAX);
							if (maxes != null) {
								for (Map<String, List> max : maxes) {
									List<Map<String, List<Map<String, Object>>>> subMaxes = max
											.get(JsonLdConsts.LIST);
									for (Map<String, List<Map<String, Object>>> subMax : subMaxes) {
										List<Map<String, Object>> realValues = subMax.get(JsonLdConsts.LIST);
										String potentialValue =  realValues.get(0)
												.get(JsonLdConsts.VALUE).toString();
										if (NumberUtils.isCreatable(potentialValue)) {
											realValues.get(0).put(JsonLdConsts.VALUE,
													NumberUtils.createNumber(potentialValue));
										}

									}
								}
							}
							List<Map<String, List>> mins = listEntry.get(NGSIConstants.NGSI_LD_MIN);
							if (mins != null) {
								for (Map<String, List> min : mins) {
									List<Map<String, List<Map<String, Object>>>> subMins = min
											.get(JsonLdConsts.LIST);
									for (Map<String, List<Map<String, Object>>> subMin : subMins) {
										List<Map<String, Object>> realValues = subMin.get(JsonLdConsts.LIST);
										String potentialValue = realValues.get(0)
												.get(JsonLdConsts.VALUE).toString();
										if (NumberUtils.isCreatable(potentialValue)) {
											realValues.get(0).put(JsonLdConsts.VALUE,
													NumberUtils.createNumber(potentialValue));
										}

									}
								}
							}
						}
					}
				}
				return json.getMap();

			});
		});

	}

	public Uni<Table<String, String, List<RegistrationEntry>>> getAllRegistries() {
		return clientManager.getClient(AppConstants.INTERNAL_NULL_KEY, false).onItem().transformToUni(client -> {
			return client.preparedQuery("SELECT tenant_id FROM tenant").execute().onItem()
					.transformToUni(tenantRows -> {
						List<Uni<Tuple2<String, RowSet<Row>>>> unis = Lists.newArrayList();
						RowIterator<Row> it = tenantRows.iterator();
						String sql = "SELECT cs_id, c_id, e_id, e_id_p, e_type, e_prop, e_rel, ST_AsGeoJSON(i_location), scopes, EXTRACT(MILLISECONDS FROM expires), endpoint, tenant_id, headers, reg_mode, createEntity, updateEntity, appendAttrs, updateAttrs, deleteAttrs, deleteEntity, createBatch, upsertBatch, updateBatch, deleteBatch, upsertTemporal, appendAttrsTemporal, deleteAttrsTemporal, updateAttrsTemporal, deleteAttrInstanceTemporal, deleteTemporal, mergeEntity, replaceEntity, replaceAttrs, mergeBatch, retrieveEntity, queryEntity, queryBatch, retrieveTemporal, queryTemporal, retrieveEntityTypes, retrieveEntityTypeDetails, retrieveEntityTypeInfo, retrieveAttrTypes, retrieveAttrTypeDetails, retrieveAttrTypeInfo, createSubscription, updateSubscription, retrieveSubscription, querySubscription, deleteSubscription, queryEntityMap, createEntityMap, updateEntityMap, deleteEntityMap, retrieveEntityMap  FROM csourceinformation WHERE retrieveTemporal OR queryTemporal";
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
								@SuppressWarnings("unchecked")
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

	public Uni<QueryResult> query(String tenant, String[] entityIds, TypeQueryTerm typeQuery, String idPattern,
			AttrsQueryTerm attrsQuery, QQueryTerm qQuery, TemporalQueryTerm tempQuery, AggrTerm aggrQuery,
			GeoQueryTerm geoQuery, ScopeQueryTerm scopeQuery, int lastN, int limit, int offset, boolean count) {
		Tuple2<String, Tuple> t;
		try {
			t = getSqlInput(entityIds, typeQuery, idPattern, attrsQuery, qQuery, tempQuery, aggrQuery, geoQuery,
					scopeQuery, lastN, limit, offset, count);
		} catch (ResponseException e) {
			return Uni.createFrom().failure(e);
		}
		String sqlString = t.getItem1();
		Tuple tuple = t.getItem2();
		return clientManager.getClient(tenant, false).onItem().transformToUni(client -> {
			return client.preparedQuery(sqlString).execute(tuple).onItem().transform(rows -> {
				QueryResult result = new QueryResult();
				if (limit == 0 && count) {
					result.setCount(rows.iterator().next().getLong(0));
				} else {
					RowIterator<Row> it = rows.iterator();
					Row next = null;
					List<Map<String, Object>> resultData = new ArrayList<Map<String, Object>>(rows.size());
					Map<String, Object> entity;
					while (it.hasNext()) {
						next = it.next();
						if (next.getJsonObject(0) != null) {
							entity = next.getJsonObject(0).getMap();
						} else {
							entity = new HashMap<>();
						}
						if (aggrQuery != null && (aggrQuery.getAggrFunctions().contains(NGSIConstants.AGGR_METH_MAX)
								|| aggrQuery.getAggrFunctions().contains(NGSIConstants.AGGR_METH_MIN))) {
							for (Entry<String, Object> entry : entity.entrySet()) {
								if (NGSIConstants.ENTITY_BASE_PROPS.contains(entry.getKey())) {
									continue;
								}
								List<Map<String, List<Map<String, List>>>> tmp = (List<Map<String, List<Map<String, List>>>>) entry
										.getValue();
								for (Map<String, List<Map<String, List>>> listEntry : tmp) {

									List<Map<String, List>> maxes = listEntry.get(NGSIConstants.NGSI_LD_MAX);
									if (maxes != null) {
										for (Map<String, List> max : maxes) {
											List<Map<String, List<Map<String, Object>>>> subMaxes = max
													.get(JsonLdConsts.LIST);
											for (Map<String, List<Map<String, Object>>> subMax : subMaxes) {
												List<Map<String, Object>> realValues = subMax.get(JsonLdConsts.LIST);
												String potentialValue =  realValues.get(0)
														.get(JsonLdConsts.VALUE).toString();
												if (NumberUtils.isCreatable(potentialValue)) {
													realValues.get(0).put(JsonLdConsts.VALUE,
															NumberUtils.createNumber(potentialValue));
												}

											}
										}
									}
									List<Map<String, List>> mins = listEntry.get(NGSIConstants.NGSI_LD_MIN);
									if (mins != null) {
										for (Map<String, List> min : mins) {
											List<Map<String, List<Map<String, Object>>>> subMins = min
													.get(JsonLdConsts.LIST);
											for (Map<String, List<Map<String, Object>>> subMin : subMins) {
												List<Map<String, Object>> realValues = subMin.get(JsonLdConsts.LIST);
												String potentialValue = realValues.get(0)
														.get(JsonLdConsts.VALUE).toString();
												if (NumberUtils.isCreatable(potentialValue)) {
													realValues.get(0).put(JsonLdConsts.VALUE,
															NumberUtils.createNumber(potentialValue));
												}

											}
										}
									}
								}
							}
						}

						resultData.add(entity);
					}
					if (count) {
						Long resultCount = next.getLong(1);
						result.setCount(resultCount);
						long leftAfter = resultCount - (offset + limit);
						if (leftAfter < 0) {
							leftAfter = 0;
						}
						result.setResultsLeftAfter(leftAfter);
					} else {
						if (resultData.size() < limit) {
							result.setResultsLeftAfter(0l);
						} else {
							result.setResultsLeftAfter((long) limit);
						}

					}
					long leftBefore = offset;

					result.setResultsLeftBefore(leftBefore);
					result.setLimit(limit);
					result.setOffset(offset);
					result.setData(resultData);
				}

				return result;

			});
		});
	}

	private Tuple2<String, Tuple> getSqlInput(String[] entityIds, TypeQueryTerm typeQuery, String idPattern,
			AttrsQueryTerm attrsQuery, QQueryTerm qQuery, TemporalQueryTerm tempQuery, AggrTerm aggrQuery,
			GeoQueryTerm geoQuery, ScopeQueryTerm scopeQuery, int lastN, int limit, int offset, boolean count)
			throws ResponseException {
		Tuple tuple = Tuple.tuple();
		int dollarCount = 1;
		String entityInfos = "entityInfos";
		StringBuilder laterSql = new StringBuilder();
		StringBuilder sql = new StringBuilder("with entityInfos"
				+ " as (select id , e_types, temporalentity.createdat as raw_createdat, temporalentity.modifiedat as raw_modifiedat, case when scopes is null then null else getScopeEntry(scopes) end as scopes, jsonb_build_array(jsonb_build_object('@type', '"
				+ NGSIConstants.NGSI_LD_DATE_TIME + "', '@value', to_char(temporalentity.createdat, " + TIMESTAMP_FORMAT
				+ "))) as r_createdat, jsonb_build_array(jsonb_build_object('@type', '"
				+ NGSIConstants.NGSI_LD_DATE_TIME + "', '@value', to_char(temporalentity.modifiedat, "
				+ TIMESTAMP_FORMAT
				+ "))) as r_modifiedat, case when deletedat is null then null else jsonb_build_array(jsonb_build_object('@type', '"
				+ NGSIConstants.NGSI_LD_DATE_TIME + "', '@value', to_char(temporalentity.deletedat, " + TIMESTAMP_FORMAT
				+ ")))  end as r_deletedat from temporalentity where 1=1");
		if (typeQuery != null) {
			sql.append(" AND ");
			dollarCount = typeQuery.toSql(sql, tuple, dollarCount);
		}
		if (entityIds != null) {
			sql.append(" AND id IN (");
			for (String id : entityIds) {
				sql.append('$');
				sql.append(dollarCount);
				sql.append(',');
				tuple.addString(id);
				dollarCount++;
			}
			sql.setCharAt(sql.length() - 1, ')');
		}
		if (idPattern != null) {
			sql.append(" AND id ~ $");
			sql.append(dollarCount);
			dollarCount++;
			tuple.addString(idPattern);
		}
		if (scopeQuery != null) {
			scopeQuery.toSql(sql);
		}
		sql.append("), ");

		sql.append(
				"ATTRIBUTEDATA AS (SELECT ID, ATTRIBID, DATA, GEOVALUE, SCOPES, E_TYPES, R_CREATEDAT, R_MODIFIEDAT, R_DELETEDAT, MODIFIEDAT, CREATEDAT, OBSERVEDAT, DELETEDAT, RAW_CREATEDAT, RAW_MODIFIEDAT FROM ");
		sql.append("(SELECT " + entityInfos
				+ ".ID AS ID, TEAI.ATTRIBUTEID AS ATTRIBID, TEAI.DATA AS DATA, TEAI.GEOVALUE AS GEOVALUE, "
				+ entityInfos + ".SCOPES AS SCOPES, " + entityInfos + ".E_TYPES AS E_TYPES, " + entityInfos
				+ ".R_CREATEDAT AS R_CREATEDAT, " + entityInfos + ".R_MODIFIEDAT AS R_MODIFIEDAT" + ", " + entityInfos
				+ ".R_DELETEDAT AS R_DELETEDAT, MODIFIEDAT, CREATEDAT, OBSERVEDAT, DELETEDAT, " + entityInfos
				+ ".RAW_CREATEDAT AS RAW_CREATEDAT, " + entityInfos
				+ ".RAW_MODIFIEDAT AS RAW_MODIFIEDAT,  ROW_NUMBER() OVER "
				+ "(PARTITION BY TEAI.temporalentity_id, TEAI.ATTRIBUTEID ORDER BY TEAI.");

		String temporalProperty = "observedAt";
		if (tempQuery != null) {
//			switch (tempQuery.getTimeProperty()) {
//
//			case NGSIConstants.NGSI_LD_CREATED_AT:
//				temporalProperty = "createdAt";
//				break;
//			case NGSIConstants.NGSI_LD_MODIFIED_AT:
//				temporalProperty = "modifiedAt";
//				break;
//			case NGSIConstants.NGSI_LD_OBSERVED_AT:
//				temporalProperty = "observedAt";
//				break;
//			case NGSIConstants.NGSI_LD_DELETED_AT:
//				temporalProperty = "deletedAt";
//				break;
//			}
			temporalProperty = tempQuery.getTimeProperty();
		}

		sql.append(temporalProperty);
		sql.append(" DESC) AS RN FROM " + entityInfos
				+ " LEFT JOIN TEMPORALENTITYATTRINSTANCE TEAI ON TEAI.TEMPORALENTITY_ID = " + entityInfos
				+ ".ID WHERE 1=1");
		if (tempQuery != null && tempQuery.getTimerel() != null) {
			sql.append(" AND TEAI.");
			dollarCount = tempQuery.toSql(sql, tuple, dollarCount);

		}
		if (geoQuery != null) {
			sql.append(" AND ");
			dollarCount = geoQuery.toTempSql(sql, tuple, dollarCount);
		}
		if (attrsQuery != null) {
			sql.append(" AND TEAI.attributeId in (");
			for (String attr : attrsQuery.getAttrs()) {
				sql.append('$');
				sql.append(dollarCount);
				sql.append(',');
				tuple.addString(attr);
				dollarCount++;
			}
			if (qQuery != null) {
				for (String attr : qQuery.getAllAttibs()) {
					sql.append('$');
					sql.append(dollarCount);
					sql.append(',');
					tuple.addString(attr);
					dollarCount++;
				}
			}
			sql.setCharAt(sql.length() - 1, ')');
		}
		if (qQuery != null) {
			sql.append(" AND CASE ");
			dollarCount = qQuery.toTempSql(sql, laterSql, tuple, dollarCount);
			sql.append("ELSE TRUE END");
		}
		sql.append(") AS A1");
		if (aggrQuery == null) {
			sql.append(" WHERE RN <= $");
			sql.append(dollarCount);
			tuple.addInteger(lastN);
			dollarCount++;
		}

		sql.append(" ), ");
		entityInfos = "attributedata";
		if (aggrQuery != null) {
			dollarCount = attachAggrQuery(sql, aggrQuery, tempQuery, dollarCount, tuple, temporalProperty);
			entityInfos = "aggr1";
		}
		sql.append("temp1 as (select id, SCOPES, E_TYPES, R_CREATEDAT, R_MODIFIEDAT, R_DELETEDAT, attribid, ");
		if (aggrQuery != null) {
			sql.append("jsonb_set(");
		}
		sql.append("jsonb_agg(data)");
		if (aggrQuery != null) {
			sql.append(", '{0," + NGSIConstants.JSON_LD_TYPE + "}',JSONB_BUILD_ARRAY(ATTRTYPE))");
		}
		sql.append(" as data from " + entityInfos
				+ " group by id, e_types, scopes, r_createdat, r_modifiedat, r_deletedat, attribid");
		if (aggrQuery != null) {
			sql.append(", ATTRTYPE");
		}
		sql.append(")");

		entityInfos = "temp1";
		if (!laterSql.isEmpty()) {
			sql.append(
					", TEMP3 AS (SELECT ID AS FILTERED_ID FROM (SELECT ID, ARRAY_AGG(attribid) as attribs from temp1 group by id)  as x WHERE ");
			sql.append(laterSql);
			sql.append("), temp4 as (SELECT * FROM temp3 left join temp1 on temp3.FILTERED_ID = temp1.id");
			if (attrsQuery != null) {
				sql.append(" WHERE attribid in (");

				for (String attr : attrsQuery.getAttrs()) {
					sql.append('$');
					sql.append(dollarCount);
					sql.append(',');
					tuple.addString(attr);
					dollarCount++;
				}
				sql.setCharAt(sql.length() - 1, ')');
			}
			sql.append(")");
			entityInfos = "temp4";
		}

		if (count) {
			sql.append(", temp2 as (select count(distinct id) as e_count from " + entityInfos + ")");
		}
		sql.append(" select (jsonb_build_object('" + NGSIConstants.JSON_LD_ID + "', " + entityInfos + ".id, '"
				+ NGSIConstants.JSON_LD_TYPE + "', " + entityInfos + ".e_types, '" + NGSIConstants.NGSI_LD_CREATED_AT
				+ "', " + entityInfos + ".r_createdat, '" + NGSIConstants.NGSI_LD_MODIFIED_AT + "', " + entityInfos
				+ ".r_modifiedat) || jsonb_object_agg(" + entityInfos + ".attribid, " + entityInfos
				+ ".data) FILTER (WHERE " + entityInfos + ".data is not null)) || (case when " + entityInfos
				+ ".r_deletedat is null then '{}'::jsonb else jsonb_build_object('" + NGSIConstants.NGSI_LD_DELETED_AT
				+ "', " + entityInfos + ".r_deletedat) end) || (case when " + entityInfos
				+ ".scopes is null then '{}'::jsonb else jsonb_build_object('" + NGSIConstants.NGSI_LD_SCOPE + "', "
				+ entityInfos + ".scopes) end)");
		if (count) {
			sql.append(", temp2.e_count");
		}
		sql.append(" FROM " + entityInfos);
		if (count) {
			sql.append(",temp2");
		}
		sql.append(" group by " + entityInfos + ".id, " + entityInfos + ".e_types, " + entityInfos + ".scopes, "
				+ entityInfos + ".r_createdat, " + entityInfos + ".r_modifiedat, " + entityInfos + ".r_deletedat");
		if (count) {
			sql.append(",temp2.e_count");
		}
		sql.append(" OFFSET $");

		sql.append(dollarCount);
		tuple.addInteger(offset);
		dollarCount++;
		sql.append(" LIMIT $");
		sql.append(dollarCount);
		tuple.addInteger(limit);
		dollarCount++;

		String sqlString = sql.toString();
		logger.debug("SQL QUERY: " + sqlString);
		logger.debug("SQL TUPLE: " + tuple.deepToString());
		return Tuple2.of(sqlString, tuple);

	}

	public static void main(String[] args) {
		StringBuilder sql = new StringBuilder();
		sql.append("json_build_object(");
		sql.append(NGSIConstants.JSON_LD_LIST);
		sql.append(",json_build_array(");
		sql.append("json_build_object(");
		sql.append(NGSIConstants.JSON_LD_VALUE);
		sql.append(",sum(case ");
		sql.append("when jsonb_typeof(data #>'{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE
				+ "}')='number' then (data #>'{https://uri.etsi.org/ngsi-ld/hasValue,0,@value}')::numeric ");
		sql.append("when jsonb_typeof(data #>'{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE
				+ "}'='boolean' then (data #>'{https://uri.etsi.org/ngsi-ld/hasValue,0,@value}')::boolean ");
		sql.append("when jsonb_typeof(data #>'{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE
				+ "}'='array' then (data #>'{https://uri.etsi.org/ngsi-ld/hasValue,0,@value}').size() ");
		sql.append("else null end)");
		sql.append(")))");
	}

	private int attachAggrQuery(StringBuilder sql, AggrTerm aggrQuery, TemporalQueryTerm tempQuery, int dollarCount,
			Tuple tuple, String tempProp) {
		// Doc comment:
		// we first build a jsonb array with jsonb_agg if it has no contents because of
		// the filter it will be null this will be used later on to filter out null
		// values
		int dollarplus = 1;
		sql.append("PERIODTIMES AS (SELECT DISTINCT ATTRIBID AS AID, MIN(" + tempProp + ") as PRSTART, MAX(" + tempProp
				+ ") as PRSTOP FROM ATTRIBUTEDATA GROUP BY ATTRIBID),");
		sql.append(
				"aggr as (SELECT DISTINCT ID, SCOPES, E_TYPES, R_CREATEDAT, R_MODIFIEDAT, R_DELETEDAT, ATTRIBID, (data #> '{@type,0}') as ATTRTYPE, ");
		for (String aggrFunction : aggrQuery.getAggrFunctions()) {
			sql.append("JSONB_BUILD_OBJECT('" + NGSIConstants.JSON_LD_LIST + "', JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT('"
					+ NGSIConstants.JSON_LD_VALUE + "', ");
			switch (aggrFunction) {
			case NGSIConstants.AGGR_METH_SUM:
				sql.append("SUM(CASE ");
				sql.append("WHEN JSONB_TYPEOF(data #> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
						+ NGSIConstants.JSON_LD_VALUE + "}') = 'number' THEN (data #> '{"
						+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE + "}')::numeric ");
				sql.append("WHEN JSONB_TYPEOF(data #> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
						+ NGSIConstants.JSON_LD_VALUE + "}') = 'boolean' THEN (data #> '{"
						+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE + "}')::numeric ");
				sql.append("WHEN JSONB_TYPEOF(data #> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
						+ NGSIConstants.JSON_LD_VALUE + "}') = 'array' THEN (JSONB_ARRAY_LENGTH(data #> ('{"
						+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE + "}')))::numeric ");
				// nulling here will make the result null as well and it will run into the
				// filter from above
				sql.append("ELSE NULL END)) ");
				dollarplus = generateTimestampForAttr(sql, dollarCount, tempQuery, aggrQuery);
				sql.append(")) as SUMDATA");
				break;
			case NGSIConstants.AGGR_METH_MIN:
				sql.append("MIN(CASE ");
				sql.append("WHEN JSONB_TYPEOF(data #> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
						+ NGSIConstants.JSON_LD_VALUE + "}') = 'number' THEN (data #>> '{"
						+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE + "}') ");
				sql.append("WHEN JSONB_TYPEOF(data #> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
						+ NGSIConstants.JSON_LD_VALUE + "}') = 'boolean' THEN (data #>> '{"
						+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE + "}') ");
				sql.append("WHEN JSONB_TYPEOF(data #> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
						+ NGSIConstants.JSON_LD_VALUE + "}') = 'string' THEN (data #>> '{"
						+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE + "}') ");
				sql.append("WHEN JSONB_TYPEOF(data #> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
						+ NGSIConstants.JSON_LD_VALUE + "}') = 'array' THEN (JSONB_ARRAY_LENGTH(data #> ('{"
						+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE + "}')))::text ");
				sql.append("ELSE NULL END)) ");
				dollarplus = generateTimestampForAttr(sql, dollarCount, tempQuery, aggrQuery);
				sql.append(")) as MINDATA");
				break;
			case NGSIConstants.AGGR_METH_MAX:

				sql.append("(MAX(CASE ");
				sql.append("WHEN JSONB_TYPEOF(data #> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
						+ NGSIConstants.JSON_LD_VALUE + "}') = 'number' THEN (data #>> '{"
						+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE + "}') ");
				sql.append("WHEN JSONB_TYPEOF(data #> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
						+ NGSIConstants.JSON_LD_VALUE + "}') = 'boolean' THEN (data #>> '{"
						+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE + "}') ");
				sql.append("WHEN JSONB_TYPEOF(data #> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
						+ NGSIConstants.JSON_LD_VALUE + "}') = 'string' THEN (data #>> '{"
						+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE + "}') ");
				sql.append("WHEN JSONB_TYPEOF(data #> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
						+ NGSIConstants.JSON_LD_VALUE + "}') = 'array' THEN (JSONB_ARRAY_LENGTH(data #> ('{"
						+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE + "}')))::text ");
				sql.append("ELSE NULL END))) ");
				dollarplus = generateTimestampForAttr(sql, dollarCount, tempQuery, aggrQuery);
				sql.append(")) as MAXDATA");
				break;
			case NGSIConstants.AGGR_METH_AVG:
				sql.append("AVG(CASE ");
				sql.append("WHEN JSONB_TYPEOF(data #> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
						+ NGSIConstants.JSON_LD_VALUE + "}') = 'number' THEN (data #> '{"
						+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE + "}')::numeric ");
				sql.append("WHEN JSONB_TYPEOF(data #> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
						+ NGSIConstants.JSON_LD_VALUE + "}') = 'boolean' THEN (data #> '{"
						+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE + "}')::numeric ");
				sql.append("WHEN JSONB_TYPEOF(data #> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
						+ NGSIConstants.JSON_LD_VALUE + "}') = 'array' THEN (JSONB_ARRAY_LENGTH(data #> ('{"
						+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE + "}')))::numeric ");
				sql.append("ELSE NULL END)) ");
				dollarplus = generateTimestampForAttr(sql, dollarCount, tempQuery, aggrQuery);
				sql.append(")) as AVGDATA");
				break;
			case NGSIConstants.AGGR_METH_STDDEV:
				sql.append("STDDEV(CASE ");
				sql.append("WHEN JSONB_TYPEOF(data #> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
						+ NGSIConstants.JSON_LD_VALUE + "}') = 'number' THEN (data #> '{"
						+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE + "}')::numeric ");
				sql.append("WHEN JSONB_TYPEOF(data #> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
						+ NGSIConstants.JSON_LD_VALUE + "}') = 'boolean' THEN (data #> '{"
						+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE + "}')::numeric ");
				sql.append("WHEN JSONB_TYPEOF(data #> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
						+ NGSIConstants.JSON_LD_VALUE + "}') = 'array' THEN (JSONB_ARRAY_LENGTH(data #> ('{"
						+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE + "}')))::numeric ");
				sql.append("ELSE NULL END)) ");
				dollarplus = generateTimestampForAttr(sql, dollarCount, tempQuery, aggrQuery);
				sql.append(")) as STDDEVDATA");
				break;
			case NGSIConstants.AGGR_METH_SUMSQ:
				sql.append("SUM(CASE ");
				sql.append("WHEN JSONB_TYPEOF(data #> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
						+ NGSIConstants.JSON_LD_VALUE + "}') = 'number' THEN ((data #> '{"
						+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE + "}')::numeric)^2 ");
				sql.append("WHEN JSONB_TYPEOF(data #> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
						+ NGSIConstants.JSON_LD_VALUE + "}') = 'boolean' THEN ((data #> '{"
						+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE + "}')::numeric)^2 ");
				sql.append("WHEN JSONB_TYPEOF(data #> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
						+ NGSIConstants.JSON_LD_VALUE + "}') = 'array' THEN (JSONB_ARRAY_LENGTH(data #> ('{"
						+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE + "}')))::numeric^2 ");
				sql.append("ELSE NULL END)) ");
				dollarplus = generateTimestampForAttr(sql, dollarCount, tempQuery, aggrQuery);
				sql.append(")) as SUMSQDATA");
				break;
			case NGSIConstants.AGGR_METH_TOTAL_COUNT:
				sql.append("COUNT(DATA)) ");
				dollarplus = generateTimestampForAttr(sql, dollarCount, tempQuery, aggrQuery);
				sql.append(")) as TOTALCOUNTDATA");
				break;
			case NGSIConstants.AGGR_METH_DISTINCT_COUNT:
				sql.append("COUNT(DISTINCT CASE ");
				sql.append("WHEN DATA@>'{\"" + NGSIConstants.JSON_LD_TYPE + "\": [\"" + NGSIConstants.NGSI_LD_PROPERTY
						+ "\"]}' THEN DATA #> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
						+ NGSIConstants.JSON_LD_VALUE + "}'");
				sql.append("WHEN DATA@>'{\"" + NGSIConstants.JSON_LD_TYPE + "\": [\""
						+ NGSIConstants.NGSI_LD_RELATIONSHIP + "\"]}' THEN DATA #> '{"
						+ NGSIConstants.NGSI_LD_HAS_OBJECT + ",0," + NGSIConstants.JSON_LD_ID + "}'");
				sql.append("WHEN DATA@>'{\"" + NGSIConstants.JSON_LD_TYPE + "\": [\""
						+ NGSIConstants.NGSI_LD_GEOPROPERTY + "\"]}' THEN DATA #> '{" + NGSIConstants.NGSI_LD_HAS_VALUE
						+ ",0," + NGSIConstants.JSON_LD_VALUE + "}'");
				sql.append(
						"WHEN DATA@>'{\"" + NGSIConstants.JSON_LD_TYPE + "\": [\"" + NGSIConstants.NGSI_LD_LANGPROPERTY
								+ "\"]}' THEN DATA #> '{" + NGSIConstants.NGSI_LD_HAS_LANGUAGE_MAP + "}'");
				sql.append("ELSE DATA END)) ");
				dollarplus = generateTimestampForAttr(sql, dollarCount, tempQuery, aggrQuery);
				sql.append(")) as DISTINCTCOUNTDATA");
				break;
			default:
				break;
			}
			sql.append(',');
		}

		if (aggrQuery.getPeriod() != null) {
			sql.append("PR");
			tuple.addString(aggrQuery.getPeriod());
		} else {
			sql.setLength(sql.length() - 1);
		}

		dollarCount += dollarplus;

		sql.append(" FROM ATTRIBUTEDATA TEAI LEFT JOIN PERIODTIMES PT ON TEAI.ATTRIBID = PT.AID ");
		if (aggrQuery.getPeriod() != null) {
			sql.append("LEFT JOIN generate_series (");
			if (tempQuery == null || tempQuery.getTimerel() == null) {
				sql.append("PT.PRSTART, PT.PRSTOP");
			} else {
				switch (tempQuery.getTimerel()) {
				case NGSIConstants.TIME_REL_BEFORE:
					sql.append("PT.PRSTART, $");
					sql.append(dollarCount);
					sql.append("::text::timestamp");
					tuple.addString(tempQuery.getTimeAt());
					dollarCount++;
					break;
				case NGSIConstants.TIME_REL_AFTER:
					sql.append("$");
					sql.append(dollarCount);
					sql.append("::text::timestamp");
					sql.append(", PT.PRSTOP");
					tuple.addString(tempQuery.getTimeAt());
					dollarCount++;
					break;
				case NGSIConstants.TIME_REL_BETWEEN:
					sql.append("$");
					sql.append(dollarCount);
					sql.append("::text::timestamp");
					tuple.addString(tempQuery.getTimeAt());
					dollarCount++;
					sql.append(", $");
					sql.append(dollarCount);
					sql.append("::text::timestamp");
					tuple.addString(tempQuery.getEndTimeAt());
					dollarCount++;
					break;
				}
			}
			dollarplus = 1;
			sql.append(", $");
			sql.append(dollarCount);
			sql.append("::text::interval) as pr(period) on teai.");
			sql.append(tempProp);
			sql.append(" between pr.period and pr.period + $");
			sql.append(dollarCount);
			sql.append("::text::interval");
			tuple.addString(aggrQuery.getPeriod());
		}
		dollarCount += dollarplus;

		sql.append(" WHERE TEAI.");
		sql.append(tempProp);
		sql.append(" IS NOT NULL");
		if (aggrQuery.getPeriod() != null) {
			sql.append(" AND PR IS NOT NULL");
		}
		sql.append(" GROUP BY ID, SCOPES, E_TYPES, R_CREATEDAT, R_MODIFIEDAT, R_DELETEDAT, ATTRIBID,");
		if (aggrQuery.getPeriod() != null) {
			sql.append(" PR,");
		}
		sql.append(" TEAI.RAW_MODIFIEDAT, TEAI.RAW_CREATEDAT, (data #> '{@type,0}')");
		sql.append(",PT.PRSTART, PT.PRSTOP");
//		if (aggrQuery.getPeriod() != null) {
//			if (tempQuery != null && tempQuery.getTimerel() != null) {
//				switch (tempQuery.getTimerel()) {
//				case NGSIConstants.TIME_REL_BEFORE:
//					sql.append(",($");
//					sql.append(dollarCount);
//					sql.append("::text::timestamp - LEAST(TEAI." + tempProp + "))::interval");
//					break;
//				case NGSIConstants.TIME_REL_AFTER:
//					sql.append(",(GREATEST(TEAI." + tempProp + ") - $");
//					sql.append(dollarCount);
//					sql.append("::text::timestamp)::interval");
//					break;
//				// sql.append(",TEAI." + tempProp);
//				// break;
//				}
//			} else {
//				// sql.append(",TEAI." + tempProp);

//			}
//		}
		sql.append(" ORDER BY ID");
		if (aggrQuery.getPeriod() != null) {
			sql.append(", PR");
		}
		sql.append("), ");
		sql.append(
				"aggr1 as (SELECT id, SCOPES, E_TYPES, R_CREATEDAT, R_MODIFIEDAT, R_DELETEDAT, attribid, jsonb_strip_nulls(jsonb_build_object(");
		for (String aggrFunction : aggrQuery.getAggrFunctions()) {
			switch (aggrFunction) {
			case NGSIConstants.AGGR_METH_SUM:
				sql.append('\'');
				sql.append(NGSIConstants.NGSI_LD_SUM);
				sql.append('\'');
				sql.append(
						",CASE WHEN (JSONB_AGG(X.SUMDATA) FILTER (WHERE X.SUMdata #>>'{@list,0,@value}' is not null)) is not null then ");
				sql.append("JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT('");
				sql.append(NGSIConstants.JSON_LD_LIST);
				sql.append(
						"',JSONB_AGG(X.SUMDATA) FILTER (WHERE X.SUMdata #>>'{@list,0,@value}' is not null))) else null end");
				break;
			case NGSIConstants.AGGR_METH_MIN:
				sql.append('\'');
				sql.append(NGSIConstants.NGSI_LD_MIN);
				sql.append('\'');
				sql.append(
						",CASE WHEN (JSONB_AGG(X.MINdata) FILTER (WHERE X.MINdata #>>'{@list,0,@value}' is not null)) is not null then ");
				sql.append("JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT('");
				sql.append(NGSIConstants.JSON_LD_LIST);
				sql.append(
						"',JSONB_AGG(X.MINDATA) FILTER (WHERE X.MINdata #>>'{@list,0,@value}' is not null))) else null end");
				break;
			case NGSIConstants.AGGR_METH_MAX:
				sql.append('\'');
				sql.append(NGSIConstants.NGSI_LD_MAX);
				sql.append('\'');
				sql.append(
						",CASE WHEN (JSONB_AGG(X.MAXDATA) FILTER (WHERE X.MAXdata #>>'{@list,0,@value}' is not null)) is not null then ");
				sql.append("JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT('");
				sql.append(NGSIConstants.JSON_LD_LIST);
				sql.append(
						"',JSONB_AGG(X.MAXDATA) FILTER (WHERE X.MAXdata #>>'{@list,0,@value}' is not null))) else null end");
				break;
			case NGSIConstants.AGGR_METH_AVG:
				sql.append('\'');
				sql.append(NGSIConstants.NGSI_LD_AVG);
				sql.append('\'');
				sql.append(
						",CASE WHEN (JSONB_AGG(X.AVGDATA) FILTER (WHERE X.AVGdata #>>'{@list,0,@value}' is not null)) is not null then ");
				sql.append("JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT('");
				sql.append(NGSIConstants.JSON_LD_LIST);
				sql.append(
						"',JSONB_AGG(X.AVGDATA) FILTER (WHERE X.AVGdata #>>'{@list,0,@value}' is not null))) else null end");
				break;
			case NGSIConstants.AGGR_METH_STDDEV:
				sql.append('\'');
				sql.append(NGSIConstants.NGSI_LD_STDDEV);
				sql.append('\'');
				sql.append(
						",CASE WHEN (JSONB_AGG(X.STDDEVDATA) FILTER (WHERE X.STDDEVdata #>>'{@list,0,@value}' is not null)) is not null then ");
				sql.append("JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT('");
				sql.append(NGSIConstants.JSON_LD_LIST);
				sql.append(
						"',JSONB_AGG(X.STDDEVDATA) FILTER (WHERE X.STDDEVdata #>>'{@list,0,@value}' is not null))) else null end");
				break;
			case NGSIConstants.AGGR_METH_SUMSQ:
				sql.append('\'');
				sql.append(NGSIConstants.NGSI_LD_SUMSQ);
				sql.append('\'');
				sql.append(
						",CASE WHEN (JSONB_AGG(X.SUMSQDATA) FILTER (WHERE X.SUMSQdata #>>'{@list,0,@value}' is not null)) is not null then ");
				sql.append("JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT('");
				sql.append(NGSIConstants.JSON_LD_LIST);
				sql.append(
						"',JSONB_AGG(X.SUMSQDATA) FILTER (WHERE X.SUMSQdata #>>'{@list,0,@value}' is not null))) else null end");
				break;
			case NGSIConstants.AGGR_METH_TOTAL_COUNT:
				sql.append('\'');
				sql.append(NGSIConstants.NGSI_LD_TOTALCOUNT);
				sql.append('\'');
				sql.append(
						",CASE WHEN (JSONB_AGG(X.TOTALCOUNTDATA) FILTER (WHERE X.TOTALCOUNTdata #>>'{@list,0,@value}' is not null)) is not null then ");
				sql.append("JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT('");
				sql.append(NGSIConstants.JSON_LD_LIST);
				sql.append(
						"',JSONB_AGG(X.TOTALCOUNTDATA) FILTER (WHERE X.TOTALCOUNTdata #>>'{@list,0,@value}' is not null))) else null end");
				break;
			case NGSIConstants.AGGR_METH_DISTINCT_COUNT:
				sql.append('\'');
				sql.append(NGSIConstants.NGSI_LD_DISTINCTCOUNT);
				sql.append('\'');
				sql.append(
						",CASE WHEN (JSONB_AGG(X.DISTINCTCOUNTDATA) FILTER (WHERE X.DISTINCTCOUNTdata #>>'{@list,0,@value}' is not null)) is not null then ");
				sql.append("JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT('");
				sql.append(NGSIConstants.JSON_LD_LIST);
				sql.append(
						"',JSONB_AGG(X.DISTINCTCOUNTDATA) FILTER (WHERE X.DISTINCTCOUNTdata #>>'{@list,0,@value}' is not null))) else null end");
				break;
			default:
				break;
			}
			sql.append(',');
		}
		sql.setLength(sql.length() - 1);
		sql.append(
				")) as data, ATTRTYPE FROM aggr X GROUP BY id, SCOPES, E_TYPES, R_CREATEDAT, R_MODIFIEDAT, R_DELETEDAT, attribid, ATTRTYPE), ");
		return dollarCount;
	}

	private int generateTimestampForAttr(StringBuilder sql, int dollarCount, TemporalQueryTerm tempQuery,
			AggrTerm aggrQuery) {
		if (aggrQuery.getPeriod() != null) {
			sql.append(",JSONB_BUILD_OBJECT('" + NGSIConstants.JSON_LD_VALUE
					+ "', to_char(pr.period, "+TIMESTAMP_FORMAT+")), ");
			sql.append("JSONB_BUILD_OBJECT('" + NGSIConstants.JSON_LD_VALUE + "', to_char(pr.period + ");
			sql.append("$");
			sql.append(dollarCount);
			sql.append("::text::interval");
			sql.append(", "+TIMESTAMP_FORMAT+"))");
			return 1;
		} else {
			sql.append(",JSONB_BUILD_OBJECT('" + NGSIConstants.JSON_LD_VALUE
					+ "', to_char(PT.PRSTART, "+TIMESTAMP_FORMAT+")), ");
			sql.append("JSONB_BUILD_OBJECT('" + NGSIConstants.JSON_LD_VALUE + "', to_char(PT.PRSTOP ");
			sql.append(", " + TIMESTAMP_FORMAT + "))");
			return 0;
		}
	}

}