package eu.neclab.ngsildbroker.historyquerymanager.repository;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.DBConstants;
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
import eu.neclab.ngsildbroker.commons.storage.ClientManager;
import eu.neclab.ngsildbroker.commons.tools.DBUtil;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowIterator;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.Tuple;

@Singleton
public class HistoryDAO {

	private static Logger logger = LoggerFactory.getLogger(HistoryDAO.class);

	@Inject
	ClientManager clientManager;

	/**
	 * 
	 * @param tenant
	 * @param entityId
	 * @param attrsQuery
	 * @param aggrQuery
	 * @param tempQuery
	 * @param lang
	 * @param lastN
	 * @return a single row with a single column containing the constructed entity.
	 */
	public Uni<Map<String, Object>> retrieveEntity(String tenant, String entityId, AttrsQueryTerm attrsQuery,
			AggrTerm aggrQuery, TemporalQueryTerm tempQuery, String lang, int lastN) {
		return clientManager.getClient(tenant, false).onItem().transformToUni(client -> {
			Tuple tuple = Tuple.tuple();
			tuple.addString(entityId);
			int dollarCount = 2;

			StringBuilder sql = new StringBuilder(
					"with a as (select id , e_types, temporalentity.createdat as raw_createdat, temporalentity.modifiedat as raw_modifiedat, case when scopes is null then null else getScopeEntry(scopes) end as scopes, jsonb_build_array(jsonb_build_object('@type', '"
							+ NGSIConstants.NGSI_LD_DATE_TIME
							+ "', '@value', to_char(temporalentity.createdat, 'YYYY-MM-DDThh:mm:ss.usZ'))) as r_createdat, jsonb_build_array(jsonb_build_object('@type', '"
							+ NGSIConstants.NGSI_LD_DATE_TIME
							+ "', '@value', to_char(temporalentity.modifiedat, 'YYYY-MM-DDThh:mm:ss.usZ'))) as r_modifiedat, case when deletedat is null then null else jsonb_build_array(jsonb_build_object('@type', '"
							+ NGSIConstants.NGSI_LD_DATE_TIME
							+ "', '@value', to_char(temporalentity.deletedat, 'YYYY-MM-DDThh:mm:ss.usZ')))  end as r_deletedat from temporalentity where id=$1),"
							+ "b as (SELECT DISTINCT TEAI.ID AS ID, TEAI.ATTRIBUTEID AS ATTRIBID, u.data as data "
							+ "FROM (A LEFT JOIN TEMPORALENTITYATTRINSTANCE on A.id = TEMPORALENTITYATTRINSTANCE.temporalentity_id) as TEAI LEFT JOIN LATERAL (");

			if (aggrQuery == null) {
				sql.append(
						"SELECT jsonb_agg(x.data) as data from (Select data as data from TEMPORALENTITYATTRINSTANCE TEAI2");
			} else {
				dollarCount = attachTopAggrQuery(sql, aggrQuery, tempQuery, dollarCount, tuple);

			}
			sql.append(
					" WHERE TEAI.ATTRIBUTEID = TEAI2.ATTRIBUTEID AND TEAI.temporalentity_id = TEAI2.temporalentity_id ");

			if (attrsQuery != null) {
				sql.append("AND TEAI2.attributeId in ($" + dollarCount + ")");
				dollarCount++;
				tuple.addArrayOfString(attrsQuery.getAttrs().toArray(new String[0]));
			}

			if (tempQuery != null && aggrQuery == null) {
				sql.append("AND TEAI2.");
				dollarCount = tempQuery.toSql(sql, tuple, dollarCount);

			}
			if (aggrQuery == null) {
				sql.append(" ORDER BY TEAI2.modifiedat LIMIT $");
				sql.append(dollarCount);
				dollarCount++;
				tuple.addInteger(lastN);
				sql.append(") as x) as u on true ORDER BY TEAI.ID) ");
			} else {
				attachAggrQueryBottomPart(sql, aggrQuery);
			}

			sql.append("select (jsonb_build_object('" + NGSIConstants.JSON_LD_ID + "', b.id, '"
					+ NGSIConstants.JSON_LD_TYPE + "', a.e_types, '" + NGSIConstants.NGSI_LD_CREATED_AT
					+ "', a.r_createdat, '" + NGSIConstants.NGSI_LD_MODIFIED_AT
					+ "', a.r_modifiedat) || jsonb_object_agg(b.attribid, b.data) FILTER (WHERE b.data is not null)) || (case when a.r_deletedat is null then '{}'::jsonb else jsonb_build_object('"
					+ NGSIConstants.NGSI_LD_DELETED_AT
					+ "', a.r_deletedat) end) || (case when a.scopes is null then '{}'::jsonb else jsonb_build_object('"
					+ NGSIConstants.NGSI_LD_SCOPE
					+ "', a.scopes) end) from b left join a on a.id = b.id group by b.id, a.e_types, a.r_createdat, a.r_modifiedat, a.r_deletedat, a.scopes");
			return client.preparedQuery(sql.toString()).execute(tuple).onItem().transform(rows -> {
				if (rows.size() == 0) {
					return new HashMap<>(0);
				}
				return rows.iterator().next().getJsonObject(0).getMap();
			});
		});

	}

	private int getPeriod(StringBuilder sql, int dollarCount, TemporalQueryTerm tempQuery, AggrTerm aggrQuery) {
		int dollarplus = 1;
		if (aggrQuery.getPeriod() != null) {
			sql.append("$");
			sql.append(dollarCount);
		} else {
			if (tempQuery != null) {
				switch (tempQuery.getTimerel()) {
				case NGSIConstants.TIME_REL_BEFORE:
					sql.append("TEAI.raw_createdat - $");
					sql.append(dollarCount);
					break;
				case NGSIConstants.TIME_REL_AFTER:
					sql.append("$");
					sql.append(dollarCount);
					sql.append("- TEAI.raw_modifiedat");
					break;
				case NGSIConstants.TIME_REL_BETWEEN:
					sql.append("$");
					sql.append(dollarCount);
					sql.append("- $");
					sql.append(dollarCount + 1);
					dollarplus = 2;
					break;
				}
			} else {
				sql.append("(TEAI.raw_modifiedAt - TEAI.raw_createdAt)::interval");
				dollarplus = 0;
			}
		}
		return dollarplus;
	}

	public Uni<Table<String, String, RegistrationEntry>> getAllRegistries() {
		return clientManager.getClient(AppConstants.INTERNAL_NULL_KEY, false).onItem().transformToUni(client -> {
			return client.preparedQuery("SELECT tenant_id FROM tenant").execute().onItem()
					.transformToUni(tenantRows -> {
						List<Uni<Tuple2<String, RowSet<Row>>>> unis = Lists.newArrayList();
						RowIterator<Row> it = tenantRows.iterator();
						String sql = "SELECT cs_id, c_id, e_id, e_id_p, e_type, e_prop, e_rel, ST_AsGeoJSON(i_location), scopes, EXTRACT(MILLISECONDS FROM expires), endpoint, tenant_id, headers, reg_mode, createEntity, updateEntity, appendAttrs, updateAttrs, deleteAttrs, deleteEntity, createBatch, upsertBatch, updateBatch, deleteBatch, upsertTemporal, appendAttrsTemporal, deleteAttrsTemporal, updateAttrsTemporal, deleteAttrInstanceTemporal, deleteTemporal, mergeEntity, replaceEntity, replaceAttrs, mergeBatch, retrieveEntity, queryEntity, queryBatch, retrieveTemporal, queryTemporal, retrieveEntityTypes, retrieveEntityTypeDetails, retrieveEntityTypeInfo, retrieveAttrTypes, retrieveAttrTypeDetails, retrieveAttrTypeInfo, createSubscription, updateSubscription, retrieveSubscription, querySubscription, deleteSubscription FROM csourceinformation WHERE retrieveTemporal OR queryTemporal";
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
								@SuppressWarnings("unchecked")
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

	public Uni<QueryResult> query(String tenant, String[] entityIds, TypeQueryTerm typeQuery, String idPattern,
			AttrsQueryTerm attrsQuery, QQueryTerm qQuery, TemporalQueryTerm tempQuery, AggrTerm aggrQuery,
			GeoQueryTerm geoQuery, ScopeQueryTerm scopeQuery, int lastN, int limit, int offset, boolean count) {
		return clientManager.getClient(tenant, false).onItem().transformToUni(client -> {
			Tuple tuple = Tuple.tuple();

			int dollarCount = 1;
			String entityInfos = "entityInfos";
			StringBuilder sql = new StringBuilder("with " + entityInfos
					+ " as (select id , e_types, temporalentity.createdat as raw_createdat, temporalentity.modifiedat as raw_modifiedat, case when scopes is null then null else getScopeEntry(scopes) end as scopes, jsonb_build_array(jsonb_build_object('@type', '"
					+ NGSIConstants.NGSI_LD_DATE_TIME
					+ "', '@value', to_char(temporalentity.createdat, 'YYYY-MM-DDThh:mm:ss.usZ'))) as r_createdat, jsonb_build_array(jsonb_build_object('@type', '"
					+ NGSIConstants.NGSI_LD_DATE_TIME
					+ "', '@value', to_char(temporalentity.modifiedat, 'YYYY-MM-DDThh:mm:ss.usZ'))) as r_modifiedat, case when deletedat is null then null else jsonb_build_array(jsonb_build_object('@type', '"
					+ NGSIConstants.NGSI_LD_DATE_TIME
					+ "', '@value', to_char(temporalentity.deletedat, 'YYYY-MM-DDThh:mm:ss.usZ')))  end as r_deletedat from temporalentity where 1=1");
			if (typeQuery != null) {
				sql.append(" AND ");
				dollarCount = typeQuery.toSql(sql, tuple, dollarCount);
			}
			if (entityIds != null) {
				sql.append(" AND id in ($");
				sql.append(dollarCount);
				sql.append(")");
				dollarCount++;
				tuple.addArrayOfString(entityIds);
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
			if (geoQuery != null) {
				String newEntityInfos = "geoFilteredInfos";
				sql.append(newEntityInfos);
				sql.append(" as (SELECT ");
				sql.append(entityInfos);
				sql.append(".* FROM ");
				sql.append(entityInfos);
				sql.append(" LEFT JOIN ");
				sql.append(DBConstants.DBTABLE_TEMPORALENTITY_ATTRIBUTEINSTANCE);
				sql.append(" ON ");
				sql.append(DBConstants.DBTABLE_TEMPORALENTITY_ATTRIBUTEINSTANCE);
				sql.append(".temporalentity_id = ");
				sql.append(entityInfos);
				sql.append(".id WHERE ");
				dollarCount = geoQuery.toTempSql(sql, tuple, dollarCount, tempQuery);
				sql.append("), ");
				entityInfos = newEntityInfos;
			}
			if (qQuery != null) {
				int[] tmp = qQuery.toTempSql(sql, dollarCount, tuple, 0, entityInfos, tempQuery);
				dollarCount = tmp[0];
				String newEntityInfos = "filteredEntityInfo";
				sql.append(newEntityInfos);
				sql.append(" as (SELECT ");
				sql.append(entityInfos);
				sql.append(".* FROM filtered");
				sql.append(tmp[1] - 1);
				sql.append(" LEFT JOIN ");
				sql.append(entityInfos);
				sql.append(" ON filtered");
				sql.append(tmp[1] - 1);
				sql.append(".id = ");
				sql.append(entityInfos);
				sql.append(".id), ");
				entityInfos = newEntityInfos;
			}

			sql.append("attributeData as (SELECT DISTINCT TEAI.ID AS ID, TEAI.ATTRIBUTEID AS ATTRIBID, u.data as data "
					+ "FROM (" + entityInfos + " LEFT JOIN TEMPORALENTITYATTRINSTANCE on " + entityInfos
					+ ".id = TEMPORALENTITYATTRINSTANCE.temporalentity_id) as TEAI LEFT JOIN LATERAL (");

			if (aggrQuery == null) {
				sql.append(
						"SELECT jsonb_agg(x.data) as data from (Select data as data from TEMPORALENTITYATTRINSTANCE TEAI2");
			} else {
				dollarCount = attachTopAggrQuery(sql, aggrQuery, tempQuery, dollarCount, tuple);

			}
			sql.append(
					" WHERE TEAI.ATTRIBUTEID = TEAI2.ATTRIBUTEID AND TEAI.temporalentity_id = TEAI2.temporalentity_id ");

			if (attrsQuery != null) {
				sql.append("AND TEAI2.attributeId in ($" + dollarCount + ")");
				dollarCount++;
				tuple.addArrayOfString(attrsQuery.getAttrs().toArray(new String[0]));
			}

			if (tempQuery != null && aggrQuery == null) {
				sql.append("AND TEAI2.");
				dollarCount = tempQuery.toSql(sql, tuple, dollarCount);

			}
			if (aggrQuery == null) {
				sql.append(" ORDER BY TEAI2.modifiedat LIMIT $");
				sql.append(dollarCount);
				dollarCount++;
				tuple.addInteger(lastN);
				sql.append(") as x) as u on true ORDER BY TEAI.ID) ");
			} else {
				attachAggrQueryBottomPart(sql, aggrQuery);
			}

			sql.append("select (jsonb_build_object('" + NGSIConstants.JSON_LD_ID + "', attributeData.id, '"
					+ NGSIConstants.JSON_LD_TYPE + "', " + entityInfos + ".e_types, '"
					+ NGSIConstants.NGSI_LD_CREATED_AT + "', " + entityInfos + ".r_createdat, '"
					+ NGSIConstants.NGSI_LD_MODIFIED_AT + "', " + entityInfos
					+ ".r_modifiedat) || jsonb_object_agg(attributeData.attribid, attributeData.data) FILTER (WHERE attributeData.data is not null)) || (case when "
					+ entityInfos + ".r_deletedat is null then '{}'::jsonb else jsonb_build_object('"
					+ NGSIConstants.NGSI_LD_DELETED_AT + "', " + entityInfos + ".r_deletedat) end) || (case when "
					+ entityInfos + ".scopes is null then '{}'::jsonb else jsonb_build_object('"
					+ NGSIConstants.NGSI_LD_SCOPE + "', " + entityInfos + ".scopes) end)");
			if (count) {
				sql.append(", count(*)");
			}
			sql.append(" from attributeData left join " + entityInfos + " on " + entityInfos
					+ ".id = attributeData.id group by attributeData.id, " + entityInfos + ".e_types, " + entityInfos
					+ ".r_createdat, " + entityInfos + ".r_modifiedat, " + entityInfos + ".r_deletedat, " + entityInfos
					+ ".scopes");
			sql.append(" LIMIT $");
			sql.append(dollarCount);
			dollarCount++;
			sql.append(" OFFSET $");
			sql.append(dollarCount);
			dollarCount++;
			tuple.addInteger(limit);
			tuple.addInteger(offset);
			System.out.println(sql.toString());
			System.out.println(tuple.deepToString());
			return client.preparedQuery(sql.toString()).execute(tuple).onItem().transform(rows -> {
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
						entity = next.getJsonObject(0).getMap();
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

	public static void main(String[] args) {
		StringBuilder sql = new StringBuilder();
		sql.append("json_build_object(");
		sql.append(NGSIConstants.JSON_LD_LIST);
		sql.append(",json_build_array(");
		sql.append("json_build_object(");
		sql.append(NGSIConstants.JSON_LD_VALUE);
		sql.append(",sum(case ");
		sql.append("when jsonb_typeof(data#>'{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE
				+ "}')='number' then (data#>'{https://uri.etsi.org/ngsi-ld/hasValue,0,@value}')::numeric ");
		sql.append("when jsonb_typeof(data#>'{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE
				+ "}'='boolean' then (data#>'{https://uri.etsi.org/ngsi-ld/hasValue,0,@value}')::boolean ");
		sql.append("when jsonb_typeof(data#>'{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE
				+ "}'='array' then (data#>'{https://uri.etsi.org/ngsi-ld/hasValue,0,@value}').size() ");
		sql.append("else null end)");
		sql.append(")))");
	}

	private int attachTopAggrQuery(StringBuilder sql, AggrTerm aggrQuery, TemporalQueryTerm tempQuery, int dollarCount,
			Tuple tuple) {
		// Doc comment:
		// we first build a jsonb array with jsonb_agg if it has no contents because of
		// the filter it will be null this will be used later on to filter out null
		// values
		sql.append("WITH Z as (SELECT ");
		for (String aggrFunction : aggrQuery.getAggrFunctions()) {
			switch (aggrFunction) {
			case NGSIConstants.AGGR_METH_SUM:
				sql.append("JSONB_AGG(X.SUMDATA) FILTER (WHERE X.SUMDATA#>>'{@list,0,@value}' is not null) AS SUMDATA");
				break;
			case NGSIConstants.AGGR_METH_MIN:
				sql.append("JSONB_AGG(X.MINDATA) FILTER (WHERE X.MINDATA#>>'{@list,0,@value}' is not null) AS MINDATA");
				break;
			case NGSIConstants.AGGR_METH_MAX:
				sql.append("JSONB_AGG(X.MAXDATA) FILTER (WHERE X.MAXDATA#>>'{@list,0,@value}' is not null) AS MAXDATA");
				break;
			case NGSIConstants.AGGR_METH_AVG:
				sql.append("JSONB_AGG(X.AVGDATA) FILTER (WHERE X.AVGDATA#>>'{@list,0,@value}' is not null) AS AVGDATA");
				break;
			case NGSIConstants.AGGR_METH_STDDEV:
				sql.append(
						"JSONB_AGG(X.STDDEVDATA) FILTER (WHERE X.STDDEVDATA#>>'{@list,0,@value}' is not null) AS STDDEVDATA");
				break;
			case NGSIConstants.AGGR_METH_SUMSQ:
				sql.append(
						"JSONB_AGG(X.SUMSQDATA) FILTER (WHERE X.SUMSQDATA#>>'{@list,0,@value}' is not null) AS SUMSQDATA");
				break;
			case NGSIConstants.AGGR_METH_TOTAL_COUNT:
				sql.append(
						"JSONB_AGG(X.TOTALCOUNTDATA) FILTER (WHERE X.TOTALCOUNTDATA#>>'{@list,0,@value}' is not null) AS TOTALCOUNTDATA");
				break;
			case NGSIConstants.AGGR_METH_DISTINCT_COUNT:
				sql.append(
						"JSONB_AGG(X.DISTINCTCOUNTDATA) FILTER (WHERE X.DISTINCTCOUNTDATA#>>'{@list,0,@value}' is not null) AS DISTINCTCOUNTDATA");
				break;
			default:
				break;
			}
			sql.append(',');
		}
		sql.setLength(sql.length() - 1);
		sql.append(" FROM (SELECT ");
		int dollarplus = 1;
		for (String aggrFunction : aggrQuery.getAggrFunctions()) {
			sql.append("JSONB_BUILD_OBJECT('" + NGSIConstants.JSON_LD_LIST + "', JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT('"
					+ NGSIConstants.JSON_LD_VALUE + "', ");
			switch (aggrFunction) {
			case NGSIConstants.AGGR_METH_SUM:
				sql.append("SUM(CASE ");
				sql.append("WHEN JSONB_TYPEOF(DATA#> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
						+ NGSIConstants.JSON_LD_VALUE + "}') = 'number' THEN (DATA#> '{"
						+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE + "}')::numeric ");
				sql.append("WHEN JSONB_TYPEOF(DATA#> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
						+ NGSIConstants.JSON_LD_VALUE + "}') = 'boolean' THEN (DATA#> '{"
						+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE + "}')::numeric ");
				sql.append("WHEN JSONB_TYPEOF(DATA#> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
						+ NGSIConstants.JSON_LD_VALUE + "}') = 'array' THEN (JSONB_ARRAY_LENGTH(DATA#> ('{"
						+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE + "}')))::numeric ");
				// nulling here will make the result null as well and it will run into the
				// filter from above
				sql.append("ELSE NULL END)), ");
				sql.append("JSONB_BUILD_OBJECT('" + NGSIConstants.JSON_LD_VALUE + "', pr.period), ");
				sql.append("JSONB_BUILD_OBJECT('" + NGSIConstants.JSON_LD_VALUE + "', pr.period + ");
				dollarplus = getPeriod(sql, dollarCount, tempQuery, aggrQuery);
				sql.append("))) as SUMDATA");
				break;
			case NGSIConstants.AGGR_METH_MIN:
				sql.append("MIN(CASE ");
				sql.append("WHEN JSONB_TYPEOF(DATA#> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
						+ NGSIConstants.JSON_LD_VALUE + "}') = 'number' THEN (DATA#>> '{"
						+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE + "}') ");
				sql.append("WHEN JSONB_TYPEOF(DATA#> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
						+ NGSIConstants.JSON_LD_VALUE + "}') = 'boolean' THEN (DATA#>> '{"
						+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE + "}') ");
				sql.append("WHEN JSONB_TYPEOF(DATA#> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
						+ NGSIConstants.JSON_LD_VALUE + "}') = 'string' THEN (DATA#>> '{"
						+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE + "}') ");
				sql.append("WHEN JSONB_TYPEOF(DATA#> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
						+ NGSIConstants.JSON_LD_VALUE + "}') = 'array' THEN (JSONB_ARRAY_LENGTH(DATA#> ('{"
						+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE + "}')))::text ");
				sql.append("ELSE NULL END)), ");
				sql.append("JSONB_BUILD_OBJECT('" + NGSIConstants.JSON_LD_VALUE + "', pr.period), ");
				sql.append("JSONB_BUILD_OBJECT('" + NGSIConstants.JSON_LD_VALUE + "', pr.period + ");
				dollarplus = getPeriod(sql, dollarCount, tempQuery, aggrQuery);
				sql.append("))) as MINDATA");
				break;
			case NGSIConstants.AGGR_METH_MAX:
				sql.append("MAX(CASE ");
				sql.append("WHEN JSONB_TYPEOF(DATA#> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
						+ NGSIConstants.JSON_LD_VALUE + "}') = 'number' THEN (DATA#>> '{"
						+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE + "}') ");
				sql.append("WHEN JSONB_TYPEOF(DATA#> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
						+ NGSIConstants.JSON_LD_VALUE + "}') = 'boolean' THEN (DATA#>> '{"
						+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE + "}') ");
				sql.append("WHEN JSONB_TYPEOF(DATA#> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
						+ NGSIConstants.JSON_LD_VALUE + "}') = 'string' THEN (DATA#>> '{"
						+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE + "}') ");
				sql.append("WHEN JSONB_TYPEOF(DATA#> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
						+ NGSIConstants.JSON_LD_VALUE + "}') = 'array' THEN (JSONB_ARRAY_LENGTH(DATA#> ('{"
						+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE + "}')))::text ");
				sql.append("ELSE NULL END)), ");
				sql.append("JSONB_BUILD_OBJECT('" + NGSIConstants.JSON_LD_VALUE + "', pr.period), ");
				sql.append("JSONB_BUILD_OBJECT('" + NGSIConstants.JSON_LD_VALUE + "', pr.period + ");
				dollarplus = getPeriod(sql, dollarCount, tempQuery, aggrQuery);
				sql.append("))) as MAXDATA");
				break;
			case NGSIConstants.AGGR_METH_AVG:
				sql.append("AVG(CASE ");
				sql.append("WHEN JSONB_TYPEOF(DATA#> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
						+ NGSIConstants.JSON_LD_VALUE + "}') = 'number' THEN (DATA#> '{"
						+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE + "}')::numeric ");
				sql.append("WHEN JSONB_TYPEOF(DATA#> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
						+ NGSIConstants.JSON_LD_VALUE + "}') = 'boolean' THEN (DATA#> '{"
						+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE + "}')::numeric ");
				sql.append("WHEN JSONB_TYPEOF(DATA#> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
						+ NGSIConstants.JSON_LD_VALUE + "}') = 'array' THEN (JSONB_ARRAY_LENGTH(DATA#> ('{"
						+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE + "}')))::numeric ");
				sql.append("ELSE NULL END)), ");
				sql.append("JSONB_BUILD_OBJECT('" + NGSIConstants.JSON_LD_VALUE + "', pr.period), ");
				sql.append("JSONB_BUILD_OBJECT('" + NGSIConstants.JSON_LD_VALUE + "', pr.period + ");
				dollarplus = getPeriod(sql, dollarCount, tempQuery, aggrQuery);
				sql.append("))) as AVGDATA");
				break;
			case NGSIConstants.AGGR_METH_STDDEV:
				sql.append("STDDEV(CASE ");
				sql.append("WHEN JSONB_TYPEOF(DATA#> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
						+ NGSIConstants.JSON_LD_VALUE + "}') = 'number' THEN (DATA#> '{"
						+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE + "}')::numeric ");
				sql.append("WHEN JSONB_TYPEOF(DATA#> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
						+ NGSIConstants.JSON_LD_VALUE + "}') = 'boolean' THEN (DATA#> '{"
						+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE + "}')::numeric ");
				sql.append("WHEN JSONB_TYPEOF(DATA#> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
						+ NGSIConstants.JSON_LD_VALUE + "}') = 'array' THEN (JSONB_ARRAY_LENGTH(DATA#> ('{"
						+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE + "}')))::numeric ");
				sql.append("ELSE NULL END)), ");
				sql.append("JSONB_BUILD_OBJECT('" + NGSIConstants.JSON_LD_VALUE + "', pr.period), ");
				sql.append("JSONB_BUILD_OBJECT('" + NGSIConstants.JSON_LD_VALUE + "', pr.period + ");
				dollarplus = getPeriod(sql, dollarCount, tempQuery, aggrQuery);
				sql.append("))) as STDDEVDATA");
				break;
			case NGSIConstants.AGGR_METH_SUMSQ:
				sql.append("SUM(CASE ");
				sql.append("WHEN JSONB_TYPEOF(DATA#> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
						+ NGSIConstants.JSON_LD_VALUE + "}') = 'number' THEN ((DATA#> '{"
						+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE + "}')::numeric)^2 ");
				sql.append("WHEN JSONB_TYPEOF(DATA#> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
						+ NGSIConstants.JSON_LD_VALUE + "}') = 'boolean' THEN ((DATA#> '{"
						+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE + "}')::numeric)^2 ");
				sql.append("WHEN JSONB_TYPEOF(DATA#> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
						+ NGSIConstants.JSON_LD_VALUE + "}') = 'array' THEN (JSONB_ARRAY_LENGTH(DATA#> ('{"
						+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE + "}')))::numeric^2 ");
				sql.append("ELSE NULL END)), ");
				sql.append("JSONB_BUILD_OBJECT('" + NGSIConstants.JSON_LD_VALUE + "', pr.period), ");
				sql.append("JSONB_BUILD_OBJECT('" + NGSIConstants.JSON_LD_VALUE + "', pr.period + ");
				dollarplus = getPeriod(sql, dollarCount, tempQuery, aggrQuery);
				sql.append("))) as SUMSQDATA");
				break;
			case NGSIConstants.AGGR_METH_TOTAL_COUNT:
				sql.append("COUNT(DATA)), ");
				sql.append("JSONB_BUILD_OBJECT('" + NGSIConstants.JSON_LD_VALUE + "', pr.period), ");
				sql.append("JSONB_BUILD_OBJECT('" + NGSIConstants.JSON_LD_VALUE + "', pr.period + ");
				dollarplus = getPeriod(sql, dollarCount, tempQuery, aggrQuery);
				sql.append("))) as TOTALCOUNTDATA");
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
				sql.append("ELSE DATA END)), ");
				sql.append("JSONB_BUILD_OBJECT('" + NGSIConstants.JSON_LD_VALUE + "', pr.period), ");
				sql.append("JSONB_BUILD_OBJECT('" + NGSIConstants.JSON_LD_VALUE + "', pr.period + ");
				dollarplus = getPeriod(sql, dollarCount, tempQuery, aggrQuery);
				sql.append("))) as TOTALCOUNTDATA");
				break;
			default:
				break;
			}
			sql.append(',');
		}
		sql.setLength(sql.length() - 1);
		if (aggrQuery.getPeriod() != null) {
			tuple.addString(aggrQuery.getPeriod());
		} else {
			switch (dollarplus) {
			case 1:
				tuple.addLocalDateTime(LocalDateTime.parse(tempQuery.getTimeAt(), SerializationTools.informatter));
				break;
			case 2:
				tuple.addLocalDateTime(LocalDateTime.parse(tempQuery.getTimeAt(), SerializationTools.informatter));
				tuple.addLocalDateTime(LocalDateTime.parse(tempQuery.getEndTimeAt(), SerializationTools.informatter));
				break;
			default:
				break;
			}
		}

		dollarCount += dollarplus;
		sql.append(" FROM TEMPORALENTITYATTRINSTANCE TEAI2 RIGHT JOIN generate_series (");
		if (tempQuery == null) {
			sql.append(
					"TEAI.raw_createdat, TEAI.raw_modifiedat - (TEAI.RAW_MODIFIEDAT - TEAI.RAW_CREATEDAT)::interval");
		} else {
			switch (tempQuery.getTimerel()) {
			case NGSIConstants.TIME_REL_BEFORE:
				sql.append("TEAI.raw_createdat, $");
				sql.append(dollarCount);
				tuple.addLocalDateTime(LocalDateTime.parse(tempQuery.getTimeAt(), SerializationTools.informatter));
				dollarCount++;
				break;
			case NGSIConstants.TIME_REL_AFTER:
				sql.append("$");
				sql.append(dollarCount);
				sql.append(", TEAI.raw_modifiedat");
				tuple.addLocalDateTime(LocalDateTime.parse(tempQuery.getTimeAt(), SerializationTools.informatter));
				dollarCount++;
				break;
			case NGSIConstants.TIME_REL_BETWEEN:
				sql.append("$");
				sql.append(dollarCount);
				tuple.addLocalDateTime(LocalDateTime.parse(tempQuery.getTimeAt(), SerializationTools.informatter));
				dollarCount++;
				sql.append(", $");
				sql.append(dollarCount);
				tuple.addLocalDateTime(LocalDateTime.parse(tempQuery.getEndTimeAt(), SerializationTools.informatter));
				dollarCount++;
				break;
			}
		}
		dollarplus = 1;
		if (aggrQuery.getPeriod() != null) {
			sql.append("-$");
			sql.append(dollarCount);
			sql.append("::interval, $");
			sql.append(dollarCount);
			sql.append("::interval) as pr(period) on teai2.modifiedat between pr.period and pr.period + $");
			sql.append(dollarCount);
			tuple.addString(aggrQuery.getPeriod());
		} else {
			if (tempQuery != null) {
				switch (tempQuery.getTimerel()) {
				case NGSIConstants.TIME_REL_BEFORE:
					sql.append("-(TEAI.raw_createdat - $");
					sql.append(dollarCount);
					sql.append(")::interval");
					sql.append(",(TEAI.raw_createdat - $");
					sql.append(dollarCount);
					sql.append(
							")::interval) as pr(period) on teai2.modifiedat between pr.period and pr.period + (TEAI.raw_createdat - $");
					sql.append(dollarCount);
					sql.append(")::interval");
					tuple.addLocalDateTime(LocalDateTime.parse(tempQuery.getTimeAt(), SerializationTools.informatter));
					break;
				case NGSIConstants.TIME_REL_AFTER:
					sql.append("-($");
					sql.append(dollarCount);
					sql.append("- TEAI.raw_modifiedat)::interval");
					sql.append(",($");
					sql.append(dollarCount);
					sql.append(
							"- TEAI.raw_modifiedat)::interval) as pr(period) on teai2.modifiedat between pr.period and pr.period + ($");
					sql.append(dollarCount);
					sql.append(" - TEAI.raw_createdat)::interval");
					tuple.addLocalDateTime(LocalDateTime.parse(tempQuery.getTimeAt(), SerializationTools.informatter));
					break;
				case NGSIConstants.TIME_REL_BETWEEN:
					sql.append("- ($");
					sql.append(dollarCount);
					sql.append("- $");
					sql.append(dollarCount + 1);
					sql.append(")::interval,($");
					sql.append(dollarCount);
					sql.append("- $");
					sql.append(dollarCount + 1);
					sql.append(")::interval) as pr(period) on teai2.modifiedat between pr.period and pr.period + ");
					sql.append("($");
					sql.append(dollarCount);
					sql.append("- $");
					sql.append(dollarCount + 1);
					sql.append(")::interval");
					tuple.addLocalDateTime(LocalDateTime.parse(tempQuery.getTimeAt(), SerializationTools.informatter));
					tuple.addLocalDateTime(
							LocalDateTime.parse(tempQuery.getEndTimeAt(), SerializationTools.informatter));
					dollarplus = 2;
					break;
				}
			} else {
				sql.append(
						",CASE WHEN (TEAI.raw_modifiedAt - TEAI.raw_createdAt)::interval = '0s'::interval THEN '1s'::interval ELSE (TEAI.raw_modifiedAt - TEAI.raw_createdAt)::interval END) as pr(period) on teai2.modifiedat between pr.period and pr.period + (TEAI.raw_modifiedAt - TEAI.raw_createdAt)::interval");
				dollarplus = 0;
			}
			dollarCount += dollarplus;
		}
		return dollarCount;
	}

	private void attachAggrQueryBottomPart(StringBuilder sql, AggrTerm aggrQuery) {
		sql.append(" GROUP BY pr.period");
		sql.append(")as x), tmp as (SELECT JSONB_BUILD_ARRAY(");
		for (String aggrFunction : aggrQuery.getAggrFunctions()) {
			switch (aggrFunction) {
			case NGSIConstants.AGGR_METH_SUM:
				sql.append("case when Z.sumdata is not null then ");
				sql.append("JSONB_BUILD_OBJECT('" + NGSIConstants.NGSI_LD_SUM + "', ");
				sql.append("JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT('" + NGSIConstants.JSON_LD_LIST + "', Z.sumdata)))");
				break;
			case NGSIConstants.AGGR_METH_MIN:
				sql.append("case when Z.sumdata is not null then ");
				sql.append("JSONB_BUILD_OBJECT('" + NGSIConstants.NGSI_LD_MIN + "', ");
				sql.append("JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT('" + NGSIConstants.JSON_LD_LIST + "', Z.MINDATA)))");
				break;
			case NGSIConstants.AGGR_METH_MAX:
				sql.append("case when Z.sumdata is not null then ");
				sql.append("JSONB_BUILD_OBJECT('" + NGSIConstants.NGSI_LD_MAX + "', ");
				sql.append("JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT('" + NGSIConstants.JSON_LD_LIST + "', Z.MAXDATA)))");

				break;
			case NGSIConstants.AGGR_METH_AVG:
				sql.append("case when Z.sumdata is not null then ");
				sql.append("JSONB_BUILD_OBJECT('" + NGSIConstants.NGSI_LD_AVG + "', ");
				sql.append("JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT('" + NGSIConstants.JSON_LD_LIST + "', Z.AVGDATA)))");
				break;
			case NGSIConstants.AGGR_METH_STDDEV:
				sql.append("case when Z.sumdata is not null then ");
				sql.append("JSONB_BUILD_OBJECT('" + NGSIConstants.NGSI_LD_STDDEV + "', ");
				sql.append(
						"JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT('" + NGSIConstants.JSON_LD_LIST + "', Z.STDDEVDATA)))");
				break;
			case NGSIConstants.AGGR_METH_SUMSQ:
				sql.append("case when Z.sumdata is not null then ");
				sql.append("JSONB_BUILD_OBJECT('" + NGSIConstants.NGSI_LD_SUMSQ + "', ");
				sql.append("JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT('" + NGSIConstants.JSON_LD_LIST + "', Z.SUMSQDATA)))");
				break;
			case NGSIConstants.AGGR_METH_TOTAL_COUNT:
				sql.append("case when Z.sumdata is not null then ");
				sql.append("JSONB_BUILD_OBJECT('" + NGSIConstants.NGSI_LD_TOTALCOUNT + "', ");
				sql.append("JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT('" + NGSIConstants.JSON_LD_LIST
						+ "', Z.TOTALCOUNTDATA)))");
				break;
			case NGSIConstants.AGGR_METH_DISTINCT_COUNT:
				sql.append("case when Z.sumdata is not null then ");
				sql.append("JSONB_BUILD_OBJECT('" + NGSIConstants.NGSI_LD_DISTINCTCOUNT + "', ");
				sql.append("JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT('" + NGSIConstants.JSON_LD_LIST
						+ "', Z.DISTINCTCOUNTDATA)))");
				break;
			default:
				break;
			}
			sql.append("end,");
		}
		sql.setLength(sql.length() - 1);
		// there is no way to just not add anything with jsonb_build_array and case so
		// we need to remove nulls in seperate step but this are at max 7 elements and
		// there is no way to use case with null it will always make a null string out
		// of it because the other things are jsonb
		// it is psqls parsing behaviour ... so it's ok like this
		sql.append(
				") as data FROM Z) SELECT JSONB_AGG(xyz.elem) filter (where not (xyz.elem = 'null')) as data FROM tmp, jsonb_array_elements(tmp.data) as xyz(elem)");

		sql.append(") as u on true ORDER BY TEAI.ID) ");
	}

}