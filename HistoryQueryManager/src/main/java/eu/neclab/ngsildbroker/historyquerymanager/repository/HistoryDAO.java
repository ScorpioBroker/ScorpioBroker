package eu.neclab.ngsildbroker.historyquerymanager.repository;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.RegistrationEntry;
import eu.neclab.ngsildbroker.commons.datatypes.results.QueryResult;
import eu.neclab.ngsildbroker.commons.datatypes.terms.AggrTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.AttrsQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.LanguageQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.QQueryTerm;
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
					"with a as (select id , e_types, case when scopes is null then null else getScopeEntry(scopes) end as scopes, jsonb_build_array(jsonb_build_object('@type', 'bladatetime', '@value', to_char(temporalentity.createdat, 'YYYY-MM-DDThh:mm:ss.usZ'))) as r_createdat, jsonb_build_array(jsonb_build_object('@type', 'bladatetime', '@value', to_char(temporalentity.modifiedat, 'YYYY-MM-DDThh:mm:ss.usZ'))) as r_modifiedat, case when deletedat is null then null else jsonb_build_array(jsonb_build_object('@type', 'bladatetime', '@value', to_char(temporalentity.deletedat, 'YYYY-MM-DDThh:mm:ss.usZ')))  end as r_deletedat from temporalentity where id=$1),"
							+ "b as (SELECT DISTINCT TEAI.ID AS ID, TEAI.ATTRIBUTEID AS ATTRIBID, u.data as data "
							+ "FROM (A LEFT JOIN TEMPORALENTITYATTRINSTANCE on A.id = TEMPORALENTITYATTRINSTANCE.temporalentity_id) as TEAI LEFT JOIN LATERAL (SELECT "
							+ "jsonb_agg(x.data order by x.modifiedat) as data from (Select modifiedat, ");

			if (aggrQuery == null) {
				sql.append("data");
			} else {

			}
			sql.append(" from TEMPORALENTITYATTRINSTANCE TEAI2 WHERE TEAI.ATTRIBUTEID = TEAI2.ATTRIBUTEID ");

			if (attrsQuery != null) {

				sql.append("AND TEAI2.attributeId in ($" + dollarCount + ")");
				dollarCount++;
				tuple.addArrayOfString(attrsQuery.getAttrs().toArray(new String[0]));
			}

			if (tempQuery != null) {
				sql.append("AND TEAI2.");
				dollarCount = tempQuery.toSql(sql, tuple, dollarCount);

			}
			sql.append(" ORDER BY TEAI2.modifiedat LIMIT $");
			sql.append(dollarCount);
			sql.append(") as x) as u on true ORDER BY TEAI.ID) ");
			dollarCount++;
			tuple.addInteger(lastN);
			sql.append("select (jsonb_build_object('" + NGSIConstants.JSON_LD_ID + "', b.id, '"
					+ NGSIConstants.JSON_LD_TYPE + "', a.e_types, '" + NGSIConstants.NGSI_LD_CREATED_AT
					+ "', a.r_createdat, '" + NGSIConstants.NGSI_LD_MODIFIED_AT
					+ "', a.r_modifiedat) || jsonb_object_agg(b.attribid, b.data)) || (case when a.r_deletedat is null then '{}'::jsonb else jsonb_build_object('"
					+ NGSIConstants.NGSI_LD_DELETED_AT
					+ "', a.r_deletedat) end) || (case when a.scopes is null then '{}'::jsonb else jsonb_build_object('"
					+ NGSIConstants.NGSI_LD_SCOPE
					+ "', a.scopes) end) from b left join a on a.id = b.id group by b.id, a.e_types, a.r_createdat, a.r_modifiedat, a.r_deletedat, a.scopes");

			System.out.println(sql.toString());
			return client.preparedQuery(sql.toString()).execute(tuple).onItem().transform(rows -> {
				if (rows.size() == 0) {
					return new HashMap<>(0);
				}
				return rows.iterator().next().getJsonObject(0).getMap();
			});
		});

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
			LanguageQueryTerm langQuery, int lastN, int limit, int offset, boolean count) {
		return clientManager.getClient(tenant, false).onItem().transformToUni(client -> {
			Tuple tuple = Tuple.tuple();
			int dollarCount = 1;
			StringBuilder sql = new StringBuilder(
					"with a as (select id , e_types, case when scopes is null then null else getScopeEntry(scopes) end as scopes, jsonb_build_array(jsonb_build_object('@type', 'bladatetime', '@value', to_char(temporalentity.createdat, 'YYYY-MM-DDThh:mm:ss.usZ'))) as r_createdat, jsonb_build_array(jsonb_build_object('@type', 'bladatetime', '@value', to_char(temporalentity.modifiedat, 'YYYY-MM-DDThh:mm:ss.usZ'))) as r_modifiedat, case when deletedat is null then null else jsonb_build_array(jsonb_build_object('@type', 'bladatetime', '@value', to_char(temporalentity.deletedat, 'YYYY-MM-DDThh:mm:ss.usZ')))  end as r_deletedat from temporalentity where 1=1");

			if (typeQuery != null) {
				sql.append(" AND ");
				dollarCount = typeQuery.toSql(sql, tuple, dollarCount);
			}
			if (entityIds != null) {
				sql.append("AND id in ($");
				sql.append(dollarCount);
				sql.append(") ");
				dollarCount++;
				tuple.addArrayOfString(entityIds);
			}
			if (idPattern != null) {
				sql.append("AND id ~ $");
				sql.append(dollarCount);
				sql.append(" ");
				dollarCount++;
				tuple.addString(idPattern);
			}
			sql.append("), b as (select a.id as id, temporalentityattrinstance.attributeid as attribid, ");
			if (aggrQuery == null) {
				sql.append("jsonb_agg(data) ");
			} else {

			}
			sql.append("as data from temporalentityattrinstance, a where temporalentity_id = a.id");

			if (attrsQuery != null) {
				sql.append(" AND attributeId in ($" + dollarCount + ")");
				dollarCount++;
				tuple.addArrayOfString(attrsQuery.getAttrs().toArray(new String[0]));
			}

			if (tempQuery != null) {
				sql.append(" AND " + tempQuery.getTimeProperty());
				switch (tempQuery.getTimerel()) {
				case NGSIConstants.TIME_REL_BEFORE:
					sql.append(" < $" + dollarCount);
					tuple.addLocalDateTime(LocalDateTime.parse(tempQuery.getTimeAt(), SerializationTools.informatter));
					dollarCount++;
					break;
				case NGSIConstants.TIME_REL_AFTER:
					sql.append(" > $" + dollarCount);
					tuple.addLocalDateTime(LocalDateTime.parse(tempQuery.getTimeAt(), SerializationTools.informatter));
					dollarCount++;
					break;
				case NGSIConstants.TIME_REL_BETWEEN:
					sql.append(" between $" + dollarCount + " AND $" + (dollarCount + 1));
					tuple.addLocalDateTime(LocalDateTime.parse(tempQuery.getTimeAt(), SerializationTools.informatter));
					tuple.addLocalDateTime(
							LocalDateTime.parse(tempQuery.getEndTimeAt(), SerializationTools.informatter));
					dollarCount += 2;
					break;
				}
			}
			if (qQuery != null) {

			}
			if (langQuery != null) {

			}

			sql.append(" group by a.id, attributeid order by a.id limit $" + dollarCount + ") ");
			dollarCount++;
			tuple.addInteger(lastN);
			sql.append("select (jsonb_build_object('" + NGSIConstants.JSON_LD_ID + "', b.id, '"
					+ NGSIConstants.JSON_LD_TYPE + "', a.e_types, '" + NGSIConstants.NGSI_LD_CREATED_AT
					+ "', a.r_createdat, '" + NGSIConstants.NGSI_LD_MODIFIED_AT
					+ "', a.r_modifiedat) || jsonb_object_agg(b.attribid, b.data)) || (case when a.r_deletedat is null then '{}'::jsonb else jsonb_build_object('"
					+ NGSIConstants.NGSI_LD_DELETED_AT
					+ "', a.r_deletedat) end) || (case when a.scopes is null then '{}'::jsonb else jsonb_build_object('"
					+ NGSIConstants.NGSI_LD_SCOPE
					+ "', a.scopes) end) from b left join a on a.id = b.id group by b.id, a.e_types, a.r_createdat, a.r_modifiedat, a.r_deletedat, a.scopes");
			sql.append(" LIMIT $");
			sql.append(dollarCount);
			dollarCount++;
			sql.append(" OFFSET $");
			sql.append(dollarCount);
			dollarCount++;
			tuple.addInteger(limit);
			tuple.addInteger(offset);
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

}