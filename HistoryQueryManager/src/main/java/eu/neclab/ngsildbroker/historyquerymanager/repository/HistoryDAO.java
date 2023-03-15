package eu.neclab.ngsildbroker.historyquerymanager.repository;

import java.time.LocalDateTime;
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
import eu.neclab.ngsildbroker.commons.datatypes.terms.AggrTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.AttrsQueryTerm;
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
			String sql = "with a as (select id , e_types, case when scopes is null then null else getScopeEntry(scopes) end as scopes, jsonb_build_array(jsonb_build_object('@type', 'bladatetime', '@value', to_char(temporalentity.createdat, 'YYYY-MM-DDThh:mm:ss.usZ'))) as r_createdat, jsonb_build_array(jsonb_build_object('@type', 'bladatetime', '@value', to_char(temporalentity.modifiedat, 'YYYY-MM-DDThh:mm:ss.usZ'))) as r_modifiedat, case when deletedat is null then null else jsonb_build_array(jsonb_build_object('@type', 'bladatetime', '@value', to_char(temporalentity.deletedat, 'YYYY-MM-DDThh:mm:ss.usZ')))  end as r_deletedat from temporalentity where id=$1),"
					+ "b as (select a.id as id, temporalentityattrinstance.attributeid as attribid, ";
			if (aggrQuery == null) {
				sql += "jsonb_agg(data) ";
			} else {

			}
			sql += "as data from temporalentityattrinstance, a where temporalentity_id = a.id ";

			if (attrsQuery != null) {

				sql += "AND attributeId in ($" + dollarCount + ")";
				dollarCount++;
				tuple.addArrayOfString(attrsQuery.getAttrs().toArray(new String[0]));
			}

			if (tempQuery != null) {
				sql += "AND " + tempQuery.getTimeProperty();
				switch (tempQuery.getTimerel()) {
				case NGSIConstants.TIME_REL_BEFORE:
					sql += " < $" + dollarCount;
					tuple.addLocalDateTime(LocalDateTime.parse(tempQuery.getTimeAt(), SerializationTools.informatter));
					dollarCount++;
					break;
				case NGSIConstants.TIME_REL_AFTER:
					sql += " > $" + dollarCount;
					tuple.addLocalDateTime(LocalDateTime.parse(tempQuery.getTimeAt(), SerializationTools.informatter));
					dollarCount++;
					break;
				case NGSIConstants.TIME_REL_BETWEEN:
					sql += " between $" + dollarCount + " AND $" + (dollarCount + 1);
					tuple.addLocalDateTime(LocalDateTime.parse(tempQuery.getTimeAt(), SerializationTools.informatter));
					tuple.addLocalDateTime(
							LocalDateTime.parse(tempQuery.getEndTimeAt(), SerializationTools.informatter));
					dollarCount += 2;
					break;
				}
			}

			sql += " group by a.id, attributeid order by a.id limit $" + dollarCount + ") ";
			dollarCount++;
			tuple.addInteger(lastN);
			sql += "select (jsonb_build_object('" + NGSIConstants.JSON_LD_ID + "', b.id, '" + NGSIConstants.JSON_LD_TYPE
					+ "', a.e_types, '" + NGSIConstants.NGSI_LD_CREATED_AT + "', a.r_createdat, '"
					+ NGSIConstants.NGSI_LD_MODIFIED_AT
					+ "', a.r_modifiedat) || jsonb_object_agg(b.attribid, b.data)) || (case when a.r_deletedat is null then '{}'::jsonb else jsonb_build_object('"
					+ NGSIConstants.NGSI_LD_DELETED_AT
					+ "', a.r_deletedat) end) || (case when a.scopes is null then '{}'::jsonb else jsonb_build_object('"
					+ NGSIConstants.NGSI_LD_SCOPE
					+ "', a.scopes) end) from b left join a on a.id = b.id group by b.id, a.e_types, a.r_createdat, a.r_modifiedat, a.r_deletedat, a.scopes";
			return client.preparedQuery(sql).execute(tuple).onItem().transform(rows -> {
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

	public Uni<RowSet<Row>> query(String tenant, Set<String> entityIds, TypeQueryTerm typeQuery, String idPattern,
			AttrsQueryTerm attrsQuery, QQueryTerm qQuery, TemporalQueryTerm tempQuery, AggrTerm aggrQuery, String lang,
			int lastN, boolean count) {
		// TODO Auto-generated method stub
//		return clientManager.getClient(tenant, false).onItem().transformToUni(client -> {
//			StringBuilder query = new StringBuilder("WITH ");
//			char currentChar = 'a';
//			boolean sqlAdded = false;
//			int dollar = 1;
//			Tuple4<Character, String, Integer, List<Object>> tmp;
//			List<Object> tupleItems = Lists.newArrayList();
//			if (typeQuery != null) {
//				tmp = typeQuery.toTemporalSql(currentChar, dollar);
//				currentChar = tmp.getItem1();
//				dollar = tmp.getItem3();
//				tupleItems.addAll(tmp.getItem4());
//				query.append(tmp.getItem2());
//				sqlAdded = true;
//			}
//			if (!entityIds.isEmpty()) {
//				if (sqlAdded) {
//					query.append("),");
//				}
//				query.append(currentChar);
//				currentChar++;
//				query.append(" as (SELECT id as iid FROM temporalentity WHERE ");
//			}
//
//			if (attrsQuery != null) {
//				if (sqlAdded) {
//					query.append("),");
//					try {
//						tmp = attrsQuery.toSql((char) (currentChar + 1), currentChar);
//					} catch (ResponseException e) {
//						return Uni.createFrom().failure(e);
//					}
//				} else {
//					try {
//						tmp = attrsQuery.toSql((char) (currentChar + 1), null);
//					} catch (ResponseException e) {
//						return Uni.createFrom().failure(e);
//					}
//				}
//
//				currentChar = tmp.getItem1();
//				query.append(tmp.getItem2());
//				sqlAdded = true;
//			}
//			if (geoQuery != null) {
//				if (sqlAdded) {
//					query.append("),");
//					try {
//						tmp = geoQuery.toSql((char) (currentChar + 1), currentChar);
//					} catch (ResponseException e) {
//						return Uni.createFrom().failure(e);
//					}
//				} else {
//					try {
//						tmp = geoQuery.toSql((char) (currentChar + 1), null);
//					} catch (ResponseException e) {
//						return Uni.createFrom().failure(e);
//					}
//				}
//				currentChar = tmp.getItem1();
//				query.append(tmp.getItem2());
//				sqlAdded = true;
//			}
//			if (qQuery != null) {
//				if (sqlAdded) {
//					query.append("),");
//					try {
//						tmp = qQuery.toSql((char) (currentChar + 1), currentChar);
//					} catch (ResponseException e) {
//						return Uni.createFrom().failure(e);
//					}
//				} else {
//					try {
//						tmp = qQuery.toSql((char) (currentChar + 1), null);
//					} catch (ResponseException e) {
//						return Uni.createFrom().failure(e);
//					}
//				}
//				currentChar = tmp.getItem1();
//				query.append(tmp.getItem2());
//				sqlAdded = true;
//			}
//			if (ids == null && idPattern == null) {
//				if (limit == 0 && count) {
//					query.append("), SELECT count(");
//					query.append(currentChar);
//					query.append(".iid) as count FROM ");
//					query.append(currentChar);
//					query.append(';');
//				} else {
//					query.append("), entityCount as (SELECT count(");
//					query.append(currentChar);
//					query.append(".iid) as count FROM ");
//					query.append(currentChar);
//					query.append("), ");
//					query.append((char) (currentChar + 1));
//					query.append(" as (SELECT ");
//					query.append(currentChar);
//					query.append(".iid FROM ");
//					query.append(currentChar);
//					query.append(" LIMIT ");
//					query.append(limit);
//					query.append(" OFFSET ");
//					query.append(offSet);
//					currentChar++;
//					query.append("), SELECT entityCount.count As count, entity.entity as entity FROM entityCount, ");
//					query.append(currentChar);
//					query.append(" LEFT JOIN ENTITY ON ");
//					query.append(currentChar);
//					query.append(".iid = ENTITY.ID;");
//				}
//			} else {
//				query.append("), SELECT count(entity.id) as count");
//				if (limit != 0 && count) {
//					query.append(", entity.entity as entity");
//				}
//				query.append(" FROM ");
//				query.append(currentChar);
//				query.append(" LEFT JOIN ENTITY ON ");
//				query.append(currentChar);
//				query.append(".iid = ENTITY.ID WHERE ");
//				if (ids != null) {
//					for (String id : ids) {
//						query.append("ENTITY.E_ID = '");
//						query.append(id);
//						query.append("' or ");
//					}
//					query.setLength(query.length() - 4);
//				}
//				if (idPattern != null) {
//					query.append(idPattern);
//					query.append(" ~ ENTITY.E_ID");
//				}
//				query.append(" LIMIT ");
//				query.append(limit);
//				query.append(" OFFSET ");
//				query.append(offSet);
//				query.append(';');
//			}
//			// TODO at the moment this does no sql escaping. toSql method should return
//			// tuple
//			// with respective values
//			return client.preparedQuery(query.toString()).execute();
//			String sql = "with a as (select id, ('{\"@id\": \"'||e_id||'\", \"" + NGSIConstants.NGSI_LD_CREATED_AT
//					+ "\": [{\"@type\": \"" + NGSIConstants.NGSI_LD_DATE_TIME
//					+ "\", \"@value\": \"'|| createdat||'\"}], \"" + NGSIConstants.NGSI_LD_MODIFIED_AT
//					+ "\": [{\"@type\": \"" + NGSIConstants.NGSI_LD_DATE_TIME
//					+ "\", \"@value\": \"'|| modifiedat||'\"}]}')::jsonb as entity from temporalentity where e_id=$1),\n"
//					+ "b as (select '@type' as key, jsonb_agg(e_type) as value from a left join tempetype2iid on a.id = tempetype2iid.iid group by a.entity),\n";
//			ArrayList<Object> tupleInput = new ArrayList<>();
//			tupleInput.add(entityId);
//			int dollarCount = 2;
//			if (aggrQuery != null) {
//				sql += "c as (select sum(case)...";
//			} else {
//				sql += "c as (select attributeId as key, jsonb_agg(data) as value from a left join temporalentityattrinstance on a.id = temporalentityattrinstance.iid where temporalentityattrinstance.is_toplevel";
//				if (attrsQuery != null || tempQuery != null) {
//					sql += " AND ";
//					if (attrsQuery != null) {
//						sql += "attributeId in ($" + dollarCount + ") ";
//						dollarCount++;
//						tupleInput.add(attrsQuery.getAttrs());
//						if (tempQuery != null) {
//							sql += " AND ";
//						}
//					}
//					if (tempQuery != null) {
//						sql += " and $" + dollarCount;
//						tupleInput.add(tempQuery.getTimeProperty());
//						dollarCount++;
//						switch (tempQuery.getTimerel()) {
//						case NGSIConstants.TIME_REL_BEFORE:
//							sql += " < $" + dollarCount + "::TIMESTAMP ";
//							tupleInput.add(tempQuery.getTimeAt());
//							dollarCount++;
//							break;
//						case NGSIConstants.TIME_REL_AFTER:
//							sql += " > $" + dollarCount + "::TIMESTAMP ";
//							tupleInput.add(tempQuery.getTimeAt());
//							dollarCount++;
//							break;
//						case NGSIConstants.TIME_REL_BETWEEN:
//							sql += " between $" + dollarCount + "::TIMESTAMP AND $" + (dollarCount + 1)
//									+ "::TIMESTAMP ";
//							tupleInput.add(tempQuery.getTimeAt());
//							tupleInput.add(tempQuery.getEndTimeAt());
//							dollarCount += 2;
//							break;
//						}
//					}
//				}
//				sql += "group by attributeId limit $" + dollarCount + ")";
//			}
//
//			sql += ",\nd as (select jsonb_object_agg(c.key, c.value) as attrs FROM c),\ne as (select '"
//					+ NGSIConstants.NGSI_LD_SCOPE
//					+ "' as key, jsonb_agg(jsonb_build_object('@id', e_scope)) as value from b, a left join tempescope2iid on a.id = tempescope2iid.iid where e_scope is not null)\n"
//					+ "select jsonb_build_object(b.key, b.value, e.key, e.value) || d.attrs || a.entity from a, b, d, e";
//
//			return client.preparedQuery(sql.toString()).execute(Tuple.from(tupleInput)).onFailure().retry().atMost(3)
//					.onFailure().recoverWithUni(e -> Uni.createFrom().failure(e));
//		});

//		select id as r_id, e_types as r_types, scopes as r_scopes ,to_char(temporalentity.createdat, 'YYYY-MM-DDThh:mm:ss.usZ') as r_createdat, to_char(temporalentity.modifiedat, 'YYYY-MM-DDThh:mm:ss.usZ') as r_modifiedat, to_char(temporalentity.deletedat, 'YYYY-MM-DDThh:mm:ss.usZ')  as r_deletedat, jsonb_build_object(temporalentityattrinstance.attributeid, jsonb_agg(data)) from temporalentity, temporalentityattrinstance where id = temporalentityattrinstance.temporalentity_id GROUP BY r_id, r_types, r_scopes, r_createdat, r_modifiedat, r_deletedat, temporalentityattrinstance.attributeid
		return null;
	}

}