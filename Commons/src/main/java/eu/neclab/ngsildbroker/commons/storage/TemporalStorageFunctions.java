package eu.neclab.ngsildbroker.commons.storage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.neclab.ngsildbroker.commons.constants.DBConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.GeoqueryRel;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.StorageFunctionsInterface;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple3;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.SqlConnection;
import io.vertx.mutiny.sqlclient.Tuple;
import io.vertx.pgclient.PgException;

import static eu.neclab.ngsildbroker.commons.interfaces.StorageFunctionsInterface.getSQLList;

public class TemporalStorageFunctions implements StorageFunctionsInterface {
	protected final static Logger logger = LoggerFactory.getLogger(TemporalStorageFunctions.class);

	protected final static String DBCOLUMN_HISTORY_ENTITY_ID = "id";
	protected final static String DBCOLUMN_HISTORY_ENTITY_TYPE = "type";
	protected final static String DBCOLUMN_HISTORY_ATTRIBUTE_ID = "attributeid";
	protected final static String DBCOLUMN_HISTORY_INSTANCE_ID = "instanceid";

	protected final static Map<String, String> NGSILD_TO_SQL_RESERVED_PROPERTIES_MAPPING_TIME = initNgsildToSqlReservedPropertiesMappingTime();

	protected static Map<String, String> initNgsildToSqlReservedPropertiesMappingTime() {
		Map<String, String> map = new HashMap<>();
		map.put(NGSIConstants.NGSI_LD_CREATED_AT, DBConstants.DBCOLUMN_CREATED_AT);
		map.put(NGSIConstants.NGSI_LD_MODIFIED_AT, DBConstants.DBCOLUMN_MODIFIED_AT);
		map.put(NGSIConstants.NGSI_LD_OBSERVED_AT, DBConstants.DBCOLUMN_OBSERVED_AT);
		return Collections.unmodifiableMap(map);
	}

	@Override
	public Uni<RowSet<Row>> translateNgsildQueryToSql(QueryParams qp, SqlConnection conn) {

		Tuple3<String, ArrayList<Object>, Integer> fullSqlWhereProperty;
		try {
			fullSqlWhereProperty = commonTranslateSql(qp, 1);
		} catch (ResponseException e) {
			return Uni.createFrom().failure(e);
		}
		int limit = qp.getLimit();
		int offSet = qp.getOffSet();
		String query = fullSqlWhereProperty.getItem1();
		if (limit > 0) {
			query += "LIMIT " + limit + " ";
		}
		if (offSet > 0) {
			query += "OFFSET " + offSet + " ";
		}
		return conn.preparedQuery(query).execute(Tuple.from(fullSqlWhereProperty.getItem2())).onFailure()
				.recoverWithUni(e -> {
					switch (((PgException) e).getCode()) {
						case "22P02":
							return Uni.createFrom().failure(new ResponseException(ErrorType.BadRequestData,
									"You have a format error in your request maybe you missed a quote on a string"));
						default:
							return Uni.createFrom().failure(e);
					}
				});
	}

	private Tuple3<String, ArrayList<Object>, Integer> getSqlWhereForField(String dbColumn, String value,
			int currentCount) {
		String sqlWhere = "";
		int newCount = currentCount;
		ArrayList<Object> replacements = Lists.newArrayList();
		if (value.indexOf(",") == -1) {
			sqlWhere = dbColumn + " = $" + newCount;
			replacements.add(value);
			newCount++;

		} else {
			Tuple3<String, ArrayList<Object>, Integer> tmp = getSQLList(value, newCount);
			newCount = tmp.getItem3();
			replacements.addAll(tmp.getItem2());
			sqlWhere = dbColumn + " IN (" + tmp.getItem1() + ")";
		}
		return Tuple3.of(sqlWhere, replacements, newCount);
	}

	protected Tuple3<String, ArrayList<Object>, Integer> translateNgsildTimequeryToSql(String timerel, String time,
			String timeproperty, String endTime, String dbPrefix, int currentCount) throws ResponseException {
		int newCount = currentCount;
		ArrayList<Object> replacements = Lists.newArrayList();
		StringBuilder sqlWhere = new StringBuilder(50);

		String sqlTestStatic = dbPrefix + "static = true AND ";

		String dbColumn = NGSILD_TO_SQL_RESERVED_PROPERTIES_MAPPING_TIME.get(timeproperty);
		if (dbColumn == null) {
			sqlTestStatic += "data ? $" + newCount + " = false";
			dbColumn = "(" + dbPrefix + "data#>>'{$" + newCount + ",0," + NGSIConstants.NGSI_LD_HAS_VALUE
					+ ",0,@value}')::timestamp ";
			newCount++;
			replacements.add(timeproperty);
		} else {
			dbColumn = dbPrefix + dbColumn;
			sqlTestStatic += dbColumn + " IS NULL";
		}

		sqlWhere.append("( (" + sqlTestStatic + ") OR "); // temporal filters do not apply to static attributes

		switch (timerel) {
			case NGSIConstants.TIME_REL_BEFORE:
				sqlWhere.append(dbColumn + DBConstants.SQLQUERY_LESSEQ + " $" + newCount + "::timestamp");
				newCount++;
				replacements.add(SerializationTools.localDateTimeFormatter(HttpUtils.utfDecoder(time)));
				break;
			case NGSIConstants.TIME_REL_AFTER:
				sqlWhere.append(dbColumn + DBConstants.SQLQUERY_GREATEREQ + " $" + newCount + "::timestamp");
				newCount++;
				replacements.add(SerializationTools.localDateTimeFormatter(HttpUtils.utfDecoder(time)));
				break;
			case NGSIConstants.TIME_REL_BETWEEN:
				sqlWhere.append(dbColumn + " BETWEEN $" + newCount + "::timestamp AND $");
				newCount++;
				replacements.add(SerializationTools.localDateTimeFormatter(HttpUtils.utfDecoder(time)));
				sqlWhere.append(newCount + "::timestamp");
				newCount++;
				replacements.add(SerializationTools.localDateTimeFormatter(HttpUtils.utfDecoder(endTime)));
				break;
			default:
				throw new ResponseException(ErrorType.BadRequestData, "Invalid georel operator: " + timerel);
		}
		sqlWhere.append(")");
		return Tuple3.of(sqlWhere.toString(), replacements, newCount);
	}

	@Override
	public Uni<RowSet<Row>> translateNgsildQueryToCountResult(QueryParams qp, SqlConnection conn) {

		Tuple3<String, ArrayList<Object>, Integer> fullSqlWhereProperty;
		try {
			fullSqlWhereProperty = commonTranslateSql(qp, 1);
		} catch (ResponseException e) {
			return Uni.createFrom().failure(e);
		}
		String sqlQuery = "SELECT Count(*) FROM ";
		if (fullSqlWhereProperty.getItem1().length() > 0) {
			sqlQuery += "( " + fullSqlWhereProperty.getItem1() + " )AS foo";
		}
		return conn.preparedQuery(sqlQuery).execute(Tuple.from(fullSqlWhereProperty.getItem2()));
	}

	protected Tuple3<String, ArrayList<Object>, Integer> commonTranslateSql(QueryParams qp, int currentCount)
			throws ResponseException {
		int newCount = currentCount;
		ArrayList<Object> replacements = Lists.newArrayList();
		StringBuilder fullSqlWhere = new StringBuilder(70);
		String sqlWhereGeoquery = "";
		String sqlWhere = "";
		String sqlQuery = "";
		Tuple3<String, ArrayList<Object>, Integer> tmp;
		List<Map<String, String>> entities = qp.getEntities();
		if (entities != null && entities.size() > 0) {
			for (Map<String, String> entityInfo : entities) {
				fullSqlWhere.append("(");
				for (Entry<String, String> entry : entityInfo.entrySet()) {
					switch (entry.getKey()) {
						case NGSIConstants.JSON_LD_ID:
							tmp = getSqlWhereForField("te." + DBCOLUMN_HISTORY_ENTITY_ID, entry.getValue(), newCount);
							newCount = tmp.getItem3();
							replacements.addAll(tmp.getItem2());
							fullSqlWhere.append(tmp.getItem1());
							break;
						case NGSIConstants.JSON_LD_TYPE:
							tmp = getSqlWhereForField("te." + DBCOLUMN_HISTORY_ENTITY_TYPE, entry.getValue(), newCount);
							newCount = tmp.getItem3();
							replacements.addAll(tmp.getItem2());
							fullSqlWhere.append(tmp.getItem1());

							break;
						case NGSIConstants.NGSI_LD_ID_PATTERN:
							fullSqlWhere.append("te." + DBCOLUMN_HISTORY_ENTITY_ID + " ~ $" + newCount);
							newCount++;
							replacements.add(entry.getValue());
							break;
						default:
							break;
					}
					fullSqlWhere.append(" AND ");
				}
				fullSqlWhere.delete(fullSqlWhere.length() - 5, fullSqlWhere.length() - 1);
				fullSqlWhere.append(") OR ");
			}
			fullSqlWhere.delete(fullSqlWhere.length() - 4, fullSqlWhere.length() - 1);
			fullSqlWhere.append(" AND ");
		}

		if (qp.getAttrs() != null) {
			tmp = getSqlWhereForField("teai." + DBCOLUMN_HISTORY_ATTRIBUTE_ID, qp.getAttrs(), newCount);
			newCount = tmp.getItem3();
			replacements.addAll(tmp.getItem2());
			fullSqlWhere.append(tmp.getItem1() + " AND ");
		}
		if (qp.getInstanceId() != null) {
			tmp = getSqlWhereForField("teai." + DBCOLUMN_HISTORY_INSTANCE_ID, qp.getInstanceId(), newCount);
			newCount = tmp.getItem3();
			replacements.addAll(tmp.getItem2());
			fullSqlWhere.append(tmp.getItem1() + " AND ");
		}
		if (qp.getScopeQ() != null) {
			sqlWhere = qp.getScopeQ();
			fullSqlWhere.append("te.");
			fullSqlWhere.append(sqlWhere);
			fullSqlWhere.append(" AND ");
		}
		// temporal query
		if (qp.getTimerel() != null) {
			tmp = translateNgsildTimequeryToSql(qp.getTimerel(), qp.getTimeAt(), qp.getTimeproperty(),
					qp.getEndTimeAt(), "teai.", newCount);
			sqlWhere = tmp.getItem1();
			newCount = tmp.getItem3();
			replacements.addAll(tmp.getItem2());
			fullSqlWhere.append(sqlWhere + " AND ");
		}

		// geoquery
		if (qp.getGeorel() != null) {
			GeoqueryRel gqr = qp.getGeorel();
			logger.debug("Georel value " + gqr.getGeorelOp());
			tmp = translateNgsildGeoqueryToPostgisQuery(gqr, qp.getGeometry(), qp.getCoordinates(), qp.getGeoproperty(),
					newCount);
			sqlWhere = tmp.getItem1();
			newCount = tmp.getItem3();
			replacements.addAll(tmp.getItem2());
			if (!sqlWhere.isEmpty()) {
				tmp = translateNgsildTimequeryToSql(qp.getTimerel(), qp.getTimeAt(), qp.getTimeproperty(),
						qp.getEndTimeAt(), "", newCount);
				newCount = tmp.getItem3();
				replacements.addAll(tmp.getItem2());
				sqlWhereGeoquery = "where exists (" + "  select 1 " + "  from temporalentityattrinstance "
						+ "  where temporalentity_id = r.id and " + "        attributeid = $" + newCount + " and "
						+ "        attributetype = '" + NGSIConstants.NGSI_LD_GEOPROPERTY + "' and " + tmp.getItem1()
						+ " and " + sqlWhere + ") ";
				newCount++;
				replacements.add(qp.getGeoproperty());
			}
		}
		if (qp.getLastN() > 0) {
			sqlQuery = "with r as ("
					+ "  select te.id, te.type, te.createdat, te.modifiedat, coalesce(teai.attributeid, '') as attributeid, array_to_json((array_agg(teai.data";
		} else {
			sqlQuery = "with r as ("
					+ "  select te.id, te.type, te.createdat, te.modifiedat, coalesce(teai.attributeid, '') as attributeid, jsonb_agg(teai.data";
		}
		if (!qp.getIncludeSysAttrs()) {
			sqlQuery += "  - '" + NGSIConstants.NGSI_LD_CREATED_AT + "' - '" + NGSIConstants.NGSI_LD_MODIFIED_AT + "'";
		}
		if (qp.getLastN() > 0) {
			sqlQuery += " order by teai.observedat desc)) [-5: " + qp.getLastN() + "]) as attributedata" + "  from "
					+ DBConstants.DBTABLE_TEMPORALENTITY + " te" + "  left join "
					+ DBConstants.DBTABLE_TEMPORALENTITY_ATTRIBUTEINSTANCE + " teai on (teai.temporalentity_id = te.id)"
					+ "  where ";

		} else {
			sqlQuery += " order by teai.modifiedat desc) as attributedata" + "  from "
					+ DBConstants.DBTABLE_TEMPORALENTITY + " te" + "  left join "
					+ DBConstants.DBTABLE_TEMPORALENTITY_ATTRIBUTEINSTANCE + " teai on (teai.temporalentity_id = te.id)"
					+ "  where ";
		}
		sqlQuery += fullSqlWhere.substring(0, fullSqlWhere.length() - 5); // remove the last AND
		sqlQuery += " group by te.id, te.type, te.createdat, te.modifiedat, teai.attributeid order by te.id, teai.attributeid ) select tedata || case when attrdata <> '{\"\": [null]}'::jsonb then attrdata else tedata end as data from ( select id, ('{\""
				+ NGSIConstants.JSON_LD_ID + "\":\"' || id || '\"}')::jsonb || ('{\"" + NGSIConstants.JSON_LD_TYPE
				+ "\":[\"' || type || '\"]}')::jsonb ";
		if (qp.getIncludeSysAttrs()) {
			sqlQuery += "|| ('{\"" + NGSIConstants.NGSI_LD_CREATED_AT + "\":";
			sqlQuery += "[ { \"" + NGSIConstants.JSON_LD_TYPE + "\": \"" + NGSIConstants.NGSI_LD_DATE_TIME + "\", \""
					+ NGSIConstants.JSON_LD_VALUE + "\": \"' ";
			sqlQuery += "|| to_char(createdat, 'YYYY-MM-DD\"T\"HH24:MI:SS.ssssss\"Z\"') || '\"";
			sqlQuery += "}]";
			sqlQuery += "}')::jsonb || ";
			sqlQuery += " ('{\"" + NGSIConstants.NGSI_LD_MODIFIED_AT + "\":";
			sqlQuery += "[ { \"" + NGSIConstants.JSON_LD_TYPE + "\": \"" + NGSIConstants.NGSI_LD_DATE_TIME + "\", \""
					+ NGSIConstants.JSON_LD_VALUE + "\": \"' ";
			sqlQuery += "|| to_char(modifiedat, 'YYYY-MM-DD\"T\"HH24:MI:SS.ssssss\"Z\"') || '\"";
			sqlQuery += "}]";
			sqlQuery += "}')::jsonb";
		}
		sqlQuery += "  as tedata, " + "jsonb_object_agg(attributeid,";
		if (qp.getTemporalValues()) {
			String timeProperty = qp.getTimeproperty();
			if (timeProperty == null || timeProperty.equalsIgnoreCase(NGSIConstants.NGSI_LD_OBSERVED_AT)) {
				sqlQuery += "                    jsonb_build_object(\r\n'" + NGSIConstants.JSON_LD_TYPE + "',\r\n"
						+ "                        (\r\n" + "                            select\r\n"
						+ "                                json_agg(jsonb(t ->'" + NGSIConstants.JSON_LD_TYPE
						+ "'))\r\n" + "                            from\r\n"
						+ "                                jsonb_array_elements(attributedata) as x (t)\r\n"
						+ "                        ) ->0\r\n" + "                    ) || jsonb_build_object(\r\n"
						+ "                        'values',\r\n" + "                        (\r\n"
						+ "                            select\r\n" + "                                json_agg(\r\n"
						+ "									json_build_array("
						+ "                                    json_build_array(\r\n"
						+ "                                        t ->'" + NGSIConstants.NGSI_LD_HAS_VALUE
						+ "' -> 0 ->'" + NGSIConstants.JSON_LD_VALUE + "',\r\n"
						+ "                                        t ->'" + NGSIConstants.NGSI_LD_OBSERVED_AT
						+ "' -> 0 ->'" + NGSIConstants.JSON_LD_VALUE + "'\r\n"
						+ "                                    )\r\n" + "                                   )\r\n"
						+ "                                )\r\n" + "                            from\r\n"
						+ "                                jsonb_array_elements(attributedata) as x (t)\r\n))";
			} else {
				if (timeProperty.equalsIgnoreCase(NGSIConstants.NGSI_LD_MODIFIED_AT)) {
					sqlQuery += "                    jsonb_build_object(\r\n'" + NGSIConstants.JSON_LD_TYPE + "',\r\n"
							+ "                        (\r\n" + "                            select\r\n"
							+ "                                json_agg(jsonb(t ->'" + NGSIConstants.JSON_LD_TYPE
							+ "'))\r\n" + "                            from\r\n"
							+ "                                jsonb_array_elements(attributedata) as x (t)\r\n"
							+ "                        ) ->0\r\n" + "                    ) || jsonb_build_object(\r\n"
							+ "                        'values',\r\n" + "                        (\r\n"
							+ "                            select\r\n" + "                                json_agg(\r\n"
							+ "									json_build_array("
							+ "                                    json_build_array(\r\n"
							+ "                                        t ->'" + NGSIConstants.NGSI_LD_HAS_VALUE
							+ "' -> 0 ->'" + NGSIConstants.JSON_LD_VALUE + "',\r\n"
							+ "                                        t ->'" + NGSIConstants.NGSI_LD_MODIFIED_AT
							+ "' -> 0 ->'" + NGSIConstants.JSON_LD_VALUE + "'\r\n"
							+ "                                    )\r\n" + "                                  )\r\n"
							+ "                                )\r\n" + "                            from\r\n"
							+ "                                jsonb_array_elements(attributedata) as x (t)\r\n))";

				} else if (timeProperty.equalsIgnoreCase(NGSIConstants.NGSI_LD_CREATED_AT)) {
					sqlQuery += "                    jsonb_build_object(\r\n'" + NGSIConstants.JSON_LD_TYPE + "',\r\n"
							+ "                        (\r\n" + "                            select\r\n"
							+ "                                json_agg(jsonb(t ->'" + NGSIConstants.JSON_LD_TYPE
							+ "'))\r\n" + "                            from\r\n"
							+ "                                jsonb_array_elements(attributedata) as x (t)\r\n"
							+ "                        ) ->0\r\n" + "                    ) || jsonb_build_object(\r\n"
							+ "                        'values',\r\n" + "                        (\r\n"
							+ "                            select\r\n" + "                                json_agg(\r\n"
							+ "									json_build_array("
							+ "                                    json_build_array(\r\n"
							+ "                                        t ->'" + NGSIConstants.NGSI_LD_HAS_VALUE
							+ "' -> 0 ->'" + NGSIConstants.JSON_LD_VALUE + "',\r\n"
							+ "                                        t ->'" + NGSIConstants.NGSI_LD_CREATED_AT
							+ "' -> 0 ->'" + NGSIConstants.JSON_LD_VALUE + "'\r\n"
							+ "                                    )\r\n" + "                                    )\r\n"
							+ "                                )\r\n" + "                            from\r\n"
							+ "                                jsonb_array_elements(attributedata) as x (t)\r\n))";

				} else {
					throw new ResponseException(ErrorType.BadRequestData, "Invalid timeproperty");

				}
			}
		} else {
			sqlQuery += "attributedata";
		}

		sqlQuery += ") as attrdata " + "  from r ";
		sqlQuery += sqlWhereGeoquery;
		sqlQuery += "  group by id, type, createdat, modifiedat ";
		sqlQuery += "  order by modifiedat desc ";
		sqlQuery += ") as m" + " ";

		// advanced query "q"
		// THIS DOESN'T WORK
		if (qp.getQ() != null) {
			sqlQuery += " where " + qp.getQ() + " ";
		}
		return Tuple3.of(sqlQuery, replacements, newCount);

	}

	@Override
	public Tuple3<String, ArrayList<Object>, Integer> translateNgsildGeoqueryToPostgisQuery(GeoqueryRel georel,
			String geometry, String coordinates, String geoproperty, int currentCount) throws ResponseException {
		StringBuilder sqlWhere = new StringBuilder(50);

		String georelOp = georel.getGeorelOp();
		logger.trace("  Geoquery term georelOp: " + georelOp);
		String dbColumn = "geovalue";
		String referenceValue = "ST_SetSRID(ST_GeomFromGeoJSON('{\"type\": \"" + geometry + "\", \"coordinates\": "
				+ coordinates + " }'), 4326)";
		String sqlPostgisFunction = DBConstants.NGSILD_TO_POSTGIS_GEO_OPERATORS_MAPPING.get(georelOp);

		switch (georelOp) {
			case NGSIConstants.GEO_REL_NEAR:
				if (georel.getDistanceType() != null && georel.getDistanceValue() != null) {
					if (georel.getDistanceType().equals(NGSIConstants.GEO_REL_MIN_DISTANCE))
						sqlWhere.append("NOT ");
					sqlWhere.append(sqlPostgisFunction + "( " + dbColumn + "::geography, " + referenceValue
							+ "::geography, " + georel.getDistanceValue() + ", false) ");
				} else {
					throw new ResponseException(ErrorType.BadRequestData,
							"GeoQuery: Type and distance are required for near relation");
				}
				break;
			case NGSIConstants.GEO_REL_WITHIN:
			case NGSIConstants.GEO_REL_CONTAINS:
			case NGSIConstants.GEO_REL_OVERLAPS:
			case NGSIConstants.GEO_REL_INTERSECTS:
			case NGSIConstants.GEO_REL_EQUALS:
			case NGSIConstants.GEO_REL_DISJOINT:
				sqlWhere.append(sqlPostgisFunction + "( " + dbColumn + ", " + referenceValue + ") ");
				break;
			default:
				throw new ResponseException(ErrorType.BadRequestData, "Invalid georel operator: " + georelOp);
		}
		return Tuple3.of(sqlWhere.toString(), Lists.newArrayList(), currentCount);
	}

	@Override
	public Uni<RowSet<Row>> typesAndAttributeQuery(QueryParams qp, SqlConnection conn) {
		return Uni.createFrom().nullItem();
	}

	@Override
	public String getAllIdsQuery() {
		return "SELECT DISTINCT id FROM entity";
	}

	@Override
	public String getEntryQuery() {
		return "SELECT data FROM entity WHERE id=$1";
	}

}
