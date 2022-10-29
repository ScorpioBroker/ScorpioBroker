package eu.neclab.ngsildbroker.commons.storage;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import eu.neclab.ngsildbroker.commons.constants.DBConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.GeoqueryRel;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.StorageFunctionsInterface;

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
	public String translateNgsildQueryToSql(QueryParams qp) throws ResponseException {

		String fullSqlWhereProperty = commonTranslateSql(qp);
		int limit = qp.getLimit();
		int offSet = qp.getOffSet();
		if (limit > 0) {
			fullSqlWhereProperty += "LIMIT " + limit + " ";
		}
		if (offSet > 0) {
			fullSqlWhereProperty += "OFFSET " + offSet + " ";
		}
		return fullSqlWhereProperty;
	}

	private String getSqlWhereForField(String dbColumn, String value) {
		String sqlWhere = "";
		if (value.indexOf(",") == -1) {
			sqlWhere = dbColumn + "='" + value + "'";
		} else {
			sqlWhere = dbColumn + " IN ('" + value.replace(",", "','") + "')";
		}
		return sqlWhere;
	}

	protected String translateNgsildTimequeryToSql(String timerel, String time, String timeproperty, String endTime,
			String dbPrefix) throws ResponseException {
		StringBuilder sqlWhere = new StringBuilder(50);

		String sqlTestStatic = dbPrefix + "static = true AND ";

		String dbColumn = NGSILD_TO_SQL_RESERVED_PROPERTIES_MAPPING_TIME.get(timeproperty);
		if (dbColumn == null) {
			sqlTestStatic += "data?'" + timeproperty + "' = false";
			dbColumn = "(" + dbPrefix + "data#>>'{" + timeproperty + ",0," + NGSIConstants.NGSI_LD_HAS_VALUE
					+ ",0,@value}')::timestamp ";
		} else {
			dbColumn = dbPrefix + dbColumn;
			sqlTestStatic += dbColumn + " IS NULL";
		}

		sqlWhere.append("( (" + sqlTestStatic + ") OR "); // temporal filters do not apply to static attributes

		switch (timerel) {
		case NGSIConstants.TIME_REL_BEFORE:
			sqlWhere.append(dbColumn + DBConstants.SQLQUERY_LESSEQ + " '" + time + "'::timestamp");
			break;
		case NGSIConstants.TIME_REL_AFTER:
			sqlWhere.append(dbColumn + DBConstants.SQLQUERY_GREATEREQ + " '" + time + "'::timestamp");
			break;
		case NGSIConstants.TIME_REL_BETWEEN:
			sqlWhere.append(dbColumn + " BETWEEN '" + time + "'::timestamp AND '" + endTime + "'::timestamp");
			break;
		default:
			throw new ResponseException(ErrorType.BadRequestData, "Invalid georel operator: " + timerel);
		}
		sqlWhere.append(")");
		return sqlWhere.toString();
	}

	@Override
	public String translateNgsildQueryToCountResult(QueryParams qp) throws ResponseException {

		String fullSqlWhereProperty = commonTranslateSql(qp);
		String sqlQuery = "SELECT Count(*) FROM ";
		if (fullSqlWhereProperty.length() > 0) {
			sqlQuery += "( " + fullSqlWhereProperty.toString() + " )AS foo";
		}
		return sqlQuery;
	}

	protected String commonTranslateSql(QueryParams qp) throws ResponseException {
		StringBuilder fullSqlWhere = new StringBuilder(70);
		String sqlWhereGeoquery = "";
		String sqlWhere = "";
		String sqlQuery = "";
		List<Map<String, String>> entities = qp.getEntities();
		if (entities != null && entities.size() > 0) {
			for (Map<String, String> entityInfo : entities) {
				fullSqlWhere.append("(");
				for (Entry<String, String> entry : entityInfo.entrySet()) {
					switch (entry.getKey()) {
					case NGSIConstants.JSON_LD_ID:
						fullSqlWhere.append(getSqlWhereForField("te." + DBCOLUMN_HISTORY_ENTITY_ID, entry.getValue()));
						break;
					case NGSIConstants.JSON_LD_TYPE:
						fullSqlWhere
								.append(getSqlWhereForField("te." + DBCOLUMN_HISTORY_ENTITY_TYPE, entry.getValue()));
						break;
					case NGSIConstants.NGSI_LD_ID_PATTERN:
						fullSqlWhere.append("te." + DBCOLUMN_HISTORY_ENTITY_ID + " ~ '" + entry.getValue() + "'");
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
			sqlWhere = getSqlWhereForField("teai." + DBCOLUMN_HISTORY_ATTRIBUTE_ID, qp.getAttrs());
			fullSqlWhere.append(sqlWhere + " AND ");
		}
		if (qp.getInstanceId() != null) {
			sqlWhere = getSqlWhereForField("teai." + DBCOLUMN_HISTORY_INSTANCE_ID, qp.getInstanceId());
			fullSqlWhere.append(sqlWhere + " AND ");
		}
		if (qp.getScopeQ() != null) {
			sqlWhere = qp.getScopeQ();
			fullSqlWhere.append("te.");
			fullSqlWhere.append(sqlWhere);
			fullSqlWhere.append(" AND ");
		}
		// temporal query
		if (qp.getTimerel() != null) {
			sqlWhere = translateNgsildTimequeryToSql(qp.getTimerel(), qp.getTimeAt(), qp.getTimeproperty(),
					qp.getEndTimeAt(), "teai.");
			fullSqlWhere.append(sqlWhere + " AND ");
		}

		// geoquery
		if (qp.getGeorel() != null) {
			GeoqueryRel gqr = qp.getGeorel();
			logger.debug("Georel value " + gqr.getGeorelOp());
			sqlWhere = translateNgsildGeoqueryToPostgisQuery(gqr, qp.getGeometry(), qp.getCoordinates(),
					qp.getGeoproperty());
			if (!sqlWhere.isEmpty()) {
				String sqlWhereTemporal = translateNgsildTimequeryToSql(qp.getTimerel(), qp.getTimeAt(),
						qp.getTimeproperty(), qp.getEndTimeAt(), "");
				sqlWhereGeoquery = "where exists (" + "  select 1 " + "  from temporalentityattrinstance "
						+ "  where temporalentity_id = r.id and " + "        attributeid = '" + qp.getGeoproperty()
						+ "' and " + "        attributetype = '" + NGSIConstants.NGSI_LD_GEOPROPERTY + "' and "
						+ sqlWhereTemporal + " and " + sqlWhere + ") ";
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
		sqlQuery += "  group by te.id, te.type, te.createdat, te.modifiedat, teai.attributeid "
				+ "  order by te.id, teai.attributeid " + ") "
				+ "select tedata || case when attrdata <> '{\"\": [null]}'::jsonb then attrdata else tedata end as data from ( "
				+ "  select id, ('{\"" + NGSIConstants.JSON_LD_ID + "\":\"' || id || '\"}')::jsonb || "
				+ "          ('{\"" + NGSIConstants.JSON_LD_TYPE + "\":[\"' || type || '\"]}')::jsonb ";
		if (qp.getIncludeSysAttrs()) {
			sqlQuery += "         || ('{\"" + NGSIConstants.NGSI_LD_CREATED_AT + "\":";
			// if (!qp.getTemporalValues()) {
			sqlQuery += "[ { \"" + NGSIConstants.JSON_LD_TYPE + "\": \"" + NGSIConstants.NGSI_LD_DATE_TIME + "\", \""
					+ NGSIConstants.JSON_LD_VALUE + "\": \"' ";
			// } else {
			// sqlQuery += "\"'";
			// }
			sqlQuery += "|| to_char(createdat, 'YYYY-MM-DD\"T\"HH24:MI:SS.ssssss\"Z\"') || '\"";
			// if (!qp.getTemporalValues()) {
			sqlQuery += "}]";
			// }
			sqlQuery += "}')::jsonb || ";
			sqlQuery += " ('{\"" + NGSIConstants.NGSI_LD_MODIFIED_AT + "\":";
			// if (!qp.getTemporalValues()) {
			sqlQuery += "[ { \"" + NGSIConstants.JSON_LD_TYPE + "\": \"" + NGSIConstants.NGSI_LD_DATE_TIME + "\", \""
					+ NGSIConstants.JSON_LD_VALUE + "\": \"' ";
			// } else {
			// sqlQuery += "\"'";
			// }
			sqlQuery += "|| to_char(modifiedat, 'YYYY-MM-DD\"T\"HH24:MI:SS.ssssss\"Z\"') || '\"";
			// if (!qp.getTemporalValues()) {
			sqlQuery += "}]";
			// }
			sqlQuery += "}')::jsonb";
		}
		sqlQuery += "  as tedata, " + "jsonb_object_agg(attributeid,";
		if (qp.getTemporalValues()) {
			String timeProperty = qp.getTimeproperty();
			if (timeProperty == null || timeProperty.equalsIgnoreCase(NGSIConstants.NGSI_LD_OBSERVED_AT)) {
				sqlQuery +=  "                    jsonb_build_object(\r\n'"
						+                         NGSIConstants.JSON_LD_TYPE+"',\r\n"
						+ "                        (\r\n"
						+ "                            select\r\n"
						+ "                                json_agg(jsonb(t ->'"+NGSIConstants.JSON_LD_TYPE+"'))\r\n"
						+ "                            from\r\n"
						+ "                                jsonb_array_elements(attributedata) as x (t)\r\n"
						+ "                        ) ->0\r\n"
						+ "                    ) || jsonb_build_object(\r\n"
						+ "                        'values',\r\n"
						+ "                        (\r\n"
						+ "                            select\r\n"
						+ "                                json_agg(\r\n"
						+"									json_build_array("
						+ "                                    json_build_array(\r\n"
						+ "                                        t ->'"+ NGSIConstants.NGSI_LD_HAS_VALUE + "' -> 0 ->'" + NGSIConstants.JSON_LD_VALUE + "',\r\n"
						+ "                                        t ->'" +NGSIConstants.NGSI_LD_OBSERVED_AT + "' -> 0 ->'" + NGSIConstants.JSON_LD_VALUE + "'\r\n"
						+ "                                    )\r\n"
						+ "                                   )\r\n"
						+ "                                )\r\n"
						+ "                            from\r\n"
						+ "                                jsonb_array_elements(attributedata) as x (t)\r\n))";
			} else {
				if (timeProperty.equalsIgnoreCase(NGSIConstants.NGSI_LD_MODIFIED_AT)) {
					sqlQuery +=  "                    jsonb_build_object(\r\n'"
							+                         NGSIConstants.JSON_LD_TYPE+"',\r\n"
							+ "                        (\r\n"
							+ "                            select\r\n"
							+ "                                json_agg(jsonb(t ->'"+NGSIConstants.JSON_LD_TYPE+"'))\r\n"
							+ "                            from\r\n"
							+ "                                jsonb_array_elements(attributedata) as x (t)\r\n"
							+ "                        ) ->0\r\n"
							+ "                    ) || jsonb_build_object(\r\n"
							+ "                        'values',\r\n"
							+ "                        (\r\n"
							+ "                            select\r\n"
							+ "                                json_agg(\r\n"
							+"									json_build_array("
							+ "                                    json_build_array(\r\n"
							+ "                                        t ->'"+ NGSIConstants.NGSI_LD_HAS_VALUE + "' -> 0 ->'" + NGSIConstants.JSON_LD_VALUE + "',\r\n"
							+ "                                        t ->'" +NGSIConstants.NGSI_LD_MODIFIED_AT + "' -> 0 ->'" + NGSIConstants.JSON_LD_VALUE + "'\r\n"
							+ "                                    )\r\n"
							+ "                                  )\r\n"
							+ "                                )\r\n"
							+ "                            from\r\n"
							+ "                                jsonb_array_elements(attributedata) as x (t)\r\n))";
				} else if (timeProperty.equalsIgnoreCase(NGSIConstants.NGSI_LD_CREATED_AT)) {
					sqlQuery +=  "                    jsonb_build_object(\r\n'"
							+                         NGSIConstants.JSON_LD_TYPE+"',\r\n"
							+ "                        (\r\n"
							+ "                            select\r\n"
							+ "                                json_agg(jsonb(t ->'"+NGSIConstants.JSON_LD_TYPE+"'))\r\n"
							+ "                            from\r\n"
							+ "                                jsonb_array_elements(attributedata) as x (t)\r\n"
							+ "                        ) ->0\r\n"
							+ "                    ) || jsonb_build_object(\r\n"
							+ "                        'values',\r\n"
							+ "                        (\r\n"
							+ "                            select\r\n"
							+ "                                json_agg(\r\n"
							+"									json_build_array("
							+ "                                    json_build_array(\r\n"
							+ "                                        t ->'"+ NGSIConstants.NGSI_LD_HAS_VALUE + "' -> 0 ->'" + NGSIConstants.JSON_LD_VALUE + "',\r\n"
							+ "                                        t ->'" +NGSIConstants.NGSI_LD_CREATED_AT + "' -> 0 ->'" + NGSIConstants.JSON_LD_VALUE + "'\r\n"
							+ "                                    )\r\n"
							+ "                                    )\r\n"
							+ "                                )\r\n"
							+ "                            from\r\n"
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
		return sqlQuery;

	}

	@Override
	public String translateNgsildGeoqueryToPostgisQuery(GeoqueryRel georel, String geometry, String coordinates,
			String geoproperty) throws ResponseException {
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
		return sqlWhere.toString();
	}

	@Override
	public String typesAndAttributeQuery(QueryParams qp) {
		// TODO Auto-generated method stub
		return "";
	}
}
