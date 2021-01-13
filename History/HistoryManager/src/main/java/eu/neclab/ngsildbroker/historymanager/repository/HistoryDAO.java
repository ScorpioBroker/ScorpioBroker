package eu.neclab.ngsildbroker.historymanager.repository;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import eu.neclab.ngsildbroker.commons.constants.DBConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.GeoqueryRel;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.storage.StorageReaderDAO;

@Repository
public class HistoryDAO extends StorageReaderDAO {

	protected final static Logger logger = LoggerFactory.getLogger(HistoryDAO.class);

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
	protected String translateNgsildQueryToSql(QueryParams qp) throws ResponseException {
		StringBuilder fullSqlWhere = new StringBuilder(70);
		String sqlWhereGeoquery = "";
		String sqlWhere = "";

		if (qp.getType() != null) {
			sqlWhere = getSqlWhereForField("te." + DBCOLUMN_HISTORY_ENTITY_TYPE, qp.getType());
			fullSqlWhere.append(sqlWhere + " AND ");
		}
		if (qp.getAttrs() != null) {
			sqlWhere = getSqlWhereForField("teai." + DBCOLUMN_HISTORY_ATTRIBUTE_ID, qp.getAttrs());
			fullSqlWhere.append(sqlWhere + " AND ");
		}
		if (qp.getInstanceId() != null) {
			sqlWhere = getSqlWhereForField("teai." + DBCOLUMN_HISTORY_INSTANCE_ID, qp.getInstanceId());
			fullSqlWhere.append(sqlWhere + " AND ");
		}
		if (qp.getId() != null) {
			sqlWhere = getSqlWhereForField("te." + DBCOLUMN_HISTORY_ENTITY_ID, qp.getId());
			fullSqlWhere.append(sqlWhere + " AND ");
		}
		if (qp.getIdPattern() != null) {
			sqlWhere = "te." + DBCOLUMN_HISTORY_ENTITY_ID + " ~ '" + qp.getIdPattern() + "'";
			fullSqlWhere.append(sqlWhere + " AND ");
		}

		// temporal query
		if (qp.getTimerel() != null) {
			sqlWhere = translateNgsildTimequeryToSql(qp.getTimerel(), qp.getTime(), qp.getTimeproperty(),
					qp.getEndTime(), "teai.");
			fullSqlWhere.append(sqlWhere + " AND ");
		}

		// geoquery
		if (qp.getGeorel() != null) {
			GeoqueryRel gqr = qp.getGeorel();
			logger.debug("Georel value " + gqr.getGeorelOp());
			sqlWhere = translateNgsildGeoqueryToPostgisQuery(gqr, qp.getGeometry(), qp.getCoordinates(),
					qp.getGeoproperty(), "geovalue");
			if (!sqlWhere.isEmpty()) {
				String sqlWhereTemporal = translateNgsildTimequeryToSql(qp.getTimerel(), qp.getTime(),
						qp.getTimeproperty(), qp.getEndTime(), "");
				sqlWhereGeoquery = "where exists (" + "  select 1 " + "  from temporalentityattrinstance "
						+ "  where temporalentity_id = r.id and " + "        attributeid = '" + qp.getGeoproperty()
						+ "' and " + "        attributetype = '" + NGSIConstants.NGSI_LD_GEOPROPERTY + "' and "
						+ sqlWhereTemporal + " and " + sqlWhere + ") ";
			}
		}


		String sqlQuery = "with r as ("
				+ "  select te.id, te.type, te.createdat, te.modifiedat, coalesce(teai.attributeid, '') as attributeid, jsonb_agg(teai.data";

		if (!qp.getIncludeSysAttrs()) {
			sqlQuery += "  - '" + NGSIConstants.NGSI_LD_CREATED_AT + "' - '" + NGSIConstants.NGSI_LD_MODIFIED_AT + "'";
		}
		sqlQuery += " order by teai.modifiedat desc) as attributedata" + "  from " + DBConstants.DBTABLE_TEMPORALENTITY
				+ " te" + "  left join " + DBConstants.DBTABLE_TEMPORALENTITY_ATTRIBUTEINSTANCE
				+ " teai on (teai.temporalentity_id = te.id)" + "  where ";
		sqlQuery += fullSqlWhere.toString() + " 1=1 ";
		sqlQuery += "  group by te.id, te.type, te.createdat, te.modifiedat, teai.attributeid "
				+ "  order by te.id, teai.attributeid " + ") "
				+ "select tedata || case when attrdata <> '{\"\": [null]}'::jsonb then attrdata else tedata end as data from ( "
				+ "  select id, ('{\"" + NGSIConstants.JSON_LD_ID + "\":\"' || id || '\"}')::jsonb || "
				+ "          ('{\"" + NGSIConstants.JSON_LD_TYPE + "\":[\"' || type || '\"]}')::jsonb ";
		if (qp.getIncludeSysAttrs()) {
			sqlQuery += "         || ('{\"" + NGSIConstants.NGSI_LD_CREATED_AT + "\":";
			//if (!qp.getTemporalValues()) {
				sqlQuery += "[ { \"" + NGSIConstants.JSON_LD_TYPE + "\": \"" + NGSIConstants.NGSI_LD_DATE_TIME
						+ "\", \"" + NGSIConstants.JSON_LD_VALUE + "\": \"' ";
			//} else {
			//	sqlQuery += "\"'";
			//}
			sqlQuery += "|| to_char(createdat, 'YYYY-MM-DD\"T\"HH24:MI:SS.ssssss\"Z\"') || '\"";
			//if (!qp.getTemporalValues()) {
				sqlQuery += "}]";
			//}
			sqlQuery += "}')::jsonb || ";
			sqlQuery += " ('{\"" + NGSIConstants.NGSI_LD_MODIFIED_AT + "\":";
			//if (!qp.getTemporalValues()) {
				sqlQuery += "[ { \"" + NGSIConstants.JSON_LD_TYPE + "\": \"" + NGSIConstants.NGSI_LD_DATE_TIME
						+ "\", \"" + NGSIConstants.JSON_LD_VALUE + "\": \"' ";
			//} else {
			//	sqlQuery += "\"'";
			//}
			sqlQuery += "|| to_char(modifiedat, 'YYYY-MM-DD\"T\"HH24:MI:SS.ssssss\"Z\"') || '\"";
			//if (!qp.getTemporalValues()) {
				sqlQuery += "}]";
			//}
			sqlQuery += "}')::jsonb";
		}
		sqlQuery += "  as tedata, " + "jsonb_object_agg(attributeid,";
		if (qp.getTemporalValues()) {
			if (qp.getTimeproperty().equalsIgnoreCase(NGSIConstants.NGSI_LD_MODIFIED_AT)) {
				sqlQuery += "(select json_agg(jsonb_build_array(t -> '" + NGSIConstants.NGSI_LD_HAS_VALUE + "'->0->'"
						+ NGSIConstants.JSON_LD_VALUE + "',t->'" + NGSIConstants.NGSI_LD_MODIFIED_AT + "'->0->'"
						+ NGSIConstants.JSON_LD_VALUE + "')) from jsonb_array_elements(attributedata) as x(t))";
			} else if (qp.getTimeproperty().equalsIgnoreCase(NGSIConstants.NGSI_LD_CREATED_AT)) {
				sqlQuery += "(select json_agg(jsonb_build_array(t -> '" + NGSIConstants.NGSI_LD_HAS_VALUE + "'->0->'"
						+ NGSIConstants.JSON_LD_VALUE + "',t->'" + NGSIConstants.NGSI_LD_CREATED_AT + "'->0->'"
						+ NGSIConstants.JSON_LD_VALUE + "')) from jsonb_array_elements(attributedata) as x(t))";
			} else if (qp.getTimeproperty().equalsIgnoreCase(NGSIConstants.NGSI_LD_OBSERVED_AT)) {
				sqlQuery += "(select json_agg(jsonb_build_array(t -> '" + NGSIConstants.NGSI_LD_HAS_VALUE + "'->0->'"
						+ NGSIConstants.JSON_LD_VALUE + "',t->'" + NGSIConstants.NGSI_LD_OBSERVED_AT + "'->0->'"
						+ NGSIConstants.JSON_LD_VALUE + "')) from jsonb_array_elements(attributedata) as x(t))";
			}
		} else {
			sqlQuery += "attributedata";
		}

		sqlQuery += ") as attrdata " + "  from r ";
		sqlQuery += sqlWhereGeoquery;
		sqlQuery += "  group by id, type, createdat, modifiedat ";
		sqlQuery += "  order by modifiedat desc ";
		sqlQuery += ") as m";
		
		// advanced query "q"
		//THIS DOESN'T WORK 
		if (qp.getQ() != null) {
			sqlQuery += " where " + qp.getQ(); 
		}

		return sqlQuery;
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

	public boolean entityExists(String entityId) {
		List list = readerJdbcTemplate.queryForList("Select id from temporalentity where id='" + entityId + "';");
		if (list == null || list.isEmpty()) {
			return false;
		}
		return true;
	}

}
