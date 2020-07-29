package eu.neclab.ngsildbroker.commons.storage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.util.ReflectionUtils;

import eu.neclab.ngsildbroker.commons.constants.DBConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.GeoqueryRel;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

abstract public class StorageReaderDAO {

	private final static Logger logger = LogManager.getLogger(StorageReaderDAO.class);

	@Autowired
	protected JdbcTemplate readerJdbcTemplate;

	@PostConstruct
	public void init() {
		readerJdbcTemplate.execute("SELECT 1"); // create connection pool and connect to database
	}

	
	public List<String> query(QueryParams qp) {
		
		try {
			String sqlQuery = translateNgsildQueryToSql(qp);
			logger.info("NGSI-LD to SQL: " + sqlQuery);
			//SqlRowSet result = readerJdbcTemplate.queryForRowSet(sqlQuery);
			
			return readerJdbcTemplate.queryForList(sqlQuery,String.class);
			

		} catch(DataIntegrityViolationException e) {
			//Empty result don't worry
			logger.warn("SQL Result Exception::", e);
			return new ArrayList<String>();
		} catch (Exception e) {
			logger.error("Exception ::", e);
		}
		return new ArrayList<String>();

	}

	public String getListAsJsonArray(List<String> s) {
		return "[" + String.join(",", s) + "]";
	}

	public List<String> getLocalTypes() {
		ArrayList<String> result = new ArrayList<String>();
		List<Map<String, Object>> list = readerJdbcTemplate.queryForList(
				"SELECT distinct type as type FROM entity WHERE type IS NOT NULL;");
		if(list == null ||list.isEmpty()) {
			return null;
		}
		for (Map<String, Object> row : list) {
			result.add(row.get("type").toString());
		}
		return result;
	}
	
	public List<String> getAllTypes() {
		ArrayList<String> result = new ArrayList<String>();
		List<Map<String, Object>> list = readerJdbcTemplate.queryForList(
				"SELECT distinct type as type FROM entity WHERE type IS NOT NULL UNION SELECT distinct entity_type as type FROM csourceinformation WHERE entity_type IS NOT NULL;");
		if(list == null ||list.isEmpty()) {
			return null;
		}
		for (Map<String, Object> row : list) {
			result.add(row.get("type").toString());
		}
		return result;
	}

	/*
	 * TODO: optimize sql queries by using prepared statements (if possible)
	 */
	protected String translateNgsildQueryToSql(QueryParams qp) throws ResponseException {
		StringBuilder fullSqlWhereProperty = new StringBuilder(70);

		// https://stackoverflow.com/questions/3333974/how-to-loop-over-a-class-attributes-in-java
		ReflectionUtils.doWithFields(qp.getClass(), field -> {
			String dbColumn, sqlOperator;
			String sqlWhereProperty = "";

			field.setAccessible(true);
			String queryParameter = field.getName();
			Object fieldValue = field.get(qp);
			if (fieldValue != null) {

				logger.trace("Query parameter:" + queryParameter);

				String queryValue = "";
				if (fieldValue instanceof String) {
					queryValue = fieldValue.toString();
					logger.trace("Query value: " + queryValue);
				}

				switch (queryParameter) {
				case NGSIConstants.QUERY_PARAMETER_IDPATTERN:
					dbColumn = DBConstants.DBCOLUMN_ID;
					sqlOperator = "~";
					sqlWhereProperty = dbColumn + " " + sqlOperator + " '" + queryValue + "'";
					break;
				case NGSIConstants.QUERY_PARAMETER_TYPE:
				case NGSIConstants.QUERY_PARAMETER_ID:
					dbColumn = queryParameter;
					if (queryValue.indexOf(",") == -1) {
						sqlOperator = "=";
						sqlWhereProperty = dbColumn + " " + sqlOperator + " '" + queryValue + "'";
					} else {
						sqlOperator = "IN";
						sqlWhereProperty = dbColumn + " " + sqlOperator + " ('" + queryValue.replace(",", "','") + "')";
					}
					break;
				case NGSIConstants.QUERY_PARAMETER_ATTRS:
					dbColumn = "data";
					sqlOperator = "?";
					if (queryValue.indexOf(",") == -1) {
						sqlWhereProperty = dbColumn + " " + sqlOperator + "'" + queryValue + "'";
					} else {
						sqlWhereProperty = "("+dbColumn + " " + sqlOperator + " '"
								+ queryValue.replace(",", "' OR " + dbColumn + " " + sqlOperator + "'") + "')";
					}
					break;
				case NGSIConstants.QUERY_PARAMETER_GEOREL:
					if (fieldValue instanceof GeoqueryRel) {
						GeoqueryRel gqr = (GeoqueryRel) fieldValue;
						logger.trace("Georel value " + gqr.getGeorelOp());
						try {
							sqlWhereProperty = translateNgsildGeoqueryToPostgisQuery(gqr, qp.getGeometry(),
									qp.getCoordinates(), qp.getGeoproperty());
						} catch (ResponseException e) {
							e.printStackTrace();
						}
					}
					break;
				case NGSIConstants.QUERY_PARAMETER_QUERY:
					sqlWhereProperty = queryValue;
					break;
				}
				fullSqlWhereProperty.append(sqlWhereProperty);
				if (!sqlWhereProperty.isEmpty())
					fullSqlWhereProperty.append(" AND ");
			}
		});

		String tableDataColumn;
		if (qp.getKeyValues()) {
			if (qp.getIncludeSysAttrs()) {
				tableDataColumn = DBConstants.DBCOLUMN_KVDATA;
			} else { // without sysattrs at root level (entity createdat/modifiedat)
				tableDataColumn = DBConstants.DBCOLUMN_KVDATA + " - '" + NGSIConstants.NGSI_LD_CREATED_AT + "' - '"
						+ NGSIConstants.NGSI_LD_MODIFIED_AT + "'";
			}
		} else {
			if (qp.getIncludeSysAttrs()) {
				tableDataColumn = DBConstants.DBCOLUMN_DATA;
			} else {
				tableDataColumn = DBConstants.DBCOLUMN_DATA_WITHOUT_SYSATTRS; // default request
			}
		}

		String dataColumn = tableDataColumn;
		if (qp.getAttrs() != null) {
			String expandedAttributeList = "'" + NGSIConstants.JSON_LD_ID + "','" + NGSIConstants.JSON_LD_TYPE + "','"
					+ qp.getAttrs().replace(",", "','") + "'";
			if (qp.getIncludeSysAttrs()) {
				expandedAttributeList += "," + NGSIConstants.NGSI_LD_CREATED_AT + ","
						+ NGSIConstants.NGSI_LD_MODIFIED_AT;
			}
			dataColumn = "(SELECT jsonb_object_agg(key, value) FROM jsonb_each(" + tableDataColumn + ") WHERE key IN ( "
					+ expandedAttributeList + "))";
		}
		String sqlQuery = "SELECT " + dataColumn + " as data FROM " + DBConstants.DBTABLE_ENTITY + " ";
		if (fullSqlWhereProperty.length() > 0) {
			sqlQuery += "WHERE " + fullSqlWhereProperty.toString() + " 1=1 ";
		}
		int limit = qp.getLimit();
		int offSet = qp.getOffSet();
				
		if(limit != -1) {
			sqlQuery += "LIMIT " + limit + " "; 
		}
		if(offSet != -1) {
			sqlQuery += "OFFSET " + offSet + " "; 
		}
		// order by ?

		return sqlQuery;
	}

	// TODO: SQL input sanitization
	// TODO: property of property
	// [SPEC] spec is not clear on how to define a "property of property" in
	// the geoproperty field. (probably using dots)
	protected String translateNgsildGeoqueryToPostgisQuery(GeoqueryRel georel, String geometry, String coordinates,
			String geoproperty, String dbColumn) throws ResponseException {
		StringBuilder sqlWhere = new StringBuilder(50);

		String georelOp = georel.getGeorelOp();
		logger.trace("  Geoquery term georelOp: " + georelOp);

		if (dbColumn == null) {
			dbColumn = DBConstants.NGSILD_TO_SQL_RESERVED_PROPERTIES_MAPPING_GEO.get(geoproperty);
			if (dbColumn == null) {
				sqlWhere.append("data @> '{\"" + geoproperty + "\": [{\"" + NGSIConstants.JSON_LD_TYPE + "\":[\""
						+ NGSIConstants.NGSI_LD_GEOPROPERTY + "\"]}]}' AND ");
				dbColumn = "ST_SetSRID(ST_GeomFromGeoJSON( " + "data#>>'{" + geoproperty + ",0,"
						+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE + "}'), 4326)";
			}
		}

		String referenceValue = "ST_SetSRID(ST_GeomFromGeoJSON('{\"type\": \"" + geometry + "\", \"coordinates\": "
				+ coordinates + " }'), 4326)";
		String sqlPostgisFunction = DBConstants.NGSILD_TO_POSTGIS_GEO_OPERATORS_MAPPING.get(georelOp);

		switch (georelOp) {
		case NGSIConstants.GEO_REL_NEAR:
			if (georel.getDistanceType() != null && georel.getDistanceValue() != null) {
				if (georel.getDistanceType().equals(NGSIConstants.GEO_REL_MIN_DISTANCE))
					sqlWhere.append("NOT ");
				sqlWhere.append(sqlPostgisFunction + "( " + dbColumn + "::geography, " + referenceValue
						+ "::geography, " + georel.getDistanceValue() + ") ");
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

	protected String translateNgsildGeoqueryToPostgisQuery(GeoqueryRel georel, String geometry, String coordinates,
			String geoproperty) throws ResponseException {
		return this.translateNgsildGeoqueryToPostgisQuery(georel, geometry, coordinates, geoproperty, null);
	}

}
