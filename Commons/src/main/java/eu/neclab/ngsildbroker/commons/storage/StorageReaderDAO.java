package eu.neclab.ngsildbroker.commons.storage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.DBConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.GeoqueryRel;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.datatypes.QueryResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tenant.DBUtil;

abstract public class StorageReaderDAO {

	private final static Logger logger = LogManager.getLogger(StorageReaderDAO.class);
	protected Map<Object, DataSource> resolvedDataSources = new HashMap<>();

	private HashMap<String, JdbcTemplate> tenant2Template = new HashMap<String, JdbcTemplate>();

	@Autowired
	private JdbcTemplate readerJdbcTemplate;
	@Autowired
	private DataSource masterDataSource;

	@Autowired
	private HikariConfig hikariConfig;

	public Random random = new Random();

	@PostConstruct
	public void init() {
		readerJdbcTemplate.execute("SELECT 1"); // create connection pool and connect to database
	}

	public String findDataBaseNameByTenantId(String tenantidvalue) {
		if (tenantidvalue == null)
			return null;
		try {

			String databasename = "ngb" + tenantidvalue;
			List<String> data;
			data = readerJdbcTemplate.queryForList("SELECT datname FROM pg_database", String.class);
			if (data.contains(databasename)) {
				return databasename;
			} else {
				return null;
			}
		} catch (EmptyResultDataAccessException e) {
			return null;
		}
	}

	public DataSource determineTargetDataSource(String tenantidvalue) throws ResponseException {
		// String tenantidvalue = (String) determineCurrentLookupKey();
		if (tenantidvalue == null) {
			return masterDataSource;
		}
		DataSource tenantDataSource = resolvedDataSources.get(tenantidvalue);
		if (tenantDataSource == null) {

			tenantDataSource = createDataSourceForTenantId(tenantidvalue);

			resolvedDataSources.put(tenantidvalue, tenantDataSource);
		}

		return tenantDataSource;
	}

	private DataSource createDataSourceForTenantId(String tenantidvalue) throws ResponseException {
		String tenantDatabaseName = findDataBaseNameByTenantId(tenantidvalue);
		if (tenantDatabaseName == null) {
			throw new ResponseException(ErrorType.TenantNotFound);
		}
		HikariConfig tenantHikariConfig = new HikariConfig();
		hikariConfig.copyStateTo(tenantHikariConfig);
		String tenantJdbcURL = DBUtil.databaseURLFromPostgresJdbcUrl(hikariConfig.getJdbcUrl(), tenantDatabaseName);
		tenantHikariConfig.setJdbcUrl(tenantJdbcURL);
		tenantHikariConfig.setPoolName(tenantDatabaseName + "-db-pool");
		return new HikariDataSource(tenantHikariConfig);
	}

	public QueryResult query(QueryParams qp) throws ResponseException {
		JdbcTemplate template;
		QueryResult queryResult = new QueryResult(null, null, ErrorType.None, -1, true);
		try {
			
			String tenantId = qp.getTenant();
			template = getJDBCTemplate(tenantId);
		} catch (Exception e) {
			throw new ResponseException(ErrorType.TenantNotFound);
		}
		try {
			if (qp.getCheck() != null) {
				String sqlQuery = typesAndAttributeQuery(qp);
				List<String> list =  template.queryForList(sqlQuery, String.class);
				queryResult.setActualDataString(list);
				return queryResult;
			}
			if(qp.getCountResult() != null) {
			if (qp.getLimit() == 0 && qp.getCountResult() == true) {
				String sqlQueryCount = translateNgsildQueryToCountResult(qp);
				Integer count = template.queryForObject(sqlQueryCount, Integer.class);
				queryResult.setCount(count);
				return queryResult;
			}
				String sqlQuery = translateNgsildQueryToSql(qp);
				List<String> list = template.queryForList(sqlQuery, String.class);
				queryResult.setActualDataString(list);
				String sqlQueryCount = translateNgsildQueryToCountResult(qp);
				Integer count = template.queryForObject(sqlQueryCount, Integer.class);
				queryResult.setCount(count);
				return queryResult;
			} else {
				String sqlQuery = translateNgsildQueryToSql(qp);
				List<String> list = template.queryForList(sqlQuery, String.class);
				queryResult.setActualDataString(list);
				return queryResult;
			}
		} catch (DataIntegrityViolationException e) {
			// Empty result don't worry
			logger.debug("SQL Result Exception::", e);
			return queryResult;
		} catch (Exception e) {
			logger.error("Exception ::", e);
		}
		return queryResult;

	}

	protected JdbcTemplate getJDBCTemplate(String tenantId) throws ResponseException {
		JdbcTemplate result;
		if (tenantId == null) {
			result = readerJdbcTemplate;
		} else {
			result = tenant2Template.get(tenantId);
			if (result == null) {
				DataSource finaldatasource = determineTargetDataSource(tenantId);
				if (finaldatasource == null) {
					throw new ResponseException(ErrorType.TenantNotFound);
				}
				result = new JdbcTemplate(finaldatasource);
				tenant2Template.put(tenantId, result);
			}
		}
		return result;
	}

	/*
	 * protected void setTenant(String tenantId) throws ResponseException { if
	 * (tenantId != null) { DataSource finaldatasource =
	 * determineTargetDataSource(tenantId); if (finaldatasource == null) { throw new
	 * ResponseException(ErrorType.TenantNotFound); } synchronized
	 * (readerJdbcTemplate) { readerJdbcTemplate = new
	 * JdbcTemplate(finaldatasource); } } else { synchronized (readerJdbcTemplate) {
	 * readerJdbcTemplate = new JdbcTemplate(masterDataSource); } } }
	 */
	public String getListAsJsonArray(List<String> s) {
		return "[" + String.join(",", s) + "]";
	}

	public List<String> getLocalTypes() {
		ArrayList<String> result = new ArrayList<String>();
		List<Map<String, Object>> list = readerJdbcTemplate
				.queryForList("SELECT distinct type as type FROM entity WHERE type IS NOT NULL;");
		if (list == null || list.isEmpty()) {
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
		if (list == null || list.isEmpty()) {
			return null;
		}
		for (Map<String, Object> row : list) {
			result.add(row.get("type").toString());
		}
		return result;
	}

	/*
	 * TODO: optimize sql queries for types and Attributes by using prepared
	 * statements (if possible)
	 */
	protected String typesAndAttributeQuery(QueryParams qp) throws ResponseException {
		String query = "";
		if (qp.getCheck() == "NonDeatilsType" && qp.getAttrs() == null) {
			int number = random.nextInt(999999);
			query = "select jsonb_build_object('" + NGSIConstants.JSON_LD_ID + "','urn:ngsi-ld:EntityTypeList:" + number
					+ "','" + NGSIConstants.JSON_LD_TYPE + "', jsonb_build_array('" + NGSIConstants.NGSI_LD_ENTITY_LIST
					+ "'), '" + NGSIConstants.NGSI_LD_TYPE_LIST + "',json_agg(distinct jsonb_build_object('"
					+ NGSIConstants.JSON_LD_ID + "', type)::jsonb)) from entity;";
			return query;
		} else if (qp.getCheck() == "deatilsType" && qp.getAttrs() == null) {
			query = "select distinct jsonb_build_object('" + NGSIConstants.JSON_LD_ID + "',type,'"
					+ NGSIConstants.JSON_LD_TYPE + "', jsonb_build_array('" + NGSIConstants.NGSI_LD_ENTITY_TYPE
					+ "'), '" + NGSIConstants.NGSI_LD_TYPE_NAME + "', jsonb_build_array(jsonb_build_object('"
					+ NGSIConstants.JSON_LD_ID + "', type)), '" + NGSIConstants.NGSI_LD_ATTRIBUTE_NAMES
					+ "', jsonb_agg(jsonb_build_object('" + NGSIConstants.JSON_LD_ID
					+ "', attribute.key))) from entity, jsonb_each(data_without_sysattrs - '" + NGSIConstants.JSON_LD_ID
					+ "' - '" + NGSIConstants.JSON_LD_TYPE + "') attribute group by id;";
			return query;
		} else if (qp.getCheck() == "type" && qp.getAttrs() != null) {
			String type = qp.getAttrs();
			query = "with r as (select distinct attribute.key as mykey, jsonb_agg(distinct jsonb_build_object('"
					+ NGSIConstants.JSON_LD_ID
					+ "', attribute.value#>>'{0,@type,0}')) as mytype from entity, jsonb_each(data_without_sysattrs - '"
					+ NGSIConstants.JSON_LD_ID + "' - '" + NGSIConstants.JSON_LD_TYPE + "') attribute where type='"
					+ type + "'" + " group by attribute.key)select jsonb_build_object('" + NGSIConstants.JSON_LD_ID
					+ "',type,'" + NGSIConstants.JSON_LD_TYPE + "', jsonb_build_array('"
					+ NGSIConstants.NGSI_LD_ENTITY_TYPE_INFO + "'), '" + NGSIConstants.NGSI_LD_TYPE_NAME
					+ "', jsonb_build_array(jsonb_build_object('" + NGSIConstants.JSON_LD_ID + "', type)),'"
					+ NGSIConstants.NGSI_LD_ENTITY_COUNT + "', jsonb_build_array(jsonb_build_object('"
					+ NGSIConstants.JSON_LD_VALUE + "', count(Distinct id))), '"
					+ NGSIConstants.NGSI_LD_ATTRIBUTE_DETAILS + "', jsonb_agg(distinct jsonb_build_object('"
					+ NGSIConstants.NGSI_LD_ATTRIBUTE_NAME + "',jsonb_build_array(jsonb_build_object('"
					+ NGSIConstants.JSON_LD_ID + "', mykey)), '" + NGSIConstants.NGSI_LD_ATTRIBUTE_TYPES
					+ "', mytype, '" + NGSIConstants.JSON_LD_ID + "', mykey, '" + NGSIConstants.JSON_LD_TYPE
					+ "',jsonb_build_array('" + NGSIConstants.NGSI_LD_ATTRIBUTE
					+ "')))) from entity, r attribute where type='" + type + "' group by type;";
			return query;
		} else if (qp.getCheck() == "NonDeatilsAttributes" && qp.getAttrs() == null) {
			int number = random.nextInt(999999);
			query = "select jsonb_build_object('" + NGSIConstants.JSON_LD_ID + "','urn:ngsi-ld:AttributeList:" + number
					+ "','" + NGSIConstants.JSON_LD_TYPE + "', jsonb_build_array('"
					+ NGSIConstants.NGSI_LD_ATTRIBUTE_LIST_1 + "'), '" + NGSIConstants.NGSI_LD_ATTRIBUTE_LIST_2
					+ "',json_agg(distinct jsonb_build_object('" + NGSIConstants.JSON_LD_ID
					+ "', attribute.key)::jsonb)) from entity,jsonb_each(data_without_sysattrs-'"
					+ NGSIConstants.JSON_LD_ID + "'-'" + NGSIConstants.JSON_LD_TYPE + "') attribute;";
			return query;

		} else if (qp.getCheck() == "deatilsAttributes" && qp.getAttrs() == null) {
			query = "select jsonb_build_object('" + NGSIConstants.JSON_LD_ID + "', attribute.key,'"
					+ NGSIConstants.JSON_LD_TYPE + "','" + NGSIConstants.NGSI_LD_ATTRIBUTE + "','"
					+ NGSIConstants.NGSI_LD_ATTRIBUTE_NAME + "',jsonb_build_object('" + NGSIConstants.JSON_LD_ID
					+ "', attribute.key),'" + NGSIConstants.NGSI_LD_TYPE_NAMES
					+ "',jsonb_agg(distinct jsonb_build_object('" + NGSIConstants.JSON_LD_ID
					+ "', type))) from entity,jsonb_each(data_without_sysattrs-'" + NGSIConstants.JSON_LD_ID + "'-'"
					+ NGSIConstants.JSON_LD_TYPE + "') attribute group by attribute.key;";
			return query;

		} else if (qp.getCheck() == "Attribute" && qp.getAttrs() != null) {
			String type = qp.getAttrs();
			query = "with r as(select count(data_without_sysattrs->'" + type
					+ "') as mycount  from entity), y as(select  jsonb_agg(distinct jsonb_build_object('"
					+ NGSIConstants.JSON_LD_ID + "',type)) as mytype,jsonb_build_array(jsonb_build_object('"
					+ NGSIConstants.JSON_LD_VALUE + "',mycount)) as finalcount, jsonb_agg(distinct jsonb_build_object('"
					+ NGSIConstants.JSON_LD_ID
					+ "',attribute.value#>>'{0,@type,0}')) as mydata from r,entity,jsonb_each(data_without_sysattrs-'"
					+ NGSIConstants.JSON_LD_ID + "'-'" + NGSIConstants.JSON_LD_TYPE
					+ "') attribute where attribute.key='" + type + "' group by mycount) select jsonb_build_object('"
					+ NGSIConstants.JSON_LD_ID + "','" + type + "','" + NGSIConstants.JSON_LD_TYPE + "','"
					+ NGSIConstants.NGSI_LD_ATTRIBUTE + "','" + NGSIConstants.NGSI_LD_ATTRIBUTE_NAME
					+ "',jsonb_build_object('" + NGSIConstants.JSON_LD_ID + "','" + type + "'),'"
					+ NGSIConstants.NGSI_LD_TYPE_NAMES + "',mytype,'" + NGSIConstants.NGSI_LD_ATTRIBUTE_COUNT
					+ "',finalcount,'" + NGSIConstants.NGSI_LD_ATTRIBUTE_TYPES + "',mydata) from y,r;";
			return query;

		}
		return null;
	}

	/*
	 * TODO: optimize sql queries by using prepared statements (if possible)
	 */
	protected String translateNgsildQueryToSql(QueryParams qp) throws ResponseException {
		StringBuilder fullSqlWhereProperty = new StringBuilder(70);
		String dbColumn, sqlOperator;
		String sqlWhereProperty = null;
		List<Map<String, String>> entities = qp.getEntities();
		fullSqlWhereProperty.append("(");
		for (Map<String, String> entityInfo : entities) {
			fullSqlWhereProperty.append("(");
			for (Entry<String, String> entry : entityInfo.entrySet()) {
				switch (entry.getKey()) {
				case NGSIConstants.JSON_LD_ID:
					dbColumn = NGSIConstants.QUERY_PARAMETER_ID;
					if (entry.getValue().indexOf(",") == -1) {
						sqlOperator = "=";
						sqlWhereProperty = dbColumn + " " + sqlOperator + " '" + entry.getValue() + "'";
					} else {
						sqlOperator = "IN";
						sqlWhereProperty = dbColumn + " " + sqlOperator + " ('" + entry.getValue().replace(",", "','") + "')";
					}			
					break;
				case NGSIConstants.JSON_LD_TYPE:
					dbColumn = NGSIConstants.QUERY_PARAMETER_TYPE;
					if (entry.getValue().indexOf(",") == -1) {
						sqlOperator = "=";
						sqlWhereProperty = dbColumn + " " + sqlOperator + " '" + entry.getValue() + "'";
					} else {
						sqlOperator = "IN";
						sqlWhereProperty = dbColumn + " " + sqlOperator + " ('" + entry.getValue().replace(",", "','") + "')";
					}			

					break;
				case NGSIConstants.NGSI_LD_ID_PATTERN:
					dbColumn = DBConstants.DBCOLUMN_ID;
					sqlOperator = "~";
					sqlWhereProperty = dbColumn + " " + sqlOperator + " '" + entry.getValue() + "'";
					break;

				default:
					break;
				}
				fullSqlWhereProperty.append(sqlWhereProperty);
				fullSqlWhereProperty.append(" AND ");
			}
			fullSqlWhereProperty.delete(fullSqlWhereProperty.length() - 5, fullSqlWhereProperty.length());
			fullSqlWhereProperty.append(") OR ");
		}
		fullSqlWhereProperty.delete(fullSqlWhereProperty.length() - 4, fullSqlWhereProperty.length());
		fullSqlWhereProperty.append(")");
		if (qp.getAttrs() != null) {
			String queryValue;
			queryValue = qp.getAttrs();
			dbColumn = "data";
			sqlOperator = "?";
			if (queryValue.indexOf(",") == -1) {
				sqlWhereProperty = dbColumn + " " + sqlOperator + "'" + queryValue + "'";
			} else {
				sqlWhereProperty = "(" + dbColumn + " " + sqlOperator + " '"
						+ queryValue.replace(",", "' OR " + dbColumn + " " + sqlOperator + "'") + "')";
			}
			fullSqlWhereProperty.append(" AND ");
			fullSqlWhereProperty.append(sqlWhereProperty);

		}
		if (qp.getGeorel() != null) {
			GeoqueryRel gqr = qp.getGeorel();
			logger.trace("Georel value " + gqr.getGeorelOp());
			try {
				sqlWhereProperty = translateNgsildGeoqueryToPostgisQuery(gqr, qp.getGeometry(), qp.getCoordinates(),
						qp.getGeoproperty());
			} catch (ResponseException e) {
				e.printStackTrace();
			}
			fullSqlWhereProperty.append(" AND ");
			fullSqlWhereProperty.append(sqlWhereProperty);

		}
		if (qp.getQ() != null) {
			sqlWhereProperty = qp.getQ();
			fullSqlWhereProperty.append(" AND ");
			fullSqlWhereProperty.append(sqlWhereProperty);
		}

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
		String sqlQuery = "SELECT DISTINCT " + dataColumn + " as data FROM " + DBConstants.DBTABLE_ENTITY + " ";
		if (fullSqlWhereProperty.length() > 0) {
			sqlQuery += "WHERE " + fullSqlWhereProperty.toString() + " ";
		}
		int limit = qp.getLimit();
		int offSet = qp.getOffSet();

		if (limit > 0) {
			sqlQuery += "LIMIT " + limit + " ";
		}
		if (offSet > 0) {
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

	protected List<String> getTenants() throws ResponseException {
		ArrayList<String> result = new ArrayList<String>();
		try {
			List<Map<String, Object>> temp = getJDBCTemplate(null).queryForList("SELECT tenant_id FROM tenant");
			for (Map<String, Object> entry : temp) {
				result.add(entry.get("tenant_id").toString());
			}
		} catch (Exception e) {
			System.out.println("tenant table not found");
		}
		return result;
	}

	protected String getTenant(String tenantId) {
		if (tenantId == null) {
			return null;
		}
		if (AppConstants.INTERNAL_NULL_KEY.equals(tenantId)) {
			return null;
		}
		return tenantId;
	}
	
	/*
	 * TODO: query for count the no of result
	 */
	protected String translateNgsildQueryToCountResult(QueryParams qp) throws ResponseException {
		StringBuilder fullSqlWhereProperty = new StringBuilder(70);
		String dbColumn, sqlOperator;
		String sqlWhereProperty = null;
		List<Map<String, String>> entities = qp.getEntities();
		fullSqlWhereProperty.append("(");
		for (Map<String, String> entityInfo : entities) {
			fullSqlWhereProperty.append("(");
			for (Entry<String, String> entry : entityInfo.entrySet()) {
				switch (entry.getKey()) {
				case NGSIConstants.JSON_LD_ID:
					dbColumn = NGSIConstants.QUERY_PARAMETER_ID;
					if (entry.getValue().indexOf(",") == -1) {
						sqlOperator = "=";
						sqlWhereProperty = dbColumn + " " + sqlOperator + " '" + entry.getValue() + "'";
					} else {
						sqlOperator = "IN";
						sqlWhereProperty = dbColumn + " " + sqlOperator + " ('" + entry.getValue().replace(",", "','") + "')";
					}			
					break;
				case NGSIConstants.JSON_LD_TYPE:
					dbColumn = NGSIConstants.QUERY_PARAMETER_TYPE;
					if (entry.getValue().indexOf(",") == -1) {
						sqlOperator = "=";
						sqlWhereProperty = dbColumn + " " + sqlOperator + " '" + entry.getValue() + "'";
					} else {
						sqlOperator = "IN";
						sqlWhereProperty = dbColumn + " " + sqlOperator + " ('" + entry.getValue().replace(",", "','") + "')";
					}			

					break;
				case NGSIConstants.NGSI_LD_ID_PATTERN:
					dbColumn = DBConstants.DBCOLUMN_ID;
					sqlOperator = "~";
					sqlWhereProperty = dbColumn + " " + sqlOperator + " '" + entry.getValue() + "'";
					break;

				default:
					break;
				}
				fullSqlWhereProperty.append(sqlWhereProperty);
				fullSqlWhereProperty.append(" AND ");
			}
			fullSqlWhereProperty.delete(fullSqlWhereProperty.length() - 5, fullSqlWhereProperty.length());
			fullSqlWhereProperty.append(") OR ");
		}
		fullSqlWhereProperty.delete(fullSqlWhereProperty.length() - 4, fullSqlWhereProperty.length());
		fullSqlWhereProperty.append(")");
		if (qp.getAttrs() != null) {
			String queryValue;
			queryValue = qp.getAttrs();
			dbColumn = "data";
			sqlOperator = "?";
			if (queryValue.indexOf(",") == -1) {
				sqlWhereProperty = dbColumn + " " + sqlOperator + "'" + queryValue + "'";
			} else {
				sqlWhereProperty = "(" + dbColumn + " " + sqlOperator + " '"
						+ queryValue.replace(",", "' OR " + dbColumn + " " + sqlOperator + "'") + "')";
			}
			fullSqlWhereProperty.append(" AND ");
			fullSqlWhereProperty.append(sqlWhereProperty);

		}
		if (qp.getGeorel() != null) {
			GeoqueryRel gqr = qp.getGeorel();
			logger.trace("Georel value " + gqr.getGeorelOp());
			try {
				sqlWhereProperty = translateNgsildGeoqueryToPostgisQuery(gqr, qp.getGeometry(), qp.getCoordinates(),
						qp.getGeoproperty());
			} catch (ResponseException e) {
				e.printStackTrace();
			}
			fullSqlWhereProperty.append(" AND ");
			fullSqlWhereProperty.append(sqlWhereProperty);

		}
		if (qp.getQ() != null) {
			sqlWhereProperty = qp.getQ();
			fullSqlWhereProperty.append(" AND ");
			fullSqlWhereProperty.append(sqlWhereProperty);
		}

		String sqlQuery = "SELECT Count(*) FROM " + DBConstants.DBTABLE_ENTITY + " ";
		if (fullSqlWhereProperty.length() > 0) {
			sqlQuery += "WHERE " + fullSqlWhereProperty.toString() + " ";
		}
		int limit = qp.getLimit();
		int offSet = qp.getOffSet();

		if (limit > 0) {
			sqlQuery += "LIMIT " + limit + " ";
		}
		if (offSet > 0) {
			sqlQuery += "OFFSET " + offSet + " ";
		}
		// order by ?
		return sqlQuery;
	}

}