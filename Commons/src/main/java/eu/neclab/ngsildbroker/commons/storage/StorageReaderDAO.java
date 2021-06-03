package eu.neclab.ngsildbroker.commons.storage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.util.ReflectionUtils;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import eu.neclab.ngsildbroker.commons.constants.DBConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.GeoqueryRel;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tenant.DBUtil;

abstract public class StorageReaderDAO {

	private final static Logger logger = LogManager.getLogger(StorageReaderDAO.class);
	protected Map<Object, DataSource> resolvedDataSources = new HashMap<>();

	@Autowired
	protected JdbcTemplate readerJdbcTemplate;
	@Autowired
	public DataSource masterDataSource;

	@Autowired
	private HikariConfig hikariConfig;

	public Random random = new Random();

	public static int countHeader = 0;

	@PostConstruct
	public void init() {
		readerJdbcTemplate.execute("SELECT 1"); // create connection pool and connect to database
	}

	public String findDataBaseNameByTenantId(String tenantidvalue) {
		if (tenantidvalue == null)
			return null;
		try {
			synchronized (readerJdbcTemplate) {
				readerJdbcTemplate = new JdbcTemplate(masterDataSource);
			}
			// String databasename="ngbcsource2";
			// SELECT EXISTS(SELECT datname FROM pg_database WHERE datname = 'tenant2');
			String sql = "SELECT database_name FROM tenant WHERE tenant_id = ?";
			String databasename;
			synchronized (readerJdbcTemplate) {
				databasename = readerJdbcTemplate.queryForObject(sql, new Object[] { tenantidvalue }, String.class);
			}
			List<String> data;
			synchronized (readerJdbcTemplate) {
				data = readerJdbcTemplate.queryForList("SELECT datname FROM pg_database", String.class);
			}
			if (data.contains(databasename)) {
				return databasename;
			} else {
				return null;
			}
		} catch (EmptyResultDataAccessException e) {
			return null;
		}
	}

	public DataSource determineTargetDataSource(String tenantidvalue) {
		// String tenantidvalue = (String) determineCurrentLookupKey();
		if (tenantidvalue == null) {
			return masterDataSource;
		}
		DataSource tenantDataSource = resolvedDataSources.get(tenantidvalue);
		if (tenantDataSource == null) {
			try {
				tenantDataSource = createDataSourceForTenantId(tenantidvalue);
			} catch (ResponseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
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

	public List<String> query(QueryParams qp) throws ResponseException {
		String tenantId = qp.getTenant();
		setTenant(tenantId);
		try {
			if (qp.getCheck() != null) {
				String sqlQuery = typesAndAttributeQuery(qp);
				return readerJdbcTemplate.queryForList(sqlQuery, String.class);
			}
			String sqlQuery = translateNgsildQueryToSql(qp);
			logger.info("NGSI-LD to SQL: " + sqlQuery);
			// SqlRowSet result = readerJdbcTemplate.queryForRowSet(sqlQuery);
			if (qp.getLimit() == 0 && qp.getCountResult() == true) {
				List<String> list = readerJdbcTemplate.queryForList(sqlQuery, String.class);
				countHeader = countHeader + list.size();
				return new ArrayList<String>();
			}
			List<String> list = readerJdbcTemplate.queryForList(sqlQuery, String.class);
			countHeader = countHeader + list.size();
			return list;
		} catch (DataIntegrityViolationException e) {
			// Empty result don't worry
			logger.warn("SQL Result Exception::", e);
			return new ArrayList<String>();
		} catch (Exception e) {
			logger.error("Exception ::", e);
		}
		return new ArrayList<String>();

	}

	protected void setTenant(String tenantId) throws ResponseException {
		if (tenantId != null) {
			DataSource finaldatasource = determineTargetDataSource(tenantId);
			if (finaldatasource == null) {
				throw new ResponseException(ErrorType.TenantNotFound);
			}
			synchronized (readerJdbcTemplate) {
				readerJdbcTemplate = new JdbcTemplate(finaldatasource);
			}
		} else {
			synchronized (readerJdbcTemplate) {
				readerJdbcTemplate = new JdbcTemplate(masterDataSource);
			}
		}
	}

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
						sqlWhereProperty = "(" + dbColumn + " " + sqlOperator + " '"
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

		if (limit == 0) {
			sqlQuery += "";
		} else {
			sqlQuery += "LIMIT " + limit + " ";
		}
		if (offSet != -1) {
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
