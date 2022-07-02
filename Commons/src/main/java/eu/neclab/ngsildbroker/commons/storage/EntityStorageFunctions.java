package eu.neclab.ngsildbroker.commons.storage;

import java.util.List;
import java.util.Map;
import java.util.Random;
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

public class EntityStorageFunctions implements StorageFunctionsInterface {

	private final static Logger logger = LoggerFactory.getLogger(EntityStorageFunctions.class);
	private Random random = new Random();

	public String translateNgsildQueryToSql(QueryParams qp) throws ResponseException {
		String fullSqlWhereProperty = commonTranslateSql(qp);
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
				expandedAttributeList += ",'" + NGSIConstants.NGSI_LD_CREATED_AT + "','"
						+ NGSIConstants.NGSI_LD_MODIFIED_AT + "'";
			}
			dataColumn = "(SELECT jsonb_object_agg(key, value) FROM jsonb_each(" + tableDataColumn + ") WHERE key IN ( "
					+ expandedAttributeList + "))";
		}
		String sqlQuery = "SELECT DISTINCT " + dataColumn + " as data";
		int limit = qp.getLimit();
		int offSet = qp.getOffSet();
		if (qp.getCountResult()) {
			sqlQuery += ", count(*) OVER() AS count";
		}

		sqlQuery += " FROM " + DBConstants.DBTABLE_ENTITY + " ";
		if (fullSqlWhereProperty.length() > 0) {
			sqlQuery += "WHERE " + fullSqlWhereProperty + " ";
		}

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
	public String translateNgsildGeoqueryToPostgisQuery(GeoqueryRel georel, String geometry, String coordinates,
			String geoproperty) throws ResponseException {
		StringBuilder sqlWhere = new StringBuilder(50);
		String georelOp = georel.getGeorelOp();
		logger.trace("  Geoquery term georelOp: " + georelOp);
		String dbColumn = DBConstants.NGSILD_TO_SQL_RESERVED_PROPERTIES_MAPPING_GEO.get(geoproperty);
		if (dbColumn == null) {
			sqlWhere.append("data @> '{\"" + geoproperty + "\": [{\"" + NGSIConstants.JSON_LD_TYPE + "\":[\""
					+ NGSIConstants.NGSI_LD_GEOPROPERTY + "\"]}]}' AND ");
			dbColumn = "ST_SetSRID(ST_GeomFromGeoJSON( getGeoJson( " + "data#>'{" + geoproperty + ",0,"
					+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0}') ), 4326)";
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

	private String commonTranslateSql(QueryParams qp) {
		StringBuilder fullSqlWhereProperty = new StringBuilder(70);
		String dbColumn, sqlOperator;
		String sqlWhereProperty = null;
		List<Map<String, String>> entities = qp.getEntities();
		boolean entitiesAdded = false;
		fullSqlWhereProperty.append("(");
		if (entities != null) {
			for (Map<String, String> entityInfo : entities) {
				fullSqlWhereProperty.append("(");
				entitiesAdded = true;
				for (Entry<String, String> entry : entityInfo.entrySet()) {
					switch (entry.getKey()) {
						case NGSIConstants.JSON_LD_ID:
							dbColumn = NGSIConstants.QUERY_PARAMETER_ID;
							if (entry.getValue().indexOf(",") == -1) {
								sqlOperator = "=";
								sqlWhereProperty = dbColumn + " " + sqlOperator + " '" + entry.getValue() + "'";
							} else {
								sqlOperator = "IN";
								sqlWhereProperty = dbColumn + " " + sqlOperator + " ('"
										+ entry.getValue().replace(",", "','") + "')";
							}
							break;
						case NGSIConstants.JSON_LD_TYPE:
							dbColumn = NGSIConstants.QUERY_PARAMETER_TYPE;
							if (entry.getValue().indexOf(",") == -1) {
								sqlOperator = "=";
								sqlWhereProperty = dbColumn + " " + sqlOperator + " '" + entry.getValue() + "'";
							} else {
								sqlOperator = "IN";
								sqlWhereProperty = dbColumn + " " + sqlOperator + " ('"
										+ entry.getValue().replace(",", "','") + "')";
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
		}
		if (entitiesAdded) {
			fullSqlWhereProperty.delete(fullSqlWhereProperty.length() - 4, fullSqlWhereProperty.length());
			fullSqlWhereProperty.append(")");
		} else {
			fullSqlWhereProperty.delete(0, 1);
		}
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
			if (entitiesAdded) {
				fullSqlWhereProperty.append(" AND ");
			}
			fullSqlWhereProperty.append(sqlWhereProperty);

		}
		if (qp.getScopeQ() != null) {
			sqlWhereProperty = qp.getScopeQ();
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
		return fullSqlWhereProperty.toString();
	}

	public String translateNgsildQueryToCountResult(QueryParams qp) throws ResponseException {
		String fullSqlWhereProperty = commonTranslateSql(qp);
		String tableName = DBConstants.DBTABLE_ENTITY;
		String sqlQuery = "SELECT Count(*) FROM " + tableName + " ";
		if (fullSqlWhereProperty.length() > 0) {
			sqlQuery += "WHERE " + fullSqlWhereProperty.toString() + " ";
		}
//		int limit = qp.getLimit();
//		int offSet = qp.getOffSet();
//
//		if (limit > 0) {
//			sqlQuery += "LIMIT " + limit + " ";
//		}
//		if (offSet > 0) {
//			sqlQuery += "OFFSET " + offSet + " ";
//		}
		// order by ?
		return sqlQuery;
	}

	@Override
	public String typesAndAttributeQuery(QueryParams qp) {
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

}
