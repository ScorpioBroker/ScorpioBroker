package eu.neclab.ngsildbroker.commons.storage;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.neclab.ngsildbroker.commons.constants.DBConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.GeoqueryRel;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.StorageFunctionsInterface;

public class RegistryStorageFunctions implements StorageFunctionsInterface {

	private final static Logger logger = LoggerFactory.getLogger(RegistryStorageFunctions.class);
	private final static String DBCOLUMN_CSOURCE_INFO_ENTITY_ID = "entity_id";
	private final static String DBCOLUMN_CSOURCE_INFO_ENTITY_IDPATTERN = "entity_idpattern";
	private final static String DBCOLUMN_CSOURCE_INFO_ENTITY_TYPE = "entity_type";
	private final static String DBCOLUMN_CSOURCE_INFO_PROPERTY_ID = "property_id";
	private final static String DBCOLUMN_CSOURCE_INFO_RELATIONSHIP_ID = "relationship_id";

	private final static Map<String, String> NGSILD_TO_SQL_RESERVED_PROPERTIES_MAPPING_GEO = initNgsildToSqlReservedPropertiesMappingGeo();

	private static Map<String, String> initNgsildToSqlReservedPropertiesMappingGeo() {
		Map<String, String> map = new HashMap<>();
		map.put(NGSIConstants.NGSI_LD_LOCATION, DBConstants.DBCOLUMN_LOCATION);
		return Collections.unmodifiableMap(map);
	}

	private final static Map<String, String> NGSILD_TO_POSTGIS_GEO_OPERATORS_MAPPING = initNgsildToPostgisGeoOperatorsMapping();

	private static Map<String, String> initNgsildToPostgisGeoOperatorsMapping() {
		Map<String, String> map = new HashMap<>();
		map.put(NGSIConstants.GEO_REL_NEAR, null);
		map.put(NGSIConstants.GEO_REL_WITHIN, DBConstants.POSTGIS_INTERSECTS);
		map.put(NGSIConstants.GEO_REL_CONTAINS, DBConstants.POSTGIS_CONTAINS);
		map.put(NGSIConstants.GEO_REL_OVERLAPS, null);
		map.put(NGSIConstants.GEO_REL_INTERSECTS, DBConstants.POSTGIS_INTERSECTS);
		map.put(NGSIConstants.GEO_REL_EQUALS, DBConstants.POSTGIS_CONTAINS);
		map.put(NGSIConstants.GEO_REL_DISJOINT, null);
		return Collections.unmodifiableMap(map);
	}

	private String commonTranslateSql(QueryParams qp) {
		StringBuilder fullSqlWhere = new StringBuilder(70);
		String sqlWhere = "";

		// if (externalCsourcesOnly) {
		// fullSqlWhere.append("(c.internal = false) AND ");
		// }

		List<Map<String, String>> entities = qp.getEntities();
		// query by type + (id, idPattern)
		if (entities != null && !entities.isEmpty()) {
			for (Map<String, String> entityInfo : entities) {
				sqlWhere += "(";
				String typeValue = entityInfo.get(NGSIConstants.JSON_LD_TYPE);
				String idValue = "";
				String idPatternValue = "";
				if (entityInfo.containsKey(NGSIConstants.JSON_LD_ID)) {
					idValue = entityInfo.get(NGSIConstants.JSON_LD_ID);
				}
				if (entityInfo.containsKey(NGSIConstants.NGSI_LD_ID_PATTERN)) {
					idPatternValue = entityInfo.get(NGSIConstants.NGSI_LD_ID_PATTERN);
				}
				// id takes precedence on idPattern. clear idPattern if both are given
				if (!idValue.isEmpty() && !idPatternValue.isEmpty())
					idPatternValue = "";

				// query by type + (id, idPattern) + attrs
				if (qp.getAttrs() != null) {
					String attrsValue = qp.getAttrs();
					sqlWhere += getCommonSqlWhereForTypeIdIdPattern(typeValue, idValue, idPatternValue);
					sqlWhere += " AND ";
					sqlWhere += getSqlWhereByAttrsInTypeFiltering(attrsValue);

				} else { // query by type + (id, idPattern) only (no attrs)

					sqlWhere += "(c.has_registrationinfo_with_attrs_only) OR ";
					sqlWhere += getCommonSqlWhereForTypeIdIdPattern(typeValue, idValue, idPatternValue);

				}
				fullSqlWhere.append(sqlWhere + ") AND ");

				sqlWhere += ") OR ";
			}
			sqlWhere = sqlWhere.substring(0, sqlWhere.length() - 4);

			// query by attrs only
		} else if (qp.getAttrs() != null) {
			String attrsValue = qp.getAttrs();
			if (attrsValue.indexOf(",") == -1) {
				sqlWhere = "ci." + DBCOLUMN_CSOURCE_INFO_PROPERTY_ID + " = '" + attrsValue + "' OR " + "ci."
						+ DBCOLUMN_CSOURCE_INFO_RELATIONSHIP_ID + " = '" + attrsValue + "'";
			} else {
				sqlWhere = "ci." + DBCOLUMN_CSOURCE_INFO_PROPERTY_ID + " IN ('" + attrsValue.replace(",", "','")
						+ "') OR " + "ci." + DBCOLUMN_CSOURCE_INFO_RELATIONSHIP_ID + " IN ('"
						+ attrsValue.replace(",", "','") + "')";
			}
			fullSqlWhere.append("(" + sqlWhere + ") AND ");

		}

		// advanced query "q"
		if (qp.getQ() != null) {
			logger.debug("'q' filter is not supported in csource discovery!");
		}
		if (qp.getScopeQ() != null) {
			sqlWhere = qp.getScopeQ();
			fullSqlWhere.append(sqlWhere);
			fullSqlWhere.append(" AND ");

		}
		if (qp.getCsf() != null) {
			sqlWhere = qp.getCsf();
			fullSqlWhere.append(sqlWhere);
			fullSqlWhere.append(" AND ");
		}

		// geoquery
		if (qp.getGeorel() != null) {
			GeoqueryRel gqr = qp.getGeorel();
			logger.debug("Georel value " + gqr.getGeorelOp());
			try {
				sqlWhere = translateNgsildGeoqueryToPostgisQuery(gqr, qp.getGeometry(), qp.getCoordinates(),
						qp.getGeoproperty());
			} catch (ResponseException e) {
				e.printStackTrace();
			}
			fullSqlWhere.append(sqlWhere + " AND ");
		}
		return fullSqlWhere.toString();
	}

	@Override
	public String translateNgsildQueryToSql(QueryParams qp) {

		String fullSqlWhere = commonTranslateSql(qp);
		String sqlQuery = "SELECT DISTINCT c.data as data";
		if (qp.getCountResult()) {
			sqlQuery += ", count(*) OVER() AS count";
		}

		sqlQuery += " FROM " + DBConstants.DBTABLE_CSOURCE + " c ";

		sqlQuery += "INNER JOIN " + DBConstants.DBTABLE_CSOURCE_INFO + " ci ON (ci.csource_id = c.id) ";

		if (fullSqlWhere.length() > 0) {
			sqlQuery += "WHERE " + fullSqlWhere + " 1=1 ";
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

	private String getCommonSqlWhereForTypeIdIdPattern(String typeValue, String idValue, String idPatternValue) {
		String sqlWhere = "";
		if (idValue.isEmpty() && idPatternValue.isEmpty()) { // case 1: type only
			sqlWhere += getSqlWhereByType(typeValue, false);
		} else if (!idValue.isEmpty() && idPatternValue.isEmpty()) { // case 2: type+id
			sqlWhere += "(";
			if (typeValue != null) {
				sqlWhere += getSqlWhereByType(typeValue, true);
				sqlWhere += " OR ";
			}
			sqlWhere += getSqlWhereById(typeValue, idValue);
			sqlWhere += ")";
		} else if (idValue.isEmpty() && !idPatternValue.isEmpty()) { // case 3: type+idPattern
			sqlWhere += "(";
			if (typeValue != null) {
				sqlWhere += getSqlWhereByType(typeValue, true);
				sqlWhere += " OR ";
			}
			sqlWhere += getSqlWhereByIdPattern(typeValue, idPatternValue);
			sqlWhere += ")";
		}
		return sqlWhere;
	}

	private String getSqlWhereByType(String typeValue, boolean includeIdAndIdPatternNullTest) {
		String sqlWhere = "(";
		if (typeValue.indexOf(",") == -1) {
			sqlWhere += "ci." + DBCOLUMN_CSOURCE_INFO_ENTITY_TYPE + " = '" + typeValue + "' ";
		} else {
			sqlWhere += "ci." + DBCOLUMN_CSOURCE_INFO_ENTITY_TYPE + " IN ('" + typeValue.replace(",", "','") + "') ";
		}
		if (includeIdAndIdPatternNullTest)
			sqlWhere += "AND ci." + DBCOLUMN_CSOURCE_INFO_ENTITY_ID + " IS NULL AND " + "ci."
					+ DBCOLUMN_CSOURCE_INFO_ENTITY_IDPATTERN + " IS NULL";
		sqlWhere += ")";
		return sqlWhere;
	}

	private String getSqlWhereById(String typeValue, String idValue) {
		String sqlWhere = "( ";
		if (typeValue != null) {
			if (typeValue.indexOf(",") == -1) {
				sqlWhere += "ci." + DBCOLUMN_CSOURCE_INFO_ENTITY_TYPE + " = '" + typeValue + "' AND ";
			} else {
				sqlWhere += "ci." + DBCOLUMN_CSOURCE_INFO_ENTITY_TYPE + " IN ('" + typeValue.replace(",", "','")
						+ "') AND ";
			}
		}
		if (idValue.indexOf(",") == -1) {
			sqlWhere += "(" + "ci." + DBCOLUMN_CSOURCE_INFO_ENTITY_ID + " = '" + idValue + "' OR " + "'" + idValue
					+ "' ~ " + "ci." + DBCOLUMN_CSOURCE_INFO_ENTITY_IDPATTERN + ")";
		} else {
			String[] ids = idValue.split(",");
			String whereId = "ci." + DBCOLUMN_CSOURCE_INFO_ENTITY_ID + " IN ( ";
			String whereIdPattern = "(";
			for (String id : ids) {
				whereId += "'" + id + "',";
				whereIdPattern += "ci." + DBCOLUMN_CSOURCE_INFO_ENTITY_IDPATTERN + " ~ '" + id + "' OR ";
			}
			whereId = StringUtils.chomp(whereId, ",");
			whereIdPattern = StringUtils.chomp(whereIdPattern, "OR ");
			whereId += ")";
			whereIdPattern += ")";

			sqlWhere += "(" + whereId + " OR " + whereIdPattern + ")";
		}

		sqlWhere += " )";
		return sqlWhere;
	}

	private String getSqlWhereByIdPattern(String typeValue, String idPatternValue) {
		String sqlWhere = "( ";
		if (typeValue.indexOf(",") == -1) {
			sqlWhere += "ci." + DBCOLUMN_CSOURCE_INFO_ENTITY_TYPE + " = '" + typeValue + "' AND ";
		} else {
			sqlWhere += "ci." + DBCOLUMN_CSOURCE_INFO_ENTITY_TYPE + " IN ('" + typeValue.replace(",", "','")
					+ "') AND ";
		}
		sqlWhere += "(" + "ci." + DBCOLUMN_CSOURCE_INFO_ENTITY_ID + " ~ '" + idPatternValue + "' OR " + "ci."
				+ DBCOLUMN_CSOURCE_INFO_ENTITY_IDPATTERN + " ~ '" + idPatternValue + "')";
		sqlWhere += " )";
		return sqlWhere;
	}

	private String getSqlWhereByAttrsInTypeFiltering(String attrsValue) {
		String sqlWhere;
		sqlWhere = "( " + "NOT EXISTS (SELECT 1 FROM csourceinformation ci2 "
				+ "	          WHERE ci2.group_id = ci.group_id AND "
				+ "	                (ci2.property_id IS NOT NULL OR ci2.relationship_id IS NOT NULL)) " + "OR "
				+ "EXISTS (SELECT 1 FROM csourceinformation ci3 " + "        WHERE ci3.group_id = ci.group_id AND ";
		if (attrsValue.indexOf(",") == -1) {
			sqlWhere += "(ci3.property_id = '" + attrsValue + "' OR " + " ci3.relationship_id = '" + attrsValue + "') ";
		} else {
			sqlWhere += "(ci3.property_id IN ('" + attrsValue.replace(",", "','") + "') OR "
					+ " ci3.relationship_id IN ('" + attrsValue.replace(",", "','") + "') ) ";
		}
		sqlWhere += ") )";
		return sqlWhere;
	}

	@Override
	public String translateNgsildGeoqueryToPostgisQuery(GeoqueryRel georel, String geometry, String coordinates,
			String geoproperty) throws ResponseException {
		if (georel.getGeorelOp().isEmpty() || geometry == null || coordinates == null || geometry.isEmpty()
				|| coordinates.isEmpty()) {
			logger.error("georel, geometry and coordinates are empty or invalid!");
			throw new ResponseException(ErrorType.BadRequestData,
					"georel, geometry and coordinates are empty or invalid!");
		}

		StringBuilder sqlWhere = new StringBuilder(50);

		String georelOp = georel.getGeorelOp();
		logger.debug("  Geoquery term georelOp: " + georelOp);

		String dbColumn = NGSILD_TO_SQL_RESERVED_PROPERTIES_MAPPING_GEO.get(geoproperty);
		if (dbColumn == null) {
			dbColumn = "ST_SetSRID(ST_GeomFromGeoJSON( getGeoJson( c.data#>'{" + geoproperty + ",0}') ), 4326)";
		} else {
			dbColumn = "c." + dbColumn;
		}

		String referenceValue = "ST_SetSRID(ST_GeomFromGeoJSON('{\"type\": \"" + geometry + "\", \"coordinates\": "
				+ coordinates + " }'), 4326)";

		switch (georelOp) {
			case NGSIConstants.GEO_REL_WITHIN:
			case NGSIConstants.GEO_REL_CONTAINS:
			case NGSIConstants.GEO_REL_INTERSECTS:
			case NGSIConstants.GEO_REL_EQUALS:
				sqlWhere.append(NGSILD_TO_POSTGIS_GEO_OPERATORS_MAPPING.get(georelOp) + "( " + dbColumn + ", "
						+ referenceValue + ") ");
				break;
			case NGSIConstants.GEO_REL_NEAR:
				if (georel.getDistanceType() != null && georel.getDistanceValue() != null) {
					if (georel.getDistanceType().equals(NGSIConstants.GEO_REL_MIN_DISTANCE))
						sqlWhere.append("NOT " + DBConstants.POSTGIS_WITHIN + "( " + dbColumn + ", ST_Buffer("
								+ referenceValue + "::geography, " + georel.getDistanceValue() + ")::geometry ) ");
					else
						sqlWhere.append(DBConstants.POSTGIS_INTERSECTS + "( " + dbColumn + ", ST_Buffer("
								+ referenceValue + "::geography, " + georel.getDistanceValue() + ")::geometry ) ");
				} else {
					throw new ResponseException(ErrorType.BadRequestData,
							"GeoQuery: Type and distance are required for near relation");
				}
				break;
			case NGSIConstants.GEO_REL_OVERLAPS:
				sqlWhere.append("(");
				sqlWhere.append(DBConstants.POSTGIS_OVERLAPS + "( " + dbColumn + ", " + referenceValue + ")");
				sqlWhere.append(" OR ");
				sqlWhere.append(DBConstants.POSTGIS_CONTAINS + "( " + dbColumn + ", " + referenceValue + ")");
				sqlWhere.append(")");
				break;
			case NGSIConstants.GEO_REL_DISJOINT:
				sqlWhere.append("NOT " + DBConstants.POSTGIS_WITHIN + "( " + dbColumn + ", " + referenceValue + ") ");
				break;
			default:
				throw new ResponseException(ErrorType.BadRequestData, "Invalid georel operator: " + georelOp);
		}
		return sqlWhere.toString();
	}

	@Override
	public String translateNgsildQueryToCountResult(QueryParams qp) {
		String fullSqlWhereProperty = commonTranslateSql(qp);
		String tableName = DBConstants.DBTABLE_CSOURCE + " c ";
		String sqlQuery = "SELECT Count(*) FROM " + tableName + " INNER JOIN " + DBConstants.DBTABLE_CSOURCE_INFO
				+ " ci ON (ci.csource_id = c.id) ";
		if (fullSqlWhereProperty.length() > 0) {
			sqlQuery += "WHERE " + fullSqlWhereProperty.toString() + " 1=1 ";
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
		// TODO Auto-generated method stub
		return "";
	}

}
