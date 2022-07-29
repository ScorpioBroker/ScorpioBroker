package eu.neclab.ngsildbroker.commons.storage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.neclab.ngsildbroker.commons.constants.DBConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.GeoqueryRel;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.StorageFunctionsInterface;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple3;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.SqlConnection;
import io.vertx.mutiny.sqlclient.Tuple;

import static eu.neclab.ngsildbroker.commons.interfaces.StorageFunctionsInterface.getSQLList;

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

	private Tuple3<String, ArrayList<Object>, Integer> commonTranslateSql(QueryParams qp, int currentCount)
			throws ResponseException {
		StringBuilder fullSqlWhere = new StringBuilder(70);
		String sqlWhere = "";
		int newCount = currentCount;
		ArrayList<Object> replacements = Lists.newArrayList();
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
					Tuple3<String, ArrayList<Object>, Integer> tmp = getCommonSqlWhereForTypeIdIdPattern(typeValue,
							idValue, idPatternValue, newCount);
					newCount = tmp.getItem3();
					replacements.addAll(tmp.getItem2());
					sqlWhere += tmp.getItem1();
					sqlWhere += " AND ";
					tmp = getSqlWhereByAttrsInTypeFiltering(attrsValue, newCount);
					newCount = tmp.getItem3();
					replacements.addAll(tmp.getItem2());
					sqlWhere += tmp.getItem1();

				} else { // query by type + (id, idPattern) only (no attrs)

					sqlWhere += "(c.has_registrationinfo_with_attrs_only) OR ";
					Tuple3<String, ArrayList<Object>, Integer> tmp = getCommonSqlWhereForTypeIdIdPattern(typeValue,
							idValue, idPatternValue, newCount);
					newCount = tmp.getItem3();
					replacements.addAll(tmp.getItem2());
					sqlWhere += tmp.getItem1();

				}
				fullSqlWhere.append(sqlWhere + ") AND ");

				sqlWhere += ") OR ";
			}
			sqlWhere = sqlWhere.substring(0, sqlWhere.length() - 4);

			// query by attrs only
		} else if (qp.getAttrs() != null) {
			String attrsValue = qp.getAttrs();
			if (attrsValue.indexOf(",") == -1) {
				sqlWhere = "ci." + DBCOLUMN_CSOURCE_INFO_PROPERTY_ID + " = $" + newCount + " OR " + "ci."
						+ DBCOLUMN_CSOURCE_INFO_RELATIONSHIP_ID + " = $" + newCount;
				newCount++;
				replacements.add(attrsValue);
			} else {
				Tuple3<String, ArrayList<Object>, Integer> tmp = getSQLList(attrsValue, newCount);
				newCount = tmp.getItem3();
				replacements.addAll(tmp.getItem2());
				sqlWhere = "ci." + DBCOLUMN_CSOURCE_INFO_PROPERTY_ID + " IN ('" + attrsValue.replace(",", "','")
						+ "') OR " + "ci." + DBCOLUMN_CSOURCE_INFO_RELATIONSHIP_ID + " IN (" + tmp.getItem1() + ")";
			}
			fullSqlWhere.append("(" + sqlWhere + ") AND ");

		}

		// advanced query "q"
		if (qp.getQ() != null) {
			logger.debug("'q' filter is not supported in csource discovery!");
		}
		if (qp.getScopeQ() != null) {
			// TODO escape
			sqlWhere = qp.getScopeQ();
			fullSqlWhere.append(sqlWhere);
			fullSqlWhere.append(" AND ");

		}
		if (qp.getCsf() != null) {
			// TODO escape
			sqlWhere = qp.getCsf();
			fullSqlWhere.append(sqlWhere);
			fullSqlWhere.append(" AND ");
		}

		// geoquery
		if (qp.getGeorel() != null) {
			GeoqueryRel gqr = qp.getGeorel();
			logger.debug("Georel value " + gqr.getGeorelOp());

			Tuple3<String, ArrayList<Object>, Integer> tmp = translateNgsildGeoqueryToPostgisQuery(gqr,
					qp.getGeometry(), qp.getCoordinates(), qp.getGeoproperty(), newCount);
			sqlWhere = tmp.getItem1();
			newCount = tmp.getItem3();
			replacements.addAll(tmp.getItem2());

			fullSqlWhere.append(sqlWhere + " AND ");
		}
		return Tuple3.of(fullSqlWhere.toString(), replacements, currentCount);
	}

	@Override
	public Uni<RowSet<Row>> translateNgsildQueryToSql(QueryParams qp, SqlConnection conn) {
		Tuple3<String, ArrayList<Object>, Integer> fullSqlWhere;
		try {
			fullSqlWhere = commonTranslateSql(qp, 1);
		} catch (ResponseException e) {
			return Uni.createFrom().failure(e);
		}
		String sqlQuery = "SELECT DISTINCT c.data as data";
		if (qp.getCountResult()) {
			sqlQuery += ", count(*) OVER() AS count";
		}
		sqlQuery += " FROM " + DBConstants.DBTABLE_CSOURCE + " c ";
		sqlQuery += "INNER JOIN " + DBConstants.DBTABLE_CSOURCE_INFO + " ci ON (ci.csource_id = c.id) ";

		if (fullSqlWhere.getItem1().length() > 0) {
			sqlQuery += "WHERE " + fullSqlWhere.getItem1() + " 1=1 ";
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
		return conn.preparedQuery(sqlQuery).execute(Tuple.from(fullSqlWhere.getItem2()));
	}

	private Tuple3<String, ArrayList<Object>, Integer> getCommonSqlWhereForTypeIdIdPattern(String typeValue,
			String idValue, String idPatternValue, int currentCount) {
		String sqlWhere = "";
		int newCount = currentCount;
		ArrayList<Object> replacements = Lists.newArrayList();
		Tuple3<String, ArrayList<Object>, Integer> tmp;
		if (idValue.isEmpty() && idPatternValue.isEmpty()) { // case 1: type only
			tmp = getSqlWhereByType(typeValue, false, newCount);
			newCount = tmp.getItem3();
			replacements.addAll(tmp.getItem2());
			sqlWhere += tmp.getItem1();
		} else if (!idValue.isEmpty() && idPatternValue.isEmpty()) { // case 2: type+id
			sqlWhere += "(";
			if (typeValue != null) {
				tmp = getSqlWhereByType(typeValue, true, newCount);
				newCount = tmp.getItem3();
				replacements.addAll(tmp.getItem2());
				sqlWhere += tmp.getItem1();
				sqlWhere += " OR ";
			}
			tmp = getSqlWhereById(typeValue, idValue, newCount);
			newCount = tmp.getItem3();
			replacements.addAll(tmp.getItem2());
			sqlWhere += tmp.getItem1();
			sqlWhere += ")";
		} else if (idValue.isEmpty() && !idPatternValue.isEmpty()) { // case 3: type+idPattern
			sqlWhere += "(";
			if (typeValue != null) {
				tmp = getSqlWhereByType(typeValue, true, newCount);
				newCount = tmp.getItem3();
				replacements.addAll(tmp.getItem2());
				sqlWhere += tmp.getItem1();
				sqlWhere += " OR ";
			}
			tmp = getSqlWhereByIdPattern(typeValue, idPatternValue, newCount);
			newCount = tmp.getItem3();
			replacements.addAll(tmp.getItem2());
			sqlWhere += tmp.getItem1();
			sqlWhere += ")";
		}
		return Tuple3.of(sqlWhere, replacements, newCount);
	}

	private Tuple3<String, ArrayList<Object>, Integer> getSqlWhereByType(String typeValue,
			boolean includeIdAndIdPatternNullTest, int currentCount) {
		int newCount = currentCount;
		ArrayList<Object> replacements = Lists.newArrayList();

		String sqlWhere = "(";
		if (typeValue.indexOf(",") == -1) {
			sqlWhere += "ci." + DBCOLUMN_CSOURCE_INFO_ENTITY_TYPE + " = $" + newCount + " ";
			newCount++;
			replacements.add(typeValue);
		} else {
			Tuple3<String, ArrayList<Object>, Integer> tmp = getSQLList(typeValue, newCount);
			newCount = tmp.getItem3();
			replacements.addAll(tmp.getItem2());
			sqlWhere += "ci." + DBCOLUMN_CSOURCE_INFO_ENTITY_TYPE + " IN (" + tmp.getItem1() + ") ";
		}
		if (includeIdAndIdPatternNullTest) {
			sqlWhere += "AND ci." + DBCOLUMN_CSOURCE_INFO_ENTITY_ID + " IS NULL AND " + "ci."
					+ DBCOLUMN_CSOURCE_INFO_ENTITY_IDPATTERN + " IS NULL";
		}
		sqlWhere += ")";
		return Tuple3.of(sqlWhere, replacements, newCount);
	}

	private Tuple3<String, ArrayList<Object>, Integer> getSqlWhereById(String typeValue, String idValue,
			int currentCount) {
		int newCount = currentCount;
		ArrayList<Object> replacements = Lists.newArrayList();
		String sqlWhere = "( ";
		if (typeValue != null) {
			if (typeValue.indexOf(",") == -1) {
				sqlWhere += "ci." + DBCOLUMN_CSOURCE_INFO_ENTITY_TYPE + " = $" + newCount + " AND ";
				replacements.add(typeValue);
				newCount++;
			} else {
				Tuple3<String, ArrayList<Object>, Integer> tmp = getSQLList(typeValue, newCount);
				newCount = tmp.getItem3();
				replacements.addAll(tmp.getItem2());

				sqlWhere += "ci." + DBCOLUMN_CSOURCE_INFO_ENTITY_TYPE + " IN (" + tmp.getItem1() + ") AND ";
			}
		}
		if (idValue.indexOf(",") == -1) {
			sqlWhere += "(" + "ci." + DBCOLUMN_CSOURCE_INFO_ENTITY_ID + " = $" + newCount + " OR " + "$" + newCount
					+ " ~ " + "ci." + DBCOLUMN_CSOURCE_INFO_ENTITY_IDPATTERN + ")";
			replacements.add(idValue);
			newCount++;
		} else {
			String[] ids = idValue.split(",");
			String whereId = "ci." + DBCOLUMN_CSOURCE_INFO_ENTITY_ID + " IN ( ";
			String whereIdPattern = "(";
			for (String id : ids) {
				whereId += "$" + newCount + ",";
				whereIdPattern += "ci." + DBCOLUMN_CSOURCE_INFO_ENTITY_IDPATTERN + " ~ $" + newCount + " OR ";
				newCount++;
				replacements.add(id);
			}
			whereId = StringUtils.removeEnd(whereId, ",");
			whereIdPattern = StringUtils.removeEnd(whereIdPattern, "OR ");
			whereId += ")";
			whereIdPattern += ")";

			sqlWhere += "(" + whereId + " OR " + whereIdPattern + ")";
		}

		sqlWhere += " )";
		return Tuple3.of(sqlWhere, replacements, newCount);
	}

	private Tuple3<String, ArrayList<Object>, Integer> getSqlWhereByIdPattern(String typeValue, String idPatternValue,
			int currentCount) {
		int newCount = currentCount;
		ArrayList<Object> replacements = Lists.newArrayList();
		String sqlWhere = "( ";
		if (typeValue != null) {
			if (typeValue.indexOf(",") == -1) {
				sqlWhere += "ci." + DBCOLUMN_CSOURCE_INFO_ENTITY_TYPE + " = $" + newCount + " AND ";
				replacements.add(typeValue);
				newCount++;
			} else {
				Tuple3<String, ArrayList<Object>, Integer> tmp = getSQLList(typeValue, newCount);
				newCount = tmp.getItem3();
				replacements.addAll(tmp.getItem2());
				sqlWhere += "ci." + DBCOLUMN_CSOURCE_INFO_ENTITY_TYPE + " IN (" + tmp.getItem1() + ") AND ";
			}
		}
		sqlWhere += "(" + "ci." + DBCOLUMN_CSOURCE_INFO_ENTITY_ID + " ~ $" + newCount + " OR " + "ci."
				+ DBCOLUMN_CSOURCE_INFO_ENTITY_IDPATTERN + " ~ $" + newCount + ")";
		newCount++;
		replacements.add(idPatternValue);
		sqlWhere += " )";
		return Tuple3.of(sqlWhere, replacements, newCount);
	}

	private Tuple3<String, ArrayList<Object>, Integer> getSqlWhereByAttrsInTypeFiltering(String attrsValue,
			int currentCount) {
		String sqlWhere;
		int newCount = currentCount;
		ArrayList<Object> replacements = Lists.newArrayList();
		sqlWhere = "( NOT EXISTS (SELECT 1 FROM csourceinformation ci2  WHERE ci2.group_id = ci.group_id AND  (ci2.property_id IS NOT NULL OR ci2.relationship_id IS NOT NULL)) OR EXISTS (SELECT 1 FROM csourceinformation ci3 WHERE ci3.group_id = ci.group_id AND ";
		if (attrsValue.indexOf(",") == -1) {
			sqlWhere += "(ci3.property_id = $" + newCount + " OR " + " ci3.relationship_id = $" + newCount + ") ";
			replacements.add(attrsValue);
			newCount++;
		} else {
			Tuple3<String, ArrayList<Object>, Integer> tmp = getSQLList(attrsValue, newCount);
			newCount = tmp.getItem3();
			replacements.addAll(tmp.getItem2());
			sqlWhere += "(ci3.property_id IN (" + tmp.getItem1() + ") OR " + " ci3.relationship_id IN ("
					+ tmp.getItem1() + ") ) ";
		}
		sqlWhere += ") )";
		return Tuple3.of(sqlWhere, replacements, newCount);
	}

	@Override
	public Tuple3<String, ArrayList<Object>, Integer> translateNgsildGeoqueryToPostgisQuery(GeoqueryRel georel,
			String geometry, String coordinates, String geoproperty, int currentCount) throws ResponseException {
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
		return Tuple3.of(sqlWhere.toString(), Lists.newArrayList(), currentCount);
	}

	@Override
	public Uni<RowSet<Row>> translateNgsildQueryToCountResult(QueryParams qp, SqlConnection conn) {
		Tuple3<String, ArrayList<Object>, Integer> fullSqlWhereProperty;
		try {
			fullSqlWhereProperty = commonTranslateSql(qp, 1);
		} catch (ResponseException e) {
			return Uni.createFrom().failure(e);
		}
		String tableName = DBConstants.DBTABLE_CSOURCE + " c ";
		String sqlQuery = "SELECT Count(*) FROM " + tableName + " INNER JOIN " + DBConstants.DBTABLE_CSOURCE_INFO
				+ " ci ON (ci.csource_id = c.id) ";
		if (fullSqlWhereProperty.getItem1().length() > 0) {
			sqlQuery += "WHERE " + fullSqlWhereProperty.getItem1() + " 1=1 ";
		}
		return conn.preparedQuery(sqlQuery).execute(Tuple.from(fullSqlWhereProperty.getItem2()));
	}

	@Override
	public Uni<RowSet<Row>> typesAndAttributeQuery(QueryParams qp, SqlConnection conn) {
		return Uni.createFrom().nullItem();
	}

	@Override
	public String getAllIdsQuery() {
		return "SELECT DISTINCT id FROM csource";
	}

	@Override
	public String getEntryQuery() {
		return "SELECT data FROM csource WHERE id=$1";
	}

}
