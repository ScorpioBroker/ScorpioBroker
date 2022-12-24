package eu.neclab.ngsildbroker.commons.storage;

import static eu.neclab.ngsildbroker.commons.interfaces.StorageFunctionsInterface.getSQLList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Lists;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple3;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.SqlConnection;
import io.vertx.mutiny.sqlclient.Tuple;
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

	@Override
	public Uni<RowSet<Row>> translateNgsildQueryToSql(QueryParams qp, SqlConnection conn) {
		int currentCount = 1;
		ArrayList<Object> replacements = Lists.newArrayList();
		Tuple3<String, ArrayList<Object>, Integer> fullSqlWhereProperty;
		try {
			fullSqlWhereProperty = commonTranslateSql(qp, currentCount);
		} catch (ResponseException e) {
			return Uni.createFrom().failure(e);
		}
		currentCount = fullSqlWhereProperty.getItem3();
		replacements.addAll(fullSqlWhereProperty.getItem2());
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
			Tuple3<String, ArrayList<Object>, Integer> qpAttrs = getSQLList(qp.getAttrs(), currentCount);
			currentCount = qpAttrs.getItem3();
			replacements.addAll(qpAttrs.getItem2());
			String expandedAttributeList = "'" + NGSIConstants.JSON_LD_ID + "','" + NGSIConstants.JSON_LD_TYPE + "',"
					+ qpAttrs.getItem1();
			if (qp.getIncludeSysAttrs()) {
				expandedAttributeList += ",'" + NGSIConstants.NGSI_LD_CREATED_AT + "','"
						+ NGSIConstants.NGSI_LD_MODIFIED_AT + "'";
			}
			dataColumn = "(SELECT jsonb_object_agg(key, value) FROM jsonb_each(" + tableDataColumn + ") WHERE key IN ( "
					+ expandedAttributeList + "))";
		}
		String sqlQuery = "SELECT " + dataColumn + " as data";
		if (qp.getCountResult()) {
			sqlQuery += ", count(*) OVER() AS count";
		}
		sqlQuery += " FROM " + DBConstants.DBTABLE_ENTITY + " ";
		if (fullSqlWhereProperty.getItem1().length() > 0) {
			sqlQuery += "WHERE " + fullSqlWhereProperty.getItem1() + " ";
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
		logger.debug("SQL Query: " + sqlQuery);
		return conn.preparedQuery(sqlQuery).execute(Tuple.from(replacements));
	}

	// TODO: SQL input sanitization
	// TODO: property of property
	// [SPEC] spec is not clear on how to define a "property of property" in
	// the geoproperty field. (probably using dots)
	@Override
	public Tuple3<String, ArrayList<Object>, Integer> translateNgsildGeoqueryToPostgisQuery(GeoqueryRel georel,
			String geometry, String coordinates, String geoproperty, int currentCount) throws ResponseException {
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
		String tableName = DBConstants.DBTABLE_ENTITY;
		String sqlQuery = "SELECT Count(*) FROM " + tableName + " ";
		if (fullSqlWhereProperty.getItem1().length() > 0) {
			sqlQuery += "WHERE " + fullSqlWhereProperty.toString() + " ";
		}
		logger.debug("SQL Query for count: " + sqlQuery);
		return conn.preparedQuery(sqlQuery).execute(Tuple.from(fullSqlWhereProperty.getItem2()));
	}

	private Tuple3<String, ArrayList<Object>, Integer> commonTranslateSql(QueryParams qp, int currentCount)
			throws ResponseException {
		StringBuilder fullSqlWhereProperty = new StringBuilder(70);
		String dbColumn;
		String sqlWhereProperty = null;
		List<Map<String, String>> entities = qp.getEntities();
		boolean entitiesAdded = false;
		int newCount = currentCount;
		ArrayList<Object> replacements = Lists.newArrayList();
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

							sqlWhereProperty = dbColumn + " = $" + currentCount;
							currentCount++;
							replacements.add(entry.getValue());
						} else {
							Tuple3<String, ArrayList<Object>, Integer> tmp = getSQLList(entry.getValue(), currentCount);
							currentCount = tmp.getItem3();
							replacements.addAll(tmp.getItem2());
							sqlWhereProperty = dbColumn + " IN (" + tmp.getItem1() + ")";
						}
						break;
					case NGSIConstants.JSON_LD_TYPE:
						dbColumn = NGSIConstants.QUERY_PARAMETER_TYPE;
						if (entry.getValue().indexOf(",") == -1) {
							sqlWhereProperty = dbColumn + " = $" + currentCount;
							currentCount++;
							replacements.add(entry.getValue());
						} else {
							Tuple3<String, ArrayList<Object>, Integer> tmp = getSQLList(entry.getValue(), currentCount);
							currentCount = tmp.getItem3();
							replacements.addAll(tmp.getItem2());
							sqlWhereProperty = dbColumn + " IN (" + tmp.getItem1() + ")";
						}

						break;
					case NGSIConstants.NGSI_LD_ID_PATTERN:
						dbColumn = DBConstants.DBCOLUMN_ID;
						sqlWhereProperty = dbColumn + " ~ $" + currentCount;
						currentCount++;
						replacements.add(entry.getValue());
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
			if (queryValue.indexOf(",") == -1) {
				sqlWhereProperty = dbColumn + " ? $" + currentCount;
				currentCount++;
				replacements.add(queryValue);
			} else {
				Tuple3<String, ArrayList<Object>, Integer> qpAttrs = getSQLList(queryValue, currentCount);
				currentCount = qpAttrs.getItem3();
				replacements.addAll(qpAttrs.getItem2());
				sqlWhereProperty = "(" + dbColumn + " ? " + qpAttrs.getItem1().replace(",", " OR " + dbColumn + " ? ")
						+ ")";
			}
			if (entitiesAdded) {
				fullSqlWhereProperty.append(" AND ");
			}
			fullSqlWhereProperty.append(sqlWhereProperty);
			entitiesAdded = true;
		}
		if (qp.getScopeQ() != null) {
			sqlWhereProperty = qp.getScopeQ();
			if (entitiesAdded) {
				fullSqlWhereProperty.append(" AND ");
			}
			fullSqlWhereProperty.append(sqlWhereProperty);
			entitiesAdded = true;
		}
		if (qp.getGeorel() != null) {
			GeoqueryRel gqr = qp.getGeorel();
			logger.trace("Georel value " + gqr.getGeorelOp());

			Tuple3<String, ArrayList<Object>, Integer> tmp = translateNgsildGeoqueryToPostgisQuery(gqr,
					qp.getGeometry(), qp.getCoordinates(), qp.getGeoproperty(), newCount);
			sqlWhereProperty = tmp.getItem1();
			currentCount = tmp.getItem3();
			replacements.addAll(tmp.getItem2());
			if (entitiesAdded) {
				fullSqlWhereProperty.append(" AND ");
			}
			fullSqlWhereProperty.append(sqlWhereProperty);
			entitiesAdded = true;

		}
		if (qp.getQ() != null) {
			// TODO SQL escape q;
			sqlWhereProperty = qp.getQ();
			if (entitiesAdded) {
				fullSqlWhereProperty.append(" AND ");
			}
			fullSqlWhereProperty.append(sqlWhereProperty);
			entitiesAdded = true;
		}
		return Tuple3.of(fullSqlWhereProperty.toString(), replacements, currentCount);
	}

	@Override
	public Uni<RowSet<Row>> typesAndAttributeQuery(QueryParams qp, SqlConnection conn) {
		String query = "";
		if (qp.getCheck() == "NonDeatilsType" && qp.getAttrs() == null) {
			int number = random.nextInt(999999);
			query = "select jsonb_build_object('" + NGSIConstants.JSON_LD_ID + "','urn:ngsi-ld:EntityTypeList:" + number
					+ "','" + NGSIConstants.JSON_LD_TYPE + "', jsonb_build_array('" + NGSIConstants.NGSI_LD_ENTITY_LIST
					+ "'), '" + NGSIConstants.NGSI_LD_TYPE_LIST + "',json_agg(distinct jsonb_build_object('"
					+ NGSIConstants.JSON_LD_ID + "', type)::jsonb)) from entity;";
			logger.debug("SQL Query: " + query);
			return conn.preparedQuery(query).execute();
		} else if (qp.getCheck() == "deatilsType" && qp.getAttrs() == null) {
			query = "select distinct jsonb_build_object('" + NGSIConstants.JSON_LD_ID + "',type,'"
					+ NGSIConstants.JSON_LD_TYPE + "', jsonb_build_array('" + NGSIConstants.NGSI_LD_ENTITY_TYPE
					+ "'), '" + NGSIConstants.NGSI_LD_TYPE_NAME + "', jsonb_build_array(jsonb_build_object('"
					+ NGSIConstants.JSON_LD_ID + "', type)), '" + NGSIConstants.NGSI_LD_ATTRIBUTE_NAMES
					+ "', jsonb_agg(jsonb_build_object('" + NGSIConstants.JSON_LD_ID
					+ "', attribute.key))) from entity, jsonb_each(data_without_sysattrs - '" + NGSIConstants.JSON_LD_ID
					+ "' - '" + NGSIConstants.JSON_LD_TYPE + "') attribute group by id;";
			logger.debug("SQL Query: " + query);
			return conn.preparedQuery(query).execute();
		} else if (qp.getCheck() == "type" && qp.getAttrs() != null) {
			query = "with r as (select distinct attribute.key as mykey, jsonb_agg(distinct jsonb_build_object('"
					+ NGSIConstants.JSON_LD_ID
					+ "', attribute.value#>>'{0,@type,0}')) as mytype from entity, jsonb_each(data_without_sysattrs - '"
					+ NGSIConstants.JSON_LD_ID + "' - '" + NGSIConstants.JSON_LD_TYPE + "') attribute where type=$1"
					+ " group by attribute.key)select jsonb_build_object('" + NGSIConstants.JSON_LD_ID + "',type,'"
					+ NGSIConstants.JSON_LD_TYPE + "', jsonb_build_array('" + NGSIConstants.NGSI_LD_ENTITY_TYPE_INFO
					+ "'), '" + NGSIConstants.NGSI_LD_TYPE_NAME + "', jsonb_build_array(jsonb_build_object('"
					+ NGSIConstants.JSON_LD_ID + "', type)),'" + NGSIConstants.NGSI_LD_ENTITY_COUNT
					+ "', jsonb_build_array(jsonb_build_object('" + NGSIConstants.JSON_LD_VALUE
					+ "', count(Distinct id))), '" + NGSIConstants.NGSI_LD_ATTRIBUTE_DETAILS
					+ "', jsonb_agg(distinct jsonb_build_object('" + NGSIConstants.NGSI_LD_ATTRIBUTE_NAME
					+ "',jsonb_build_array(jsonb_build_object('" + NGSIConstants.JSON_LD_ID + "', mykey)), '"
					+ NGSIConstants.NGSI_LD_ATTRIBUTE_TYPES + "', mytype, '" + NGSIConstants.JSON_LD_ID + "', mykey, '"
					+ NGSIConstants.JSON_LD_TYPE + "',jsonb_build_array('" + NGSIConstants.NGSI_LD_ATTRIBUTE
					+ "')))) from entity, r attribute where type = $1 group by type;";
			logger.debug("SQL Query: " + query);
			return conn.preparedQuery(query).execute(Tuple.of(qp.getAttrs()));
		} else if (qp.getCheck() == "NonDeatilsAttributes" && qp.getAttrs() == null) {
			int number = random.nextInt(999999);
			query = "select jsonb_build_object('" + NGSIConstants.JSON_LD_ID + "','urn:ngsi-ld:AttributeList:" + number
					+ "','" + NGSIConstants.JSON_LD_TYPE + "', jsonb_build_array('"
					+ NGSIConstants.NGSI_LD_ATTRIBUTE_LIST_1 + "'), '" + NGSIConstants.NGSI_LD_ATTRIBUTE_LIST_2
					+ "',json_agg(distinct jsonb_build_object('" + NGSIConstants.JSON_LD_ID
					+ "', attribute.key)::jsonb)) from entity,jsonb_each(data_without_sysattrs-'"
					+ NGSIConstants.JSON_LD_ID + "'-'" + NGSIConstants.JSON_LD_TYPE + "') attribute;";
			logger.debug("SQL Query: " + query);
			return conn.preparedQuery(query).execute();

		} else if (qp.getCheck() == "deatilsAttributes" && qp.getAttrs() == null) {
			query = "select jsonb_build_object('" + NGSIConstants.JSON_LD_ID + "', attribute.key,'"
					+ NGSIConstants.JSON_LD_TYPE + "','" + NGSIConstants.NGSI_LD_ATTRIBUTE + "','"
					+ NGSIConstants.NGSI_LD_ATTRIBUTE_NAME + "',jsonb_build_object('" + NGSIConstants.JSON_LD_ID
					+ "', attribute.key),'" + NGSIConstants.NGSI_LD_TYPE_NAMES
					+ "',jsonb_agg(distinct jsonb_build_object('" + NGSIConstants.JSON_LD_ID
					+ "', type))) from entity,jsonb_each(data_without_sysattrs-'" + NGSIConstants.JSON_LD_ID + "'-'"
					+ NGSIConstants.JSON_LD_TYPE + "') attribute group by attribute.key;";
			logger.debug("SQL Query: " + query);
			return conn.preparedQuery(query).execute();

		} else if (qp.getCheck() == "Attribute" && qp.getAttrs() != null) {
			query = "with r as(select count(data_without_sysattrs->$1) as mycount  from entity), y as(select  jsonb_agg(distinct jsonb_build_object('"
					+ NGSIConstants.JSON_LD_ID + "',type)) as mytype,jsonb_build_array(jsonb_build_object('"
					+ NGSIConstants.JSON_LD_VALUE + "',mycount)) as finalcount, jsonb_agg(distinct jsonb_build_object('"
					+ NGSIConstants.JSON_LD_ID
					+ "',attribute.value#>>'{0,@type,0}')) as mydata from r,entity,jsonb_each(data_without_sysattrs-'"
					+ NGSIConstants.JSON_LD_ID + "'-'" + NGSIConstants.JSON_LD_TYPE
					+ "') attribute where attribute.key = $1 group by mycount) select jsonb_build_object('"
					+ NGSIConstants.JSON_LD_ID + "',$1,'" + NGSIConstants.JSON_LD_TYPE + "','"
					+ NGSIConstants.NGSI_LD_ATTRIBUTE + "','" + NGSIConstants.NGSI_LD_ATTRIBUTE_NAME
					+ "',jsonb_build_object('" + NGSIConstants.JSON_LD_ID + "',$1),'" + NGSIConstants.NGSI_LD_TYPE_NAMES
					+ "',mytype,'" + NGSIConstants.NGSI_LD_ATTRIBUTE_COUNT + "',finalcount,'"
					+ NGSIConstants.NGSI_LD_ATTRIBUTE_TYPES + "',mydata) from y,r;";
			logger.debug("SQL Query: " + query);
			return conn.preparedQuery(query).execute(Tuple.of(qp.getAttrs()));

		}
		return Uni.createFrom().failure(new ResponseException(ErrorType.InternalError, "Unknown operation"));
	}

	@Override
	public String getAllIdsQuery() {
		return "SELECT id FROM entity";
	}

	@Override
	public String getEntryQuery() {
		return "SELECT data FROM entity WHERE id = $1";
	}

}
