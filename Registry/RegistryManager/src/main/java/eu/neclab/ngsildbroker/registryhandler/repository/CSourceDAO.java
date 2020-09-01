package eu.neclab.ngsildbroker.registryhandler.repository;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Repository;

import eu.neclab.ngsildbroker.commons.constants.DBConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.GeoqueryRel;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.storage.StorageReaderDAO;

@Repository("rmcsourcedao")
public class CSourceDAO extends StorageReaderDAO {

	private final static Logger logger = LogManager.getLogger(CSourceDAO.class);

	protected final static String DBCOLUMN_CSOURCE_INFO_ENTITY_ID = "entity_id";
	protected final static String DBCOLUMN_CSOURCE_INFO_ENTITY_IDPATTERN = "entity_idpattern";
	protected final static String DBCOLUMN_CSOURCE_INFO_ENTITY_TYPE = "entity_type";
	protected final static String DBCOLUMN_CSOURCE_INFO_PROPERTY_ID = "property_id";
	protected final static String DBCOLUMN_CSOURCE_INFO_RELATIONSHIP_ID = "relationship_id";
	
	protected final static Map<String, String> NGSILD_TO_SQL_RESERVED_PROPERTIES_MAPPING_GEO = initNgsildToSqlReservedPropertiesMappingGeo();

	protected static Map<String, String> initNgsildToSqlReservedPropertiesMappingGeo() {
		Map<String, String> map = new HashMap<>();
		map.put(NGSIConstants.NGSI_LD_LOCATION, DBConstants.DBCOLUMN_LOCATION);
		return Collections.unmodifiableMap(map);
	}

	protected final static Map<String, String> NGSILD_TO_POSTGIS_GEO_OPERATORS_MAPPING = initNgsildToPostgisGeoOperatorsMapping();

	protected static Map<String, String> initNgsildToPostgisGeoOperatorsMapping() {
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

	private boolean externalCsourcesOnly = false; 
	
	@Override
	public List<String> query(QueryParams qp) {
		this.externalCsourcesOnly = false;
		return super.query(qp);
	}
	
	public List<String> queryExternalCsources(QueryParams qp) throws SQLException {
		this.externalCsourcesOnly = true;
		return super.query(qp);
	}

	@Override
	protected String translateNgsildQueryToSql(QueryParams qp) throws ResponseException {
		StringBuilder fullSqlWhere = new StringBuilder(70);
		String sqlWhere = "";
		boolean csourceInformationIsNeeded = false;
		boolean sqlOk = false;

		if (externalCsourcesOnly) {
			fullSqlWhere.append("(c.internal = false) AND ");
		}
		
		// query by type + (id, idPattern)
		if (qp.getType()!=null) {
			
			String typeValue = qp.getType();
			String idValue = "";
			String idPatternValue = "";
			if (qp.getId()!=null)
				idValue = qp.getId();
			if (qp.getIdPattern()!=null)
				idPatternValue = qp.getIdPattern();
			// id takes precedence on idPattern. clear idPattern if both are given
			if (!idValue.isEmpty() && !idPatternValue.isEmpty())
				idPatternValue = "";

			// query by type + (id, idPattern) + attrs
			if (qp.getAttrs()!=null) {
				String attrsValue = qp.getAttrs();
				sqlWhere = getCommonSqlWhereForTypeIdIdPattern(typeValue, idValue, idPatternValue);
				sqlWhere += " AND ";
				sqlWhere += getSqlWhereByAttrsInTypeFiltering(attrsValue);
				
			} else {  // query by type + (id, idPattern) only (no attrs)
				
				sqlWhere = "(c.has_registrationinfo_with_attrs_only) OR ";	
				sqlWhere += getCommonSqlWhereForTypeIdIdPattern(typeValue, idValue, idPatternValue);
				
			}
			fullSqlWhere.append("(" + sqlWhere + ") AND ");
			csourceInformationIsNeeded = true;
			sqlOk = true;
			
		// query by attrs only		
		} else if (qp.getAttrs()!=null) {
			String attrsValue = qp.getAttrs();
			if (attrsValue.indexOf(",") == -1) {
				sqlWhere = "ci." + DBCOLUMN_CSOURCE_INFO_PROPERTY_ID+" = '"+attrsValue+"' OR "
						+"ci." + DBCOLUMN_CSOURCE_INFO_RELATIONSHIP_ID+" = '"+attrsValue+"'";
			}else {
				sqlWhere="ci." + DBCOLUMN_CSOURCE_INFO_PROPERTY_ID+" IN ('"+attrsValue.replace(",", "','")+"') OR "
						+"ci." + DBCOLUMN_CSOURCE_INFO_RELATIONSHIP_ID+" IN ('"+attrsValue.replace(",", "','")+"')";
			}
			fullSqlWhere.append("(" + sqlWhere + ") AND ");
			csourceInformationIsNeeded = true;
			sqlOk = true;
		}

		// advanced query "q"
		if (qp.getQ()!=null) {
			// TODO: it's not clear in spec how this should work
			logger.error("'q' filter has not been developed yet in csource discovery!");
			return "";
		}

		// geoquery
		if (qp.getGeorel()!=null) {
			GeoqueryRel gqr = qp.getGeorel();
			logger.debug("Georel value " + gqr.getGeorelOp());
			try {
				sqlWhere = translateNgsildGeoqueryToPostgisQuery(gqr, qp.getGeometry(), qp.getCoordinates(),
						qp.getGeoproperty());
			} catch (ResponseException e) {
				e.printStackTrace();
			}
			fullSqlWhere.append(sqlWhere + " AND ");
			sqlOk = true;
		}

		if (sqlOk) {
			String sqlQuery = "SELECT DISTINCT c.data " + "FROM " + DBConstants.DBTABLE_CSOURCE + " c ";
			if (csourceInformationIsNeeded)
				sqlQuery += "INNER JOIN " + DBConstants.DBTABLE_CSOURCE_INFO + " ci ON (ci.csource_id = c.id) ";
	
			if (fullSqlWhere.length() > 0) {
				sqlQuery += "WHERE " + fullSqlWhere.toString() + " 1=1 ";
			}
			// order by ?
			return sqlQuery;
		} else {
			return "";			
		}
	}

	private String getCommonSqlWhereForTypeIdIdPattern(String typeValue, String idValue, String idPatternValue) {
		String sqlWhere = "";
		if (idValue.isEmpty() && idPatternValue.isEmpty()) { // case 1: type only
			sqlWhere += getSqlWhereByType(typeValue, false);
		} else if (!idValue.isEmpty() && idPatternValue.isEmpty()) { // case 2: type+id
			sqlWhere += "(";				
			sqlWhere += getSqlWhereByType(typeValue, true);
			sqlWhere += " OR ";
			sqlWhere += getSqlWhereById(typeValue, idValue);
			sqlWhere += ")";
		} else if (idValue.isEmpty() && !idPatternValue.isEmpty()) { // case 3: type+idPattern
			sqlWhere += "(";				
			sqlWhere += getSqlWhereByType(typeValue, true);
			sqlWhere += " OR ";
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
			sqlWhere += "AND ci." + DBCOLUMN_CSOURCE_INFO_ENTITY_ID + " IS NULL AND "
					  + "ci." + DBCOLUMN_CSOURCE_INFO_ENTITY_IDPATTERN + " IS NULL";
		sqlWhere += ")";
		return sqlWhere;
	}
	
	private String getSqlWhereById(String typeValue, String idValue) {
		String sqlWhere = "( ";
	
		if (typeValue.indexOf(",") == -1) {
			sqlWhere += "ci." + DBCOLUMN_CSOURCE_INFO_ENTITY_TYPE + " = '" + typeValue + "' AND ";
		} else {
			sqlWhere += "ci." + DBCOLUMN_CSOURCE_INFO_ENTITY_TYPE + " IN ('" + typeValue.replace(",", "','") + "') AND ";
		}
		
		if (idValue.indexOf(",") == -1) {
			sqlWhere += "(" + "ci." + DBCOLUMN_CSOURCE_INFO_ENTITY_ID + " = '" + idValue + "' OR " + "'"
					+ idValue + "' ~ " + "ci." + DBCOLUMN_CSOURCE_INFO_ENTITY_IDPATTERN + ")";
		} else {			
			String[] ids = idValue.split(",");
			String whereId = "ci." + DBCOLUMN_CSOURCE_INFO_ENTITY_ID + " IN ( ";
			String whereIdPattern = "(";
			for (String id : ids) {
				whereId += "'" + id + "',";
				whereIdPattern += "ci." + DBCOLUMN_CSOURCE_INFO_ENTITY_IDPATTERN + " ~ '" + id
						+ "' OR ";
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
			sqlWhere += "ci." + DBCOLUMN_CSOURCE_INFO_ENTITY_TYPE + " IN ('" + typeValue.replace(",", "','") + "') AND ";
		}
		sqlWhere += "(" + "ci." + DBCOLUMN_CSOURCE_INFO_ENTITY_ID + " ~ '" + idPatternValue + "' OR "
				+ "ci." +  DBCOLUMN_CSOURCE_INFO_ENTITY_IDPATTERN + " ~ '" + idPatternValue + "')";					
		sqlWhere += " )";		
		return sqlWhere;
	}	
	
	private String getSqlWhereByAttrsInTypeFiltering(String attrsValue) {
		String sqlWhere;		
		sqlWhere = "( " + 
				"NOT EXISTS (SELECT 1 FROM csourceinformation ci2 " + 
				"	          WHERE ci2.group_id = ci.group_id AND " + 
				"	                (ci2.property_id IS NOT NULL OR ci2.relationship_id IS NOT NULL)) " + 
				"OR " + 
				"EXISTS (SELECT 1 FROM csourceinformation ci3 " + 
				"        WHERE ci3.group_id = ci.group_id AND " ;	
		if (attrsValue.indexOf(",") == -1) {
			sqlWhere +=	"(ci3.property_id = '" + attrsValue + "' OR " + 
						" ci3.relationship_id = '" + attrsValue + "') ";
		} else {
			sqlWhere +=	"(ci3.property_id IN ('" + attrsValue.replace(",", "','") + "') OR " + 
						" ci3.relationship_id IN ('" + attrsValue.replace(",", "','") + "') ) ";			
		}		
		sqlWhere += ") )"; 		
		return sqlWhere;
	}
	
	// TODO: SQL input sanitization
	// TODO: property of property
	// TODO: [SPEC] spec is not clear on how to define a "property of property" in
	// the geoproperty field. (probably using dots, but...)
	@Override
	protected String translateNgsildGeoqueryToPostgisQuery(GeoqueryRel georel, String geometry, String coordinates,
			String geoproperty) throws ResponseException {
		if (georel.getGeorelOp().isEmpty() || geometry==null || coordinates==null || geometry.isEmpty() || coordinates.isEmpty()) {
			logger.error("georel, geometry and coordinates are empty or invalid!");
			throw new ResponseException(ErrorType.BadRequestData,
					"georel, geometry and coordinates are empty or invalid!");
		}

		StringBuilder sqlWhere = new StringBuilder(50);

		String georelOp = georel.getGeorelOp();
		logger.debug("  Geoquery term georelOp: " + georelOp);

		String dbColumn = NGSILD_TO_SQL_RESERVED_PROPERTIES_MAPPING_GEO.get(geoproperty);
		if (dbColumn == null) {
			dbColumn = "ST_SetSRID(ST_GeomFromGeoJSON( c.data#>>'{" + geoproperty + ",0,"
					+ NGSIConstants.JSON_LD_VALUE + "}'), 4326)";
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
			if (georel.getDistanceType()!=null && georel.getDistanceValue()!=null) {
				if (georel.getDistanceType().equals(NGSIConstants.GEO_REL_MIN_DISTANCE))
					sqlWhere.append("NOT " + DBConstants.POSTGIS_WITHIN + "( " + dbColumn + ", ST_Buffer(" + referenceValue
							+ "::geography, " + georel.getDistanceValue()
							+ ")::geometry ) ");
				else
					sqlWhere.append(DBConstants.POSTGIS_INTERSECTS + "( " + dbColumn + ", ST_Buffer(" + referenceValue
							+ "::geography, " + georel.getDistanceValue()
							+ ")::geometry ) ");
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

}
