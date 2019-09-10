package eu.neclab.ngsildbroker.commons.constants;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class DBConstants {
	
	public final static String DBTABLE_ENTITY = "entity";
	public final static String DBTABLE_CSOURCE = "csource";
	public final static String DBTABLE_CSOURCE_INFO = "csourceinformation";
	public final static String DBTABLE_TEMPORALENTITY = "temporalentity";
	public final static String DBTABLE_TEMPORALENTITY_ATTRIBUTEINSTANCE= "temporalentityattrinstance";	
	
	public final static String DBCOLUMN_DATA = "data";
	public final static String DBCOLUMN_KVDATA = "kvdata";
	public final static String DBCOLUMN_DATA_WITHOUT_SYSATTRS = "data_without_sysattrs";
	public final static String DBCOLUMN_ID = "id";
	public final static String DBCOLUMN_TYPE = "type";
	public final static String DBCOLUMN_CREATED_AT = "createdat";
	public final static String DBCOLUMN_MODIFIED_AT = "modifiedat";
	public final static String DBCOLUMN_OBSERVED_AT = "observedat";
	public final static String DBCOLUMN_LOCATION = "location";
	public final static String DBCOLUMN_OBSERVATION_SPACE = "observationspace";
	public final static String DBCOLUMN_OPERATION_SPACE = "operationspace";

	public final static Map<String, String> NGSILD_TO_SQL_RESERVED_PROPERTIES_MAPPING = initNgsildToSqlReservedPropertiesMapping();

	public static Map<String, String> initNgsildToSqlReservedPropertiesMapping() {
		Map<String, String> map = new HashMap<>();
		map.put(NGSIConstants.JSON_LD_ID, DBCOLUMN_ID);
		map.put(NGSIConstants.JSON_LD_TYPE, DBCOLUMN_TYPE);
		map.put(NGSIConstants.NGSI_LD_CREATED_AT, DBCOLUMN_CREATED_AT);
		map.put(NGSIConstants.NGSI_LD_MODIFIED_AT, DBCOLUMN_MODIFIED_AT);
		// the type conversion (from geometry to geojson text) changes the format (i.e. 
		// remove spaces in geojson), so it is better to use the original data
//		map.put(NGSIConstants.NGSI_LD_LOCATION, DBCOLUMN_LOCATION);
//		map.put(NGSIConstants.NGSI_LD_OBSERVATION_SPACE, DBCOLUMN_OBSERVATION_SPACE);
//		map.put(NGSIConstants.NGSI_LD_OPERATION_SPACE, DBCOLUMN_OPERATION_SPACE);
		return Collections.unmodifiableMap(map);
	}

	public final static Map<String, String> NGSILD_TO_SQL_RESERVED_PROPERTIES_MAPPING_GEO = initNgsildToSqlReservedPropertiesMappingGeo();

	public static Map<String, String> initNgsildToSqlReservedPropertiesMappingGeo() {
		Map<String, String> map = new HashMap<>();
		map.put(NGSIConstants.NGSI_LD_LOCATION, DBCOLUMN_LOCATION);
		map.put(NGSIConstants.NGSI_LD_OBSERVATION_SPACE, DBCOLUMN_OBSERVATION_SPACE);
		map.put(NGSIConstants.NGSI_LD_OPERATION_SPACE, DBCOLUMN_OPERATION_SPACE);
		return Collections.unmodifiableMap(map);
	}

	public final static String SQLQUERY_EQUAL = "=";
	public final static String SQLQUERY_UNEQUAL = "<>";
	public final static String SQLQUERY_GREATEREQ = ">=";
	public final static String SQLQUERY_GREATER = ">";
	public final static String SQLQUERY_LESSEQ = "<=";
	public final static String SQLQUERY_LESS = "<";

	public final static Map<String, String> NGSILD_TO_SQL_OPERATORS_MAPPING = initNgsildToSqlOperatorsMapping();

	public static Map<String, String> initNgsildToSqlOperatorsMapping() {
		Map<String, String> map = new HashMap<>();
		map.put(NGSIConstants.QUERY_EQUAL, SQLQUERY_EQUAL);
		map.put(NGSIConstants.QUERY_UNEQUAL, SQLQUERY_UNEQUAL);
		map.put(NGSIConstants.QUERY_GREATEREQ, SQLQUERY_GREATEREQ);
		map.put(NGSIConstants.QUERY_GREATER, SQLQUERY_GREATER);
		map.put(NGSIConstants.QUERY_LESSEQ, SQLQUERY_LESSEQ);
		map.put(NGSIConstants.QUERY_LESS, SQLQUERY_LESS);
		return Collections.unmodifiableMap(map);
	}

	public final static String POSTGIS_NEAR = "ST_DWithin";
	public final static String POSTGIS_WITHIN = "ST_Within";
	public final static String POSTGIS_CONTAINS = "ST_Contains";
	public final static String POSTGIS_OVERLAPS = "ST_Overlaps";
	public final static String POSTGIS_INTERSECTS = "ST_Intersects";
	public final static String POSTGIS_EQUALS = "ST_Equals";
	public final static String POSTGIS_DISJOINT = "ST_Disjoint";

	public final static Map<String, String> NGSILD_TO_POSTGIS_GEO_OPERATORS_MAPPING = initNgsildToPostgisGeoOperatorsMapping();

	public static Map<String, String> initNgsildToPostgisGeoOperatorsMapping() {
		Map<String, String> map = new HashMap<>();
		map.put(NGSIConstants.GEO_REL_NEAR, POSTGIS_NEAR);
		map.put(NGSIConstants.GEO_REL_WITHIN, POSTGIS_WITHIN);
		map.put(NGSIConstants.GEO_REL_CONTAINS, POSTGIS_CONTAINS);
		map.put(NGSIConstants.GEO_REL_OVERLAPS, POSTGIS_OVERLAPS);
		map.put(NGSIConstants.GEO_REL_INTERSECTS, POSTGIS_INTERSECTS);
		map.put(NGSIConstants.GEO_REL_EQUALS, POSTGIS_EQUALS);
		map.put(NGSIConstants.GEO_REL_DISJOINT, POSTGIS_DISJOINT);
		return Collections.unmodifiableMap(map);
	}

}
