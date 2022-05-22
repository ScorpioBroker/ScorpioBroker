package eu.neclab.ngsildbroker.registryhandler.repository;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import eu.neclab.ngsildbroker.commons.constants.DBConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.interfaces.StorageFunctionsInterface;
import eu.neclab.ngsildbroker.commons.storage.RegistryStorageFunctions;
import eu.neclab.ngsildbroker.commons.storage.StorageDAO;


public class CSourceDAO extends StorageDAO {

	

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
	

	
	@Override
	protected StorageFunctionsInterface getStorageFunctions() {
		return new RegistryStorageFunctions();
	}

	

}
