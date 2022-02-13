package eu.neclab.ngsildbroker.registryhandler.repository;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.enterprise.context.ApplicationScoped;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.DBConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.StorageFunctionsInterface;
import eu.neclab.ngsildbroker.commons.storage.RegistryStorageFunctions;
import eu.neclab.ngsildbroker.commons.storage.StorageDAO;

@ApplicationScoped
public class RegistryCSourceDAO extends StorageDAO {

	private final static Logger logger = LoggerFactory.getLogger(RegistryCSourceDAO.class);

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

	public ArrayListMultimap<String, String> getAllIds() throws ResponseException {
		ArrayListMultimap<String, String> result = ArrayListMultimap.create();
		result.putAll(AppConstants.INTERNAL_NULL_KEY,
				getJDBCTemplate(null).queryForList("SELECT DISTINCT id FROM csource", String.class));
		List<String> tenants = getTenants();
		for (String tenant : tenants) {
			result.putAll(tenant,
					getJDBCTemplate(tenant).queryForList("SELECT DISTINCT id FROM csource", String.class));
		}

		return result;
	}

	public String getEntity(String tenantId, String entityId) throws ResponseException {

		List<String> tempList = getJDBCTemplate(getTenant(tenantId))
				.queryForList("SELECT data FROM csource WHERE id='" + entityId + "'", String.class);
		return tempList.get(0);
	}

	public List<String> getAllRegistrations(String tenant) throws ResponseException {
		tenant = getTenant(tenant);
		return getJDBCTemplate(tenant).queryForList("SELECT data FROM csource", String.class);
	}

	@Override
	protected StorageFunctionsInterface getStorageFunctions() {
		return new RegistryStorageFunctions();
	}

	public Map<String, List<String>> getAllEntities() {
		List<String> tenants = getTenants();
		HashMap<String, List<String>> result = new HashMap<String, List<String>>();
		result.put(AppConstants.INTERNAL_NULL_KEY,
				getJDBCTemplate(null).queryForList("SELECT data FROM ENTITY", String.class));
		for (String tenant : tenants) {
			result.put(tenant, getJDBCTemplate(tenant).queryForList("SELECT data FROM ENTITY", String.class));
		}
		return result;
	}

}
