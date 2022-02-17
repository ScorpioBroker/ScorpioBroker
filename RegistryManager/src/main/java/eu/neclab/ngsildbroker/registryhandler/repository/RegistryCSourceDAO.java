package eu.neclab.ngsildbroker.registryhandler.repository;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;

import eu.neclab.ngsildbroker.commons.constants.DBConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.StorageFunctionsInterface;
import eu.neclab.ngsildbroker.commons.storage.RegistryStorageFunctions;
import eu.neclab.ngsildbroker.commons.storage.StorageDAO;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.pgclient.PgPool;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.Tuple;

@Singleton
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
		for (Entry<String, PgPool> entry : clientManager.getAllClients().entrySet()) {
			String key = entry.getKey();
			entry.getValue().query("SELECT DISTINCT id FROM csource").executeAndAwait().forEach(t -> {
				result.put(key, t.getString(0));
			});

		}
		return result;
	}

	public String getEntity(String tenantId, String entityId) throws ResponseException {
		String result = null;
		RowSet<Row> rowSet = clientManager.getClient(tenantId, false)
				.preparedQuery("SELECT data FROM csource WHERE id=$1").executeAndAwait(Tuple.of(entityId));
		for (Row entry : rowSet) {
			result = ((JsonObject) entry.getJson(0)).encode();
		}
		return result;
	}

	public List<String> getAllRegistrations(String tenant) throws ResponseException {
		List<String> result = Lists.newArrayList();
		clientManager.getClient(tenant, false).query("SELECT data FROM csource").executeAndAwait().forEach(t -> {
			result.add(t.getString(0));
		});
		return result;
	}

	@Override
	protected StorageFunctionsInterface getStorageFunctions() {
		return new RegistryStorageFunctions();
	}

	public Map<String, List<String>> getAllEntities() {
		HashMap<String, List<String>> result = new HashMap<String, List<String>>();
		for (Entry<String, PgPool> entry : clientManager.getAllClients().entrySet()) {
			String tenant = entry.getKey();
			PgPool client = entry.getValue();
			List<String> temp = Lists.newArrayList();
			client.query("SELECT data FROM ENTITY").executeAndAwait().forEach(t -> {
				temp.add(((JsonObject) t.getJson(0)).encode());
			});
			result.put(tenant, temp);
		}
		return result;
	}

}
