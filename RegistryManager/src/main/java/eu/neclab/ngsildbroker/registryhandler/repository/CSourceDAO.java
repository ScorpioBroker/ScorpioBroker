package eu.neclab.ngsildbroker.registryhandler.repository;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;
import javax.inject.Singleton;

import eu.neclab.ngsildbroker.commons.constants.DBConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CSourceRequest;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.StorageFunctionsInterface;
import eu.neclab.ngsildbroker.commons.storage.ClientManager;
import eu.neclab.ngsildbroker.commons.storage.StorageDAO;
import eu.neclab.ngsildbroker.registryhandler.service.RegistryStorageFunctions;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.sqlclient.Tuple;
import io.vertx.pgclient.PgException;

@Singleton
public class CSourceDAO extends StorageDAO {
	
	@Inject
	ClientManager clientManager;

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

	public Uni<Map<String, Object>> getRegistrationById(String id, String tenant) {
		return clientManager.getClient(tenant, false).onItem().transformToUni(client -> {
			return client.preparedQuery("SELECT data FROM csource WHERE id = $1").execute(Tuple.of(id)).onItem()
					.transformToUni(rowSet -> {
						if (rowSet.size() == 0) {
							return Uni.createFrom()
									.failure(new ResponseException(ErrorType.NotFound , id + "was not found"));
						}
						return Uni.createFrom().item(rowSet.iterator().next().getJsonObject(0).getMap());
					});
		});

	}
	public Uni<Void> createRegistry(CSourceRequest request) {
		return clientManager.getClient(request.getTenant(), true).onItem().transformToUni(client -> {
			JsonObject value = new JsonObject(request.getPayload());
			String sql;

			sql = "INSERT INTO " + DBConstants.DBTABLE_CSOURCE + " (id, " + DBConstants.DBCOLUMN_DATA
					+ ") VALUES ($1, $2::jsonb)";

			return client.preparedQuery(sql).execute(Tuple.of(request.getId(), value)).onFailure().recoverWithUni(e -> {
				if (e instanceof PgException) {
					PgException pgE = (PgException) e;
					if (pgE.getCode().equals("23505")) { // code for unique constraint
						return Uni.createFrom().failure(
								new ResponseException(ErrorType.AlreadyExists, request.getId() + " already exists"));
					}
				}
				return Uni.createFrom().failure(e);
			}).onItem().transformToUni(t -> Uni.createFrom().voidItem());
		});
	}
}
