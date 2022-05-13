package eu.neclab.ngsildbroker.historymanager.repository;

import java.util.Map.Entry;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.StorageFunctionsInterface;
import eu.neclab.ngsildbroker.commons.storage.ClientManager;
import eu.neclab.ngsildbroker.commons.storage.StorageDAO;
import eu.neclab.ngsildbroker.commons.storage.TemporalStorageFunctions;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.pgclient.PgPool;

@Singleton
public class HistoryDAO extends StorageDAO {

	@Inject
	ClientManager clientManager;

	@Override
	protected StorageFunctionsInterface getStorageFunctions() {
		return new TemporalStorageFunctions();
	}

	public Uni<Void> entityExists(String entityId, String tenantId) throws ResponseException {
		ArrayListMultimap<String, String> result = ArrayListMultimap.create();
		if (tenantId == AppConstants.INTERNAL_NULL_KEY) {
			clientManager.getClient(null, false).query("SELECT DISTINCT id FROM temporalentity").executeAndAwait()
					.forEach(t -> {
						result.put(tenantId, t.getString(0));
					});
		} else {
			clientManager.getClient(tenantId, false).query("SELECT DISTINCT id FROM temporalentity").executeAndAwait()
					.forEach(t -> {
						result.put(tenantId, t.getString(0));
					});
		}
		if (result.containsValue(entityId)) {
			throw new ResponseException(ErrorType.AlreadyExists, entityId + " already exists");
		}
		return Uni.createFrom().nullItem();
	}

	public Uni<Void> getAllIds(String entityId, String tenantId) throws ResponseException {
		ArrayListMultimap<String, String> result = ArrayListMultimap.create();

		for (Entry<String, PgPool> entry : clientManager.getAllClients().entrySet()) {
			PgPool client = entry.getValue();
			String tenant = entry.getKey();
			client.query("SELECT DISTINCT id FROM temporalentity").executeAndAwait().forEach(t -> {
				result.put(tenant, t.getString(0));
			});
		}
		if (!result.containsValue(entityId)) {
			throw new ResponseException(ErrorType.NotFound, "Entity Id " + entityId + " not found");

		}
		if (!result.containsKey(tenantId)) {
			throw new ResponseException(ErrorType.TenantNotFound, "tenant " + tenantId + " not found");
		}
		return Uni.createFrom().nullItem();
	}
}