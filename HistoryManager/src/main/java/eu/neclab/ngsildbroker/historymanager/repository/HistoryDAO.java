package eu.neclab.ngsildbroker.historymanager.repository;

import java.util.Map;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.StorageFunctionsInterface;
import eu.neclab.ngsildbroker.commons.storage.ClientManager;
import eu.neclab.ngsildbroker.commons.storage.StorageDAO;
import eu.neclab.ngsildbroker.commons.storage.TemporalStorageFunctions;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.sqlclient.Tuple;

@Singleton
public class HistoryDAO extends StorageDAO {

	@Inject
	ClientManager clientManager;

	@Override
	protected StorageFunctionsInterface getStorageFunctions() {
		return new TemporalStorageFunctions();
	}

	public Uni<Object> getTemporalEntity(String entityId, String tenantId) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {
			return client.preparedQuery("SELECT DISTINCT id FROM temporalentity WHERE id = $1").execute(Tuple.of(entityId)).onItem()
					.transformToUni(t -> {
						if (t.rowCount() == 0) {
							return Uni.createFrom()
									.failure(new ResponseException(ErrorType.NotFound, entityId + " was not found"));
						}
						return Uni.createFrom().item(t.iterator().next().getString(0));
					});
		});
	}
	
}