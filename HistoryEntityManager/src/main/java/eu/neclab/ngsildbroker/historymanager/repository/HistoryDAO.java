package eu.neclab.ngsildbroker.historymanager.repository;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.github.jsonldjava.core.Context;

import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.StorageFunctionsInterface;
import eu.neclab.ngsildbroker.commons.ngsiqueries.ParamsResolver;
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
			return client.preparedQuery("SELECT DISTINCT id FROM temporalentity WHERE id = $1")
					.execute(Tuple.of(entityId)).onItem().transformToUni(t -> {
						if (t.rowCount() == 0) {
							return Uni.createFrom()
									.failure(new ResponseException(ErrorType.NotFound, entityId + " was not found"));
						}
						return Uni.createFrom().item(t.iterator().next().getString(0));
					});
		});
	}

	public Uni<Object> temporalEntityAttrInstanceExist(String entityId, String tenantId, String attrsId,
			String instanceId, Context linkHeaders) {
		if (attrsId == null)
			return Uni.createFrom().nullItem();

		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {
			String resolvedAttrId = null;
			try {
				resolvedAttrId = ParamsResolver.expandAttribute(attrsId, linkHeaders);
			} catch (ResponseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			String query = "SELECT DISTINCT attributeid,instanceid FROM temporalentityattrinstance WHERE temporalentity_id = $1 and attributeid = $2";
			Tuple tValue = Tuple.of(entityId, resolvedAttrId);

			return client.preparedQuery(query).execute(tValue).onItem().transformToUni(t -> {
				if (t.rowCount() == 0) {
					return Uni.createFrom()
							.failure(new ResponseException(ErrorType.NotFound, attrsId + " was not found"));
				}

				if (instanceId != null) {
					List<String> tmpInstanceList = new ArrayList<>();
					t.forEach(t2 -> tmpInstanceList.add(t2.getString(1)));
					if (!tmpInstanceList.contains(instanceId)) {
						return Uni.createFrom()
								.failure(new ResponseException(ErrorType.NotFound, instanceId + " was not found"));
					}

				}
				return Uni.createFrom().item(t.iterator().next().getString(0));
			});
		});
	}

	public Uni<Object> isTempEntityExist(String entityId, String tenantId) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {
			return client.preparedQuery("SELECT DISTINCT id FROM temporalentity WHERE id = $1")
					.execute(Tuple.of(entityId)).onItem().transformToUni(t -> {
						if (t.rowCount() == 0) {
							return Uni.createFrom().item(true);
						}
						return Uni.createFrom().item(false);
					});
		});
	}

}