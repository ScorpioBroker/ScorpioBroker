package eu.neclab.ngsildbroker.queryhandler.repository;

import java.util.Map;
import java.util.Set;

import javax.inject.Singleton;

import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.StorageFunctionsInterface;
import eu.neclab.ngsildbroker.commons.storage.EntityStorageFunctions;
import eu.neclab.ngsildbroker.commons.storage.StorageDAO;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.Tuple;

@Singleton
public class QueryDAO extends StorageDAO {

	public Uni<Map<String, Object>> getEntity(String entryId, String tenantId) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {
			return client.preparedQuery("SELECT ENTITY FROM ENTITY WHERE E_ID=$1").execute(Tuple.of(entryId)).onItem()
					.transformToUni(t -> {
						if (t.rowCount() == 0) {
							return Uni.createFrom()
									.failure(new ResponseException(ErrorType.NotFound, entryId + " was not found"));
						}
						return Uni.createFrom().item(t.iterator().next().getJsonObject(0).getMap());
					});
		});

	}

	public Uni<RowSet<Row>> getRemoteSourcesForEntity(String entityId, Set<String> expandedAttrs, String tenantId) {
		return clientManager.getClient(tenantId, false).onItem().transformToUni(client -> {
			// TODO add expires to where
			return client.preparedQuery(
					"SELECT C.endpoint C.tenant_id, c.headers, c.reg_mode FROM CSOURCEINFORMATION AS C WHERE C.retrieveEntity=true AND (C.E_ID=$1 OR C.E_ID=NULL) AND (C.e_prop=NULL OR C.e_prop IN $2) AND (C.e_rel=NULL OR C.e_rel IN $2)")
					.execute(Tuple.of(entityId, expandedAttrs));
		});
	}

}
