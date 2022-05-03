package eu.neclab.ngsildbroker.entityhandler.services;

import java.util.Map.Entry;
import javax.inject.Inject;
import javax.inject.Singleton;
import com.google.common.collect.ArrayListMultimap;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.StorageFunctionsInterface;
import eu.neclab.ngsildbroker.commons.storage.ClientManager;
import eu.neclab.ngsildbroker.commons.storage.EntityStorageFunctions;
import eu.neclab.ngsildbroker.commons.storage.StorageDAO;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.pgclient.PgPool;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.Tuple;

@Singleton
public class EntityInfoDAO extends StorageDAO {
	@Inject
	ClientManager clientManager;

	public ArrayListMultimap<String, String> getAllIds() throws ResponseException {
		ArrayListMultimap<String, String> result = ArrayListMultimap.create();
		for (Entry<String, PgPool> entry : clientManager.getAllClients().entrySet()) {
			PgPool client = entry.getValue();
			String tenant = entry.getKey();
			client.query("SELECT DISTINCT id FROM entity").executeAndAwait().forEach(t -> {
				result.put(tenant, t.getString(0));
			});
		}
		return result;
	}

	public Uni<String> getEntity(String entityId, String tenantId) {

		return clientManager.getClient(tenantId, false).preparedQuery("SELECT data FROM entity WHERE id=$1")
				.execute(Tuple.of(entityId)).onItem().transform(t -> {
					String result = "";
					for (Row entry : t) {
						result = ((JsonObject) entry.getJson(0)).encode();
					}
					return result;
				});

	}

	@Override
	protected StorageFunctionsInterface getStorageFunctions() {
		return new EntityStorageFunctions();
	}
}
