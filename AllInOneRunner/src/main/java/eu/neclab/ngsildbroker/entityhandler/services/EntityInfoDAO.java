package eu.neclab.ngsildbroker.entityhandler.services;

import java.util.Map.Entry;

import javax.enterprise.context.ApplicationScoped;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.StorageFunctionsInterface;
import eu.neclab.ngsildbroker.commons.storage.EntityStorageFunctions;
import eu.neclab.ngsildbroker.commons.storage.StorageDAO;
import io.vertx.mutiny.pgclient.PgPool;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.Tuple;

@ApplicationScoped
public class EntityInfoDAO extends StorageDAO {

	public ArrayListMultimap<String, String> getAllIds() throws ResponseException {
		ArrayListMultimap<String, String> result = ArrayListMultimap.create();
		for (Entry<String, PgPool> entry : this.tenant2Client.entrySet()) {
			PgPool client = entry.getValue();
			String tenant = entry.getKey();
			client.query("SELECT DISTINCT id FROM entity").executeAndAwait().forEach(t -> {
				result.put(tenant, t.getString(0));
			});
		}
		return result;
	}

	public String getEntity(String entityId, String tenantId) throws ResponseException {
		String result = null;
		RowSet<Row> rowSet = this.tenant2Client.get(tenantId).preparedQuery("SELECT data FROM entity WHERE id=$1")
				.executeAndAwait(Tuple.of(entityId));
		for (Row entry : rowSet) {
			result = entry.getString(0);
		}
		return result;
	}

	@Override
	protected StorageFunctionsInterface getStorageFunctions() {
		return new EntityStorageFunctions();
	}
}
