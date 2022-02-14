package eu.neclab.ngsildbroker.historymanager.repository;

import java.util.Map.Entry;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.StorageFunctionsInterface;
import eu.neclab.ngsildbroker.commons.storage.StorageDAO;
import eu.neclab.ngsildbroker.commons.storage.TemporalStorageFunctions;
import io.vertx.mutiny.pgclient.PgPool;

public class HistoryDAO extends StorageDAO {

	@Override
	protected StorageFunctionsInterface getStorageFunctions() {
		return new TemporalStorageFunctions();
	}

	public ArrayListMultimap<String, String> getAllIds() throws ResponseException {
		ArrayListMultimap<String, String> result = ArrayListMultimap.create();
		for (Entry<String, PgPool> entry : this.tenant2Client.entrySet()) {
			String key = entry.getKey();
			entry.getValue().query("SELECT DISTINCT id FROM temporalentity").executeAndAwait().forEach(t -> {
				result.put(key, t.getString(0));
			});

		}
		return result;
	}

}