package eu.neclab.ngsildbroker.registryhandler.repository;

import java.util.Map;

import javax.inject.Inject;
import javax.inject.Singleton;

import eu.neclab.ngsildbroker.commons.storage.ClientManager;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.Tuple;

@Singleton
public class CSourceDAO {

	@Inject
	ClientManager clientManager;

	public Uni<RowSet<Row>> getRegistrationById(String tenant, String id) {
		return clientManager.getClient(tenant, false).onItem().transformToUni(client -> {
			return client.preparedQuery("SELECT data FROM csource WHERE id = $1").execute(Tuple.of(id)).onFailure()
					.retry().atMost(3);

		});

	}

	public Uni<RowSet<Row>> createRegistration(String tenant, Map<String, Object> registration) {
		return clientManager.getClient(tenant, true).onItem().transformToUni(client -> {
			return client.preparedQuery("INSERT INTO csource(reg) VALUES ($1)")
					.execute(Tuple.of(new JsonObject(registration))).onFailure().retry().atMost(3);
		});
	}

	public Uni<RowSet<Row>> updateRegistration(String tenant, String registrationId, Map<String, Object> entry) {
		return clientManager.getClient(tenant, true).onItem().transformToUni(client -> {
			return client.preparedQuery("UPDATE csource SET reg=$1 where c_id=$2")
					.execute(Tuple.of(new JsonObject(entry), registrationId)).onFailure().retry().atMost(3);
		});
	}

	public Uni<RowSet<Row>> deleteRegistration(String tenant, String registrationId) {
		// TODO Auto-generated method stub
		return null;
	}

}
