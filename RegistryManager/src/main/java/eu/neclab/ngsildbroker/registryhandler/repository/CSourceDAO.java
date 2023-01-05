package eu.neclab.ngsildbroker.registryhandler.repository;

import javax.inject.Inject;
import javax.inject.Singleton;

import eu.neclab.ngsildbroker.commons.datatypes.requests.AppendCSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CreateCSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteCSourceRequest;
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
			return client.preparedQuery("SELECT reg FROM csource WHERE id = $1").execute(Tuple.of(id)).onFailure()
					.retry().atMost(3);

		});

	}

	public Uni<RowSet<Row>> createRegistration(CreateCSourceRequest request) {
		return clientManager.getClient(request.getTenant(), true).onItem().transformToUni(client -> {
			return client.preparedQuery("INSERT INTO csource(reg) VALUES ($1)")
					.execute(Tuple.of(new JsonObject(request.getPayload()))).onFailure().retry().atMost(3);
		});
	}

	public Uni<RowSet<Row>> updateRegistration(AppendCSourceRequest request) {
		return clientManager.getClient(request.getTenant(), true).onItem().transformToUni(client -> {
			return client.preparedQuery("UPDATE csource SET reg= reg || $1 where c_id=$2 RETURNING reg")
					.execute(Tuple.of(new JsonObject(request.getPayload()), request.getId())).onFailure().retry()
					.atMost(3);
		});
	}

	public Uni<RowSet<Row>> deleteRegistration(DeleteCSourceRequest request) {
		return clientManager.getClient(request.getTenant(), true).onItem().transformToUni(client -> {
			return client.preparedQuery("DELETE FROM csource WHERE c_id=$1 RETURNING reg")
					.execute(Tuple.of(request.getId())).onFailure().retry().atMost(3);
		});
	}

}
