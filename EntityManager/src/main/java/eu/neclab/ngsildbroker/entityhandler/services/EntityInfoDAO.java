package eu.neclab.ngsildbroker.entityhandler.services;

import javax.inject.Singleton;

import eu.neclab.ngsildbroker.commons.datatypes.requests.AppendEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CreateEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteAttributeRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.UpdateEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.results.NGSILDOperationResult;
import eu.neclab.ngsildbroker.commons.interfaces.StorageFunctionsInterface;
import eu.neclab.ngsildbroker.commons.storage.EntityStorageFunctions;
import eu.neclab.ngsildbroker.commons.storage.StorageDAO;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.Tuple;

@Singleton
public class EntityInfoDAO extends StorageDAO {

	public Uni<RowSet<Row>> createEntity(CreateEntityRequest request) {
		return clientManager.getClient(request.getTenant(), true).onItem().transformToUni(client -> {
			String sql = "SELECT * FROM NGSILD_CREATEENTITY($1::jsonb)";
			return client.preparedQuery(sql).execute(Tuple.of(new JsonObject(request.getPayload()))).onFailure().retry()
					.atMost(3).onFailure().recoverWithUni(e -> Uni.createFrom().failure(e));
		});

	}

	public Uni<RowSet<Row>> updateEntity(UpdateEntityRequest request) {
		return clientManager.getClient(request.getTenant(), false).onItem().transformToUni(client -> {
			String sql = "SELECT * FROM NGSILD_UPDATEENTITY($1, $2::jsonb)";
			return client.preparedQuery(sql).execute(Tuple.of(request.getId(), new JsonObject(request.getPayload())))
					.onFailure().retry().atMost(3).onFailure().recoverWithUni(e -> Uni.createFrom().failure(e));
		});
	}

	public Uni<RowSet<Row>> appendEntity(AppendEntityRequest request, boolean noOverwrite) {
		return clientManager.getClient(request.getTenant(), false).onItem().transformToUni(client -> {
			String sql = "SELECT * FROM NGSILD_APPENDENTITY($1, $2::jsonb, $3)";
			return client.preparedQuery(sql)
					.execute(Tuple.of(request.getId(), new JsonObject(request.getPayload()), noOverwrite)).onFailure()
					.retry().atMost(3).onFailure().recoverWithUni(e -> Uni.createFrom().failure(e));
		});
	}

	public Uni<RowSet<Row>> partialUpdateEntity(UpdateEntityRequest request) {
		return clientManager.getClient(request.getTenant(), false).onItem().transformToUni(client -> {
			String sql = "SELECT * FROM NGSILD_PARTIALUPDATEENTITY($1, $2::jsonb)";
			return client.preparedQuery(sql).execute(Tuple.of(request.getId(), new JsonObject(request.getPayload())))
					.onFailure().retry().atMost(3).onFailure().recoverWithUni(e -> Uni.createFrom().failure(e));
		});
	}

	public Uni<RowSet<Row>> deleteAttribute(DeleteAttributeRequest request) {
		return clientManager.getClient(request.getTenant(), false).onItem().transformToUni(client -> {
			String sql = "SELECT * FROM NGSILD_DELETEATTR($1, $2, $3, $4)";
			return client.preparedQuery(sql)
					.execute(Tuple.of(request.getId(), request.getAttribName(), request.getDatasetId(),
							request.deleteAll()))
					.onFailure().retry().atMost(3).onFailure().recoverWithUni(e -> Uni.createFrom().failure(e));
		});
	}

	public Uni<RowSet<Row>> deleteEntity(DeleteEntityRequest request) {
		return clientManager.getClient(request.getTenant(), false).onItem().transformToUni(client -> {
			String sql = "SELECT * FROM NGSILD_DELETEENTITY($1)";
			return client.preparedQuery(sql).execute(Tuple.of(request.getId())).onFailure().retry().atMost(3)
					.onFailure().recoverWithUni(e -> Uni.createFrom().failure(e));
		});

	}

	public Uni<RowSet<Row>> upsertEntity(CreateEntityRequest request) {
		return clientManager.getClient(request.getTenant(), true).onItem().transformToUni(client -> {
			String sql = "SELECT * FROM NGSILD_UPSERTENTITY($1::jsonb)";
			return client.preparedQuery(sql).execute(Tuple.of(new JsonObject(request.getPayload()))).onFailure().retry()
					.atMost(3).onFailure().recoverWithUni(e -> Uni.createFrom().failure(e));
		});
	}

}
