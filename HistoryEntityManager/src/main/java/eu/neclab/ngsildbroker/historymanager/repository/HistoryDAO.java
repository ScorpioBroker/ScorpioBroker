package eu.neclab.ngsildbroker.historymanager.repository;

import javax.inject.Inject;
import javax.inject.Singleton;

import eu.neclab.ngsildbroker.commons.datatypes.requests.AppendHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CreateHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteAttrHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteAttrInstanceHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.UpdateAttrHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.storage.ClientManager;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.Tuple;

@Singleton
public class HistoryDAO {

	@Inject
	ClientManager clientManager;

	public Uni<RowSet<Row>> createHistoryEntity(CreateHistoryEntityRequest request) {
		return clientManager.getClient(request.getTenant(), true).onItem().transformToUni(client -> {
			String sql = "SELECT * FROM NGSILD_CREATETEMPORALENTITY($1::jsonb)";
			return client.preparedQuery(sql).execute(Tuple.of(new JsonObject(request.getPayload()))).onFailure().retry()
					.atMost(3).onFailure().recoverWithUni(e -> Uni.createFrom().failure(e));
		});
	}

	public Uni<RowSet<Row>> deleteHistoryEntity(DeleteHistoryEntityRequest request) {
		return clientManager.getClient(request.getTenant(), true).onItem().transformToUni(client -> {
			String sql = "SELECT * FROM NGSILD_DELETETEMPORALENTITY($1)";
			return client.preparedQuery(sql).execute(Tuple.of(request.getId())).onFailure().retry().atMost(3)
					.onFailure().recoverWithUni(e -> Uni.createFrom().failure(e));
		});

	}

	public Uni<RowSet<Row>> appendToHistoryEntity(AppendHistoryEntityRequest request) {
		return clientManager.getClient(request.getTenant(), true).onItem().transformToUni(client -> {
			String sql = "SELECT * FROM NGSILD_APPENDTOTEMPORALENTITY($1, $2)";
			return client.preparedQuery(sql).execute(Tuple.of(request.getId(), new JsonObject(request.getPayload())))
					.onFailure().retry().atMost(3).onFailure().recoverWithUni(e -> Uni.createFrom().failure(e));
		});
	}

	public Uni<RowSet<Row>> updateAttrInstanceInHistoryEntity(UpdateAttrHistoryEntityRequest request) {
		return clientManager.getClient(request.getTenant(), true).onItem().transformToUni(client -> {
			String sql = "SELECT * FROM NGSILD_UPDATETEMPORALATTRINSTANCE($1, $2, $3, $4)";
			return client.preparedQuery(sql)
					.execute(Tuple.of(request.getId(), request.getAttrId(), request.getInstanceId(),
							new JsonObject(request.getPayload())))
					.onFailure().retry().atMost(3).onFailure().recoverWithUni(e -> Uni.createFrom().failure(e));
		});
	}

	public Uni<RowSet<Row>> deleteAttrFromHistoryEntity(DeleteAttrHistoryEntityRequest request) {
		return clientManager.getClient(request.getTenant(), true).onItem().transformToUni(client -> {
			String sql = "SELECT * FROM NGSILD_DELETEATTRFROMTEMPENTITY($1, $2, $3, $4)";
			return client.preparedQuery(sql)
					.execute(Tuple.of(request.getId(), request.getAttribName(), request.getDatasetId(),
							request.isDeleteAll()))
					.onFailure().retry().atMost(3).onFailure().recoverWithUni(e -> Uni.createFrom().failure(e));
		});
	}

	public Uni<RowSet<Row>> deleteAttrInstanceInHistoryEntity(DeleteAttrInstanceHistoryEntityRequest request) {
		return clientManager.getClient(request.getTenant(), true).onItem().transformToUni(client -> {
			String sql = "SELECT * FROM NGSILD_DELETEATTRFROMTEMPENTITY($1, $2, $3, $4)";
			return client.preparedQuery(sql)
					.execute(Tuple.of(request.getId(), request.getAttrId(), request.getTenant())).onFailure().retry()
					.atMost(3).onFailure().recoverWithUni(e -> Uni.createFrom().failure(e));
		});
	}

}