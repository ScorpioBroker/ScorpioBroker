package eu.neclab.ngsildbroker.historyentitymanager.repository;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import javax.inject.Inject;
import javax.inject.Singleton;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.AppendHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CreateHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteAttrHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteAttrInstanceHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteAttributeRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.UpdateAttrHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.storage.ClientManager;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;
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
			String sql = "SELECT * FROM NGSILD_DELETEATTRSTNSTANCEFROMTEMPENTITY($1, $2, $3)";
			return client.preparedQuery(sql)
					.execute(Tuple.of(request.getId(), request.getAttrId(), request.getInstanceId())).onFailure()
					.retry().atMost(3).onFailure().recoverWithUni(e -> Uni.createFrom().failure(e));
		});
	}

	public Uni<Void> setEntityDeleted(BaseRequest request) {
		return clientManager.getClient(request.getTenant(), false).onItem().transformToUni(client -> {
			return client.preparedQuery("UPDATE temporalentity SET deletedat=$1::TIMESTAMP")
					// You would think that we do the conversion in the db but somehow postgres
					// can't easily convert utc into a timestamp without a timezone
					.execute(Tuple.of(SerializationTools.notifiedAt_formatter.format(
							LocalDateTime.ofInstant(Instant.ofEpochMilli(request.getSendTimestamp()), ZoneId.of("Z")))))
					.onItem().transformToUni(t -> Uni.createFrom().voidItem());
		});

	}

	public Uni<Void> setAttributeDeleted(DeleteAttributeRequest request) {
		return clientManager.getClient(request.getTenant(), false).onItem().transformToUni(client -> {
			String sql = "WITH a as (select gen_random_uuid() as instanceid, id as iid from temporalentity where e_id = $1)\r\n"
					+ "INSERT INTO temporalentityattrinstance(attributeid, instanceid, data, deletedat, iid, datasetid) SELECT $2, a.instanceid, ('{\""
					+ NGSIConstants.NGSI_LD_DELETED_AT + "\": [{\"" + NGSIConstants.JSON_LD_TYPE + "\": \""
					+ NGSIConstants.NGSI_LD_DATE_TIME + "\", \"" + NGSIConstants.JSON_LD_VALUE + "\": \"$3\"}], \""
					+ NGSIConstants.NGSI_LD_INSTANCE_ID + "\": [{\"" + NGSIConstants.JSON_LD_ID
					+ "\": \"'|| a.instanceid||'\"}]";
			if (request.getDatasetId() != null) {
				sql += ",\"" + NGSIConstants.NGSI_LD_DATA_SET_ID + "\": [{\"" + NGSIConstants.JSON_LD_ID
						+ "\": \"$4\"}]";
			}
			sql += "}')::jsonb, $3::TIMSTAMP, a.iid, $4 FROM a";
			return client.preparedQuery(sql)
					// You would think that we do the date conversion in the db but somehow postgres
					// can't easily convert utc into a timestamp without a timezone
					.execute(Tuple.of(request.getId(), request.getAttribName(), request.getDatasetId(),
							SerializationTools.notifiedAt_formatter.format(LocalDateTime
									.ofInstant(Instant.ofEpochMilli(request.getSendTimestamp()), ZoneId.of("Z")))))
					.onItem().transformToUni(t -> Uni.createFrom().voidItem());
		});
	}

}