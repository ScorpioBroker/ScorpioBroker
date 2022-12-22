package eu.neclab.ngsildbroker.commons.interfaces;

import java.util.Map;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.datatypes.BatchInfo;
import eu.neclab.ngsildbroker.commons.datatypes.results.CreateResult;
import eu.neclab.ngsildbroker.commons.datatypes.results.UpdateResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.storage.StorageDAO;
import io.smallrye.mutiny.Uni;
import io.vertx.core.MultiMap;

public interface EntryCRUDService {
	Uni<UpdateResult> updateEntry(MultiMap headers, String entityId, Map<String, Object> entry);

	Uni<UpdateResult> appendToEntry(MultiMap headers, String entityId, Map<String, Object> entry, String[] options);

	Uni<CreateResult> createEntry(MultiMap headers, Map<String, Object> resolved);

	Uni<Boolean> deleteEntry(MultiMap headers, String entryId);

	Uni<UpdateResult> updateEntry(MultiMap headers, String entityId, Map<String, Object> entry, BatchInfo batchInfo);

	Uni<UpdateResult> appendToEntry(MultiMap headers, String entityId, Map<String, Object> entry, String[] options,
			BatchInfo batchInfo);

	Uni<CreateResult> createEntry(MultiMap headers, Map<String, Object> resolved, BatchInfo batchInfo);

	Uni<Boolean> deleteEntry(MultiMap headers, String entryId, BatchInfo batchInfo);

	Uni<Void> sendFail(BatchInfo batchInfo);

	public static Uni<Map<String, Object>> validateIdAndGetBody(String entityId, String tenantId, StorageDAO dao) {
		// null id check
		if (entityId == null) {
			return Uni.createFrom()
					.failure(new ResponseException(ErrorType.BadRequestData, "empty entity id not allowed"));
		}
		return dao.getEntity(entityId, tenantId);
	}

}
