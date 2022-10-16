package eu.neclab.ngsildbroker.commons.interfaces;

import java.util.Map;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.datatypes.results.CreateResult;
import eu.neclab.ngsildbroker.commons.datatypes.results.UpdateResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.storage.StorageDAO;
import io.smallrye.mutiny.Uni;

public interface EntryCRUDService {
	Uni<UpdateResult> updateEntry(ArrayListMultimap<String, String> headers, String entityId,
			Map<String, Object> entry);

	Uni<UpdateResult> appendToEntry(ArrayListMultimap<String, String> headers, String entityId,
			Map<String, Object> entry, String[] options);

	Uni<CreateResult> createEntry(ArrayListMultimap<String, String> headers, Map<String, Object> resolved);

	Uni<Boolean> deleteEntry(ArrayListMultimap<String, String> headers, String entryId);

	Uni<UpdateResult> updateEntry(ArrayListMultimap<String, String> headers, String entityId, Map<String, Object> entry,
			int batchId);

	Uni<UpdateResult> appendToEntry(ArrayListMultimap<String, String> headers, String entityId,
			Map<String, Object> entry, String[] options, int batchId);

	Uni<CreateResult> createEntry(ArrayListMultimap<String, String> headers, Map<String, Object> resolved, int batchId);

	Uni<Boolean> deleteEntry(ArrayListMultimap<String, String> headers, String entryId, int batchId);

	Uni<Void> finalizeBatch(int batchId);
	
	public static Uni<Map<String, Object>> validateIdAndGetBody(String entityId, String tenantId, StorageDAO dao) {
		// null id check
		if (entityId == null) {
			return Uni.createFrom()
					.failure(new ResponseException(ErrorType.BadRequestData, "empty entity id not allowed"));
		}
		return dao.getEntity(entityId, tenantId);
	}

}
