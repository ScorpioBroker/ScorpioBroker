package eu.neclab.ngsildbroker.commons.interfaces;

import java.util.List;
import java.util.Map;

import eu.neclab.ngsildbroker.commons.datatypes.BatchInfo;
import eu.neclab.ngsildbroker.commons.datatypes.results.NGSILDOperationResult;
import io.smallrye.mutiny.Uni;

public interface EntryCRUDService {

	Uni<NGSILDOperationResult> createEntry(String tenant, Map<String, Object> resolved, List<Object> originalContext,
			BatchInfo batchInfo);

	Uni<NGSILDOperationResult> createEntry(String tenant, Map<String, Object> resolved, List<Object> originalContext);

	Uni<NGSILDOperationResult> updateEntry(String tenant, String entityId, Map<String, Object> entry,
			List<Object> originalContext);

	Uni<NGSILDOperationResult> updateEntry(String tenant, String entityId, Map<String, Object> entry,
			List<Object> originalContext, BatchInfo batchInfo);

	Uni<NGSILDOperationResult> appendToEntry(String tenant, String entityId, Map<String, Object> entry,
			String[] options, List<Object> originalContext);

	Uni<NGSILDOperationResult> appendToEntry(String tenant, String entityId, Map<String, Object> entry,
			String[] options, List<Object> originalContext, BatchInfo batchInfo);

	Uni<NGSILDOperationResult> deleteEntry(String tenant, String entryId, List<Object> originalContext);

	Uni<NGSILDOperationResult> deleteEntry(String tenant, String entryId, List<Object> originalContext,
			BatchInfo batchInfo);

	Uni<Void> sendFail(BatchInfo batchInfo);

}
