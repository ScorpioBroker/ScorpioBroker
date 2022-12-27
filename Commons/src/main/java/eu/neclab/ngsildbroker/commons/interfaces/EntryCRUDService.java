package eu.neclab.ngsildbroker.commons.interfaces;

import java.util.List;
import java.util.Map;

import com.github.jsonldjava.core.Context;

import eu.neclab.ngsildbroker.commons.datatypes.BatchInfo;
import eu.neclab.ngsildbroker.commons.datatypes.results.NGSILDOperationResult;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;

public interface EntryCRUDService {

	Uni<NGSILDOperationResult> createEntry(String tenant, Map<String, Object> resolved, Context originalContext);

	Uni<List<NGSILDOperationResult>> createMultipleEntry(String tenant,
			List<Tuple2<Context, Map<String, Object>>> entity2Context);

	Uni<NGSILDOperationResult> updateEntry(String tenant, String entityId, Map<String, Object> entry,
			Context originalContext);

	Uni<NGSILDOperationResult> appendToEntry(String tenant, String entityId, Map<String, Object> entry,
			String[] options, Context originalContext);

	Uni<NGSILDOperationResult> deleteEntry(String tenant, String entryId, Context originalContext);

	Uni<Void> sendFail(BatchInfo batchInfo);

}
