package eu.neclab.ngsildbroker.commons.interfaces;

import java.util.Map;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.datatypes.results.UpdateResult;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import io.smallrye.mutiny.Uni;

public interface EntryCRUDService {
	UpdateResult updateEntry(ArrayListMultimap<String, String> headers, String entityId, Map<String, Object> entry)
			throws ResponseException, Exception;

	UpdateResult appendToEntry(ArrayListMultimap<String, String> headers, String entityId, Map<String, Object> entry,
			String[] options) throws ResponseException, Exception;

	Uni<String> createEntry(ArrayListMultimap<String, String> headers, Map<String, Object> resolved);

	boolean deleteEntry(ArrayListMultimap<String, String> headers, String entryId) throws ResponseException, Exception;

}
