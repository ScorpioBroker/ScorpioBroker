package eu.neclab.ngsildbroker.commons.interfaces;

import java.util.Map;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.datatypes.results.CreateResult;
import eu.neclab.ngsildbroker.commons.datatypes.results.UpdateResult;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

public interface EntryCRUDService {
	UpdateResult updateEntry(ArrayListMultimap<String, String> headers, String entityId, Map<String, Object> entry)
			throws ResponseException, Exception;

	UpdateResult updateEntry(ArrayListMultimap<String, String> headers, String entityId, Map<String, Object> entry,
			int batchId) throws ResponseException, Exception;

	UpdateResult appendToEntry(ArrayListMultimap<String, String> headers, String entityId, Map<String, Object> entry,
			String[] options) throws ResponseException, Exception;

	UpdateResult appendToEntry(ArrayListMultimap<String, String> headers, String entityId, Map<String, Object> entry,
			String[] options, int batchId) throws ResponseException, Exception;

	CreateResult createEntry(ArrayListMultimap<String, String> headers, Map<String, Object> resolved)
			throws ResponseException, Exception;

	CreateResult createEntry(ArrayListMultimap<String, String> headers, Map<String, Object> resolved, int batchId)
			throws ResponseException, Exception;

	boolean deleteEntry(ArrayListMultimap<String, String> headers, String entryId) throws ResponseException, Exception;

	boolean deleteEntry(ArrayListMultimap<String, String> headers, String entryId, int batchId)
			throws ResponseException, Exception;

	void finalizeBatch(int batchId);

}
