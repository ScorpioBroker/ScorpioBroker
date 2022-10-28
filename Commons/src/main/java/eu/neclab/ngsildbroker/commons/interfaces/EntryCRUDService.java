package eu.neclab.ngsildbroker.commons.interfaces;

import java.util.Map;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.datatypes.BatchInfo;
import eu.neclab.ngsildbroker.commons.datatypes.results.CreateResult;
import eu.neclab.ngsildbroker.commons.datatypes.results.UpdateResult;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

public interface EntryCRUDService {
	UpdateResult updateEntry(ArrayListMultimap<String, String> headers, String entityId, Map<String, Object> entry)
			throws ResponseException, Exception;

	UpdateResult updateEntry(ArrayListMultimap<String, String> headers, String entityId, Map<String, Object> entry,
			BatchInfo batchInfo) throws ResponseException, Exception;

	UpdateResult appendToEntry(ArrayListMultimap<String, String> headers, String entityId, Map<String, Object> entry,
			String[] options) throws ResponseException, Exception;

	UpdateResult appendToEntry(ArrayListMultimap<String, String> headers, String entityId, Map<String, Object> entry,
			String[] options, BatchInfo batchInfo) throws ResponseException, Exception;

	CreateResult createEntry(ArrayListMultimap<String, String> headers, Map<String, Object> resolved)
			throws ResponseException, Exception;

	CreateResult createEntry(ArrayListMultimap<String, String> headers, Map<String, Object> resolved, BatchInfo batchInfo)
			throws ResponseException, Exception;

	boolean deleteEntry(ArrayListMultimap<String, String> headers, String entryId) throws ResponseException, Exception;

	boolean deleteEntry(ArrayListMultimap<String, String> headers, String entryId, BatchInfo batchInfo)
			throws ResponseException, Exception;

	void sendFail(BatchInfo batchInfo);



}
