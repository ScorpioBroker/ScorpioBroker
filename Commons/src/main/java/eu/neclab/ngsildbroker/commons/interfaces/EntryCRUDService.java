package eu.neclab.ngsildbroker.commons.interfaces;

import java.util.Map;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.datatypes.AppendResult;
import eu.neclab.ngsildbroker.commons.datatypes.UpdateResult;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

public interface EntryCRUDService {
	UpdateResult updateEntry(ArrayListMultimap<String, String> headers, String entityId, Map<String, Object> entry)
			throws ResponseException, Exception;

	AppendResult appendToEntry(ArrayListMultimap<String, String> headers, String entityId, Map<String, Object> entry,
			String[] options) throws ResponseException, Exception;

	String createEntry(ArrayListMultimap<String, String> headers, Map<String, Object> resolved)
			throws ResponseException, Exception;

	boolean deleteEntry(ArrayListMultimap<String, String> headers, String entryId) throws ResponseException, Exception;

}
