package eu.neclab.ngsildbroker.commons.interfaces;

import java.util.Map;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.datatypes.AppendResult;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

public interface EntityCRUDService {

	AppendResult appendMessage(ArrayListMultimap<String, String> headers, String entityId, Map<String, Object> entry,
			String options) throws ResponseException, Exception;

	String createMessage(ArrayListMultimap<String, String> headers, Map<String, Object> resolved) throws ResponseException, Exception;

	boolean deleteEntity(ArrayListMultimap<String, String> headers, String entityId) throws ResponseException, Exception;

}
