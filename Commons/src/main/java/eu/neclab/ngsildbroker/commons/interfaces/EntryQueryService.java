package eu.neclab.ngsildbroker.commons.interfaces;

import java.util.List;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.datatypes.results.QueryResult;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

public interface EntryQueryService {

	public QueryResult getData(QueryParams qp, String rawQueryString, List<Object> linkHeaders,
			ArrayListMultimap<String, String> headers, Boolean postQuery) throws ResponseException;

}
