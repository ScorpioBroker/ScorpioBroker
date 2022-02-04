package eu.neclab.ngsildbroker.commons.interfaces;

import java.util.List;
import java.util.Map;

import com.github.jsonldjava.core.Context;

import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

public interface PayloadQueryParamParser {



	public QueryParams parse(Map<String, Object> payload, Integer limit, Integer offset, int defaultLimit, int maxLimit, boolean count, List<String> options, Context context)
			throws ResponseException;
}
