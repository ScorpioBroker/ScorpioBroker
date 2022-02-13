package eu.neclab.ngsildbroker.commons.controllers;

import java.net.URLDecoder;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Response.StatusType;

import org.apache.http.HttpStatus;
import org.jboss.resteasy.reactive.RestResponse;
import org.jboss.resteasy.reactive.server.jaxrs.RestResponseBuilderImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;

import eu.neclab.ngsildbroker.commons.datatypes.results.QueryResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.EntryQueryService;
import eu.neclab.ngsildbroker.commons.interfaces.PayloadQueryParamParser;
import eu.neclab.ngsildbroker.commons.ngsiqueries.ParamsResolver;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpServerRequest;

public interface QueryControllerFunctions {// implements QueryHandlerInterface {
	final static Logger logger = LoggerFactory.getLogger(QueryControllerFunctions.class);
	static JsonLdOptions opts = new JsonLdOptions(JsonLdOptions.JSON_LD_1_1);
	static final String emptyResult1 = "[ ]";
	static final String emptyResult2 = "[]";

	/**
	 * Method(GET) for multiple attributes separated by comma list
	 * 
	 * @param request
	 * @param entityId
	 * @param attrs
	 * @return
	 * @throws ResponseException
	 */

	public static RestResponse<String> getEntity(EntryQueryService queryService, HttpServerRequest request,
			List<String> attrs, List<String> options, String entityId, boolean temporal, int defaultLimit,
			int maxLimit) {

		try {
			HttpUtils.validateUri(entityId);
		} catch (ResponseException exception) {
			return RestResponseBuilderImpl.create(RestResponse.Status.BAD_REQUEST,
					new eu.neclab.ngsildbroker.commons.datatypes.RestResponse(ErrorType.BadRequestData,
							"id is not a URI").toJson())
					.build();
		}
		String originalQuery = NGSIConstants.QUERY_PARAMETER_ID + "=" + entityId;
		MultiMap paramMap = request.params();
		paramMap.add(NGSIConstants.QUERY_PARAMETER_ID, entityId);
		RestResponse<String> result = getQueryData(queryService, request, originalQuery, paramMap, false, false,
				temporal, defaultLimit, maxLimit, null);
		if (result.getEntity().equals(emptyResult1) || result.getEntity().equals(emptyResult2)) {
			return RestResponseBuilderImpl.create(RestResponse.Status.NOT_FOUND,
					new eu.neclab.ngsildbroker.commons.datatypes.RestResponse(ErrorType.NotFound, "Resource not found.")
							.toJson())
					.build();
		}
		return result;
	}

	/**
	 * Method(GET) for fetching all entities by kafka and other geo query operation
	 * by database
	 * 
	 * @param request
	 * @param type
	 * @return ResponseEntity object
	 */
	public static RestResponse<String> queryForEntries(EntryQueryService queryService, HttpServerRequest request,
			boolean temporal, int defaultLimit, int maxLimit, boolean typeRequired) {
		return getQueryData(queryService, request, request.getQueryString(), HttpUtils.getQueryParamMap(request),
				typeRequired, true, temporal, defaultLimit, maxLimit, null);
	}

	public static RestResponse<String> getAllTypes(EntryQueryService queryService, HttpServerRequest request,
			boolean details, boolean temporal, int defaultLimit, int maxLimit) {
		String check = "NonDeatilsType";
		if (details == true) {
			check = "deatilsType";
		}
		RestResponse<String> result = getQueryData(queryService, request, "", HttpUtils.getQueryParamMap(request),
				false, false, false, defaultLimit, maxLimit, check);
		if (result.getBody().equals(emptyResult1) || result.getBody().equals(emptyResult2)) {
			return ResponseEntity.status(HttpStatus.NOT_FOUND)
					.body(new RestResponse(ErrorType.NotFound, "Resource not found.").toJson());
		}
		return result;
	}

	public static RestResponse<String> getType(EntryQueryService queryService, HttpServerRequest request, String type,
			boolean details, boolean temporal, int defaultLimit, int maxLimit) {
		String check = "type";
		MultiValueMap<String, String> params = HttpUtils.getQueryParamMap(request);
		params.put(NGSIConstants.QUERY_PARAMETER_ATTRS, Arrays.asList(type));
		RestResponse<String> result = getQueryData(queryService, request, "", params, false, false, false, defaultLimit,
				maxLimit, check);
		if (result.getBody().equals(emptyResult1) || result.getBody().equals(emptyResult2)) {
			return ResponseEntity.status(HttpStatus.NOT_FOUND)
					.body(new RestResponse(ErrorType.NotFound, "Resource not found.").toJson());
		}
		return result;
	}

	public static RestResponse<String> getAllAttributes(EntryQueryService queryService, HttpServerRequest request,
			boolean details, boolean temporal, int defaultLimit, int maxLimit) {

		String check = "NonDeatilsAttributes";
		if (details == true) {
			check = "deatilsAttributes";
		}
		RestResponse<String> result = getQueryData(queryService, request, "", HttpUtils.getQueryParamMap(request),
				false, false, false, defaultLimit, maxLimit, check);
		if (result.getBody().equals(emptyResult1) || result.getBody().equals(emptyResult2)) {
			return ResponseEntity.status(HttpStatus.NOT_FOUND)
					.body(new RestResponse(ErrorType.NotFound, "Resource not found.").toJson());
		}
		return result;
	}

	public static RestResponse<String> getAttribute(EntryQueryService queryService, HttpServerRequest request,
			String attribute, boolean details, boolean temporal, int defaultLimit, int maxLimit) {
		String check = "Attribute";
		MultiValueMap<String, String> params = HttpUtils.getQueryParamMap(request);
		params.put(NGSIConstants.QUERY_PARAMETER_ATTRS, Arrays.asList(attribute));
		RestResponse<String> result = getQueryData(queryService, request, "", params, false, false, false, defaultLimit,
				maxLimit, check);
		if (result.getBody().equals(emptyResult1) || result.getBody().equals(emptyResult2)) {
			return ResponseEntity.status(HttpStatus.NOT_FOUND)
					.body(new RestResponse(ErrorType.NotFound, "Resource not found.").toJson());
		}
		return result;
	}

	private static RestResponse<String> getQueryData(EntryQueryService queryService, HttpServerRequest request,
			String originalQueryParams, MultiMap paramMap, boolean typeRequired,
			boolean forceArray, boolean temporal, int defaultLimit, int maxLimit, String check) {
		String tenantid = request.getHeader(NGSIConstants.TENANT_HEADER_FOR_INTERNAL_CHECK);
		try {
			logger.trace("getAllEntity() ::");
			List<Object> linkHeaders = HttpUtils.getAtContext(request);
			Context context = JsonLdProcessor.getCoreContextClone();
			context = context.parse(linkHeaders, true);
			if (originalQueryParams != null) {
				originalQueryParams = URLDecoder.decode(originalQueryParams, NGSIConstants.ENCODE_FORMAT);
				QueryParams qp = ParamsResolver.getQueryParamsFromUriQuery(paramMap, context, temporal, typeRequired,
						defaultLimit, maxLimit);
				if (qp == null) // invalid query
					throw new ResponseException(ErrorType.InvalidRequest, "Empty query parameters");
				qp.setTenant(tenantid);
				qp.setCheck(check);
				ArrayListMultimap<String, String> headers = HttpUtils.getHeaders(request);
				QueryResult qResult;
				try {
					qResult = queryService.getData(qp, originalQueryParams, linkHeaders, headers, false);
				} catch (Exception e) {
					// logger.error(e.getMessage());
					logger.error("Tenant for the request not found", e);
					return ResponseEntity.status(HttpStatus.NOT_FOUND)
							.body(new RestResponse(ErrorType.TenantNotFound, "Tenant not found.").toJson());
				}
				return HttpUtils.generateReply(request, qResult, forceArray, qp.getCountResult(), context, linkHeaders,
						AppConstants.QUERY_ENDPOINT);

			} else {
				return HttpUtils
						.handleControllerExceptions(new ResponseException(ErrorType.BadRequestData, "invalid query"));
			}
		} catch (Exception exception) {
			return HttpUtils.handleControllerExceptions(exception);
		}
	}

	@SuppressWarnings({ "unchecked" })
	public static RestResponse<String> postQuery(EntryQueryService queryService, HttpServerRequest request,
			String payload, Integer limit, Integer offset, String qToken, List<String> options, boolean count,
			int defaultLimit, int maxLimit, int payloadType, PayloadQueryParamParser queryParser) {
		try {
			List<Object> linkHeaders = HttpUtils.getAtContext(request);
			boolean atContextAllowed = HttpUtils.doPreflightCheck(request, linkHeaders);
			Map<String, Object> rawPayload = (Map<String, Object>) JsonUtils.fromString(payload);
			Map<String, Object> queries = (Map<String, Object>) JsonLdProcessor
					.expand(linkHeaders, rawPayload, opts, payloadType, atContextAllowed).get(0);

			if (rawPayload.containsKey(NGSIConstants.JSON_LD_CONTEXT)) {
				Object payloadContext = rawPayload.get(NGSIConstants.JSON_LD_CONTEXT);
				if (payloadContext instanceof List) {
					linkHeaders.addAll((List<Object>) payloadContext);
				} else {
					linkHeaders.add(payloadContext);
				}
			}
			Context context = JsonLdProcessor.getCoreContextClone();

			context = context.parse(linkHeaders, true);

			QueryParams params = queryParser.parse(queries, limit, offset, defaultLimit, maxLimit, count, options,
					context);

			return HttpUtils.generateReply(request,
					queryService.getData(params, payload, linkHeaders, HttpUtils.getHeaders(request), true), true,
					count, context, linkHeaders, AppConstants.QUERY_ENDPOINT);

		} catch (Exception exception) {
			return HttpUtils.handleControllerExceptions(exception);
		}
	}

}