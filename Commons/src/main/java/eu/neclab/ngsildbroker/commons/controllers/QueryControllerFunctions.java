package eu.neclab.ngsildbroker.commons.controllers;

import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.datatypes.RestResponse;
import eu.neclab.ngsildbroker.commons.datatypes.results.QueryResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.EntryQueryService;
import eu.neclab.ngsildbroker.commons.interfaces.PayloadQueryParamParser;
import eu.neclab.ngsildbroker.commons.ngsiqueries.ParamsResolver;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.Validator;

public interface QueryControllerFunctions {// implements QueryHandlerInterface {
	final static Logger logger = LogManager.getLogger(QueryControllerFunctions.class);
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

	public static ResponseEntity<String> getEntity(EntryQueryService queryService, HttpServletRequest request,
			List<String> attrs, List<String> options, String entityId, boolean temporal, int defaultLimit,
			int maxLimit) {

		try {
			HttpUtils.validateUri(entityId);
		} catch (ResponseException exception) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST)
					.body(new RestResponse(ErrorType.BadRequestData, "id is not a URI").toJson());
		}
		String originalQuery = NGSIConstants.QUERY_PARAMETER_ID + "=" + entityId;
		LinkedMultiValueMap<String, String> paramMap = new LinkedMultiValueMap<String, String>();
		paramMap.add(NGSIConstants.QUERY_PARAMETER_ID, entityId);
		ResponseEntity<String> result = getQueryData(queryService, request, originalQuery, paramMap, attrs, null, null,
				null, options, false, true, false, null, temporal, defaultLimit, maxLimit);
		if (result.getBody().equals(emptyResult1) || result.getBody().equals(emptyResult2)) {
			return ResponseEntity.status(HttpStatus.NOT_FOUND)
					.body(new RestResponse(ErrorType.NotFound, "Resource not found.").toJson());
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
	public static ResponseEntity<String> queryForEntries(EntryQueryService queryService, HttpServletRequest request,
			List<String> attrs, Integer limit, Integer offset, String qToken, List<String> options,
			Boolean showServices, boolean count, boolean temporal, int defaultLimit, int maxLimit, boolean dontCheckForType) {

		return getQueryData(queryService, request, request.getQueryString(), HttpUtils.getQueryParamMap(request), attrs,
				limit, offset, qToken, options, showServices, dontCheckForType, count, null, temporal, defaultLimit, maxLimit);
	}

	public static ResponseEntity<String> getAllTypes(EntryQueryService queryService, HttpServletRequest request,
			boolean details, boolean temporal, int defaultLimit, int maxLimit) {
		String check = "NonDeatilsType";
		if (details == true) {
			check = "deatilsType";
		}
		ResponseEntity<String> result = getQueryData(queryService, request, null, HttpUtils.getQueryParamMap(request),
				null, null, null, null, null, false, true, false, check, temporal, defaultLimit, maxLimit);
		if (result.getBody().equals(emptyResult1) || result.getBody().equals(emptyResult2)) {
			return ResponseEntity.status(HttpStatus.NOT_FOUND)
					.body(new RestResponse(ErrorType.NotFound, "Resource not found.").toJson());
		}
		return result;
	}

	public static ResponseEntity<String> getType(EntryQueryService queryService, HttpServletRequest request,
			String type, boolean details, boolean temporal, int defaultLimit, int maxLimit) {
		String check = "type";
		ArrayList<String> types = new ArrayList<String>();
		types.add(type);
		ResponseEntity<String> result = getQueryData(queryService, request, null, HttpUtils.getQueryParamMap(request),
				types, null, null, null, null, false, true, false, check, temporal, defaultLimit, maxLimit);
		if (result.getBody().equals(emptyResult1) || result.getBody().equals(emptyResult2)) {
			return ResponseEntity.status(HttpStatus.NOT_FOUND)
					.body(new RestResponse(ErrorType.NotFound, "Resource not found.").toJson());
		}
		return result;
	}

	public static ResponseEntity<String> getAllAttribute(EntryQueryService queryService, HttpServletRequest request,
			boolean details, boolean temporal, int defaultLimit, int maxLimit) {

		String check = "NonDeatilsAttributes";
		if (details == true) {
			check = "deatilsAttributes";
		}
		ResponseEntity<String> result = getQueryData(queryService, request, null, HttpUtils.getQueryParamMap(request),
				null, null, null, null, null, false, true, false, check, temporal, defaultLimit, maxLimit);
		if (result.getBody().equals(emptyResult1) || result.getBody().equals(emptyResult2)) {
			return ResponseEntity.status(HttpStatus.NOT_FOUND)
					.body(new RestResponse(ErrorType.NotFound, "Resource not found.").toJson());
		}
		return result;
	}

	public static ResponseEntity<String> getAttributes(EntryQueryService queryService, HttpServletRequest request,
			String attributes, boolean details, boolean temporal, int defaultLimit, int maxLimit) {
		String check = "Attribute";
		ArrayList<String> types = new ArrayList<String>();
		types.add(attributes);
		ResponseEntity<String> result = getQueryData(queryService, request, null, HttpUtils.getQueryParamMap(request),
				types, null, null, null, null, false, true, false, check, temporal, defaultLimit, maxLimit);
		if (result.getBody().equals(emptyResult1) || result.getBody().equals(emptyResult2)) {
			return ResponseEntity.status(HttpStatus.NOT_FOUND)
					.body(new RestResponse(ErrorType.NotFound, "Resource not found.").toJson());
		}
		return result;
	}

	private static ResponseEntity<String> getQueryData(EntryQueryService queryService, HttpServletRequest request,
			String originalQueryParams, MultiValueMap<String, String> paramMap, List<String> attrs, Integer limit,
			Integer offset, String qToken, List<String> options, Boolean showServices, boolean retrieve,
			Boolean countResult, String check, boolean temporal, int defaultLimit, int maxLimit) {
		// long start = System.currentTimeMillis();
		String tenantid = request.getHeader(NGSIConstants.TENANT_HEADER);

		if (limit == null) {
			limit = defaultLimit;
		}
		if (offset == null) {
			offset = 0;
		}

		try {
			logger.trace("getAllEntity() ::");
			if (countResult == false && limit == 0) {
				return ResponseEntity.status(HttpStatus.BAD_REQUEST).build();
			}
			// long prelink = System.currentTimeMillis();
			List<Object> linkHeaders = HttpUtils.getAtContext(request);
			Context context = JsonLdProcessor.getCoreContextClone();
			context = context.parse(linkHeaders, true);
			// long postlink = System.currentTimeMillis();

			if (retrieve || originalQueryParams != null) {
				Validator.validate(HttpUtils.getQueryParamMap(request), maxLimit, retrieve);
				if (originalQueryParams != null) {
					originalQueryParams = URLDecoder.decode(originalQueryParams, NGSIConstants.ENCODE_FORMAT);
				}
				QueryParams qp = ParamsResolver.getQueryParamsFromUriQuery(paramMap, context, temporal);
				if (qp == null) // invalid query
					throw new ResponseException(ErrorType.InvalidRequest, "Empty query parameters");
				qp.setTenant(tenantid);
				qp.setCheck(check);
				qp.setKeyValues((options != null && options.contains(NGSIConstants.QUERY_PARAMETER_OPTIONS_KEYVALUES)));
				qp.setIncludeSysAttrs(
						(options != null && options.contains(NGSIConstants.QUERY_PARAMETER_OPTIONS_SYSATTRS)));
				if (attrs != null) {
					ArrayList<String> expandedAttrs = new ArrayList<String>();
					for (String attrib : attrs) {
						try {
							expandedAttrs.add(ParamsResolver.expandAttribute(attrib, context));
						} catch (ResponseException exception) {
							continue;
						}
					}
					qp.setAttrs(String.join(",", expandedAttrs));
				}

				checkParamsForValidity(qp);
				ArrayListMultimap<String, String> headers = HttpUtils.getHeaders(request);
				QueryResult qResult;
				qp.setLimit(limit);
				qp.setOffSet(offset);
				qp.setCountResult(countResult);
				try {
					qResult = queryService.getData(qp, originalQueryParams, linkHeaders, headers, false);
				} catch (Exception e) {
					//logger.error(e.getMessage());
					logger.error("Tenant for the request not found", e);
					return ResponseEntity.status(HttpStatus.NOT_FOUND)
							.body(new RestResponse(ErrorType.TenantNotFound, "Tenant not found.").toJson());
				}
				return HttpUtils.generateReply(request, qResult, !retrieve, countResult, context, linkHeaders,
						AppConstants.QUERY_ENDPOINT);

			} else {
				ResponseException responseException = new ResponseException(ErrorType.BadRequestData, "invalid query");
				return ResponseEntity.status(responseException.getHttpStatus())
						.body(new RestResponse(responseException).toJson());
			}
		} catch (Exception exception) {
			return HttpUtils.handleControllerExceptions(exception);
		}
	}

	private static void checkParamsForValidity(QueryParams qp) throws ResponseException {
		if (qp.getGeometry() != null && !qp.getGeometry().isEmpty()) {
			if (!NGSIConstants.ALLOWED_GEOMETRIES.contains(qp.getGeometry())) {
				throw new ResponseException(ErrorType.BadRequestData, "Invalid geometry provided");
			}
		}
		if (qp.getGeorel() != null && qp.getGeorel().getGeorelOp() != null && !qp.getGeorel().getGeorelOp().isEmpty()) {
			if (!NGSIConstants.ALLOWED_GEOREL.contains(qp.getGeorel().getGeorelOp())) {
				throw new ResponseException(ErrorType.BadRequestData, "Invalid georel provided");
			}
		}

	}

	@SuppressWarnings({ "unchecked" })
	public static ResponseEntity<String> postQuery(EntryQueryService queryService, HttpServletRequest request,
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