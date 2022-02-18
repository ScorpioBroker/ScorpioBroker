package eu.neclab.ngsildbroker.commons.controllers;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.jboss.resteasy.reactive.RestResponse;
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
import io.smallrye.mutiny.Uni;
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

	public static Uni<RestResponse<Object>> getEntity(EntryQueryService queryService, HttpServerRequest request,
			List<String> attrs, List<String> options, String entityId, boolean temporal, int defaultLimit,
			int maxLimit) {

		try {
			HttpUtils.validateUri(entityId);
		} catch (ResponseException exception) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(exception));
		}
		String originalQuery = NGSIConstants.QUERY_PARAMETER_ID + "=" + entityId;
		MultiMap paramMap = request.params();
		paramMap.add(NGSIConstants.QUERY_PARAMETER_ID, entityId);
		return getQueryData(queryService, request, originalQuery, paramMap, false, false, temporal, defaultLimit,
				maxLimit, null).onItem().transform(t -> {
					if (t.getEntity().equals(emptyResult1) || t.getEntity().equals(emptyResult2)) {
						return HttpUtils.NOT_FOUND_REPLY;
					} else {
						return t;
					}
				});

	}

	/**
	 * Method(GET) for fetching all entities by kafka and other geo query operation
	 * by database
	 * 
	 * @param request
	 * @param type
	 * @return ResponseEntity object
	 */
	public static Uni<RestResponse<Object>> queryForEntries(EntryQueryService queryService, HttpServerRequest request,
			boolean temporal, int defaultLimit, int maxLimit, boolean typeRequired) {
		return getQueryData(queryService, request, request.query(), request.params(), typeRequired, true, temporal,
				defaultLimit, maxLimit, null);
	}

	public static Uni<RestResponse<Object>> getAllTypes(EntryQueryService queryService, HttpServerRequest request,
			boolean details, boolean temporal, int defaultLimit, int maxLimit) {
		String check = "NonDeatilsType";
		if (details == true) {
			check = "deatilsType";
		}
		return getQueryData(queryService, request, "", request.params(), false, false, false, defaultLimit, maxLimit,
				check).onItem().transform(t -> {
					if (t.getEntity().equals(emptyResult1) || t.getEntity().equals(emptyResult2)) {
						return HttpUtils.NOT_FOUND_REPLY;
					} else {
						return t;
					}
				});
	}

	public static Uni<RestResponse<Object>> getType(EntryQueryService queryService, HttpServerRequest request,
			String type, boolean details, boolean temporal, int defaultLimit, int maxLimit) {
		String check = "type";
		MultiMap params = request.params();
		params.add(NGSIConstants.QUERY_PARAMETER_ATTRS, type);
		return getQueryData(queryService, request, "", params, false, false, false, defaultLimit, maxLimit, check)
				.onItem().transform(t -> {
					if (t.getEntity().equals(emptyResult1) || t.getEntity().equals(emptyResult2)) {
						return HttpUtils.NOT_FOUND_REPLY;
					} else {
						return t;
					}
				});
	}

	public static Uni<RestResponse<Object>> getAllAttributes(EntryQueryService queryService, HttpServerRequest request,
			boolean details, boolean temporal, int defaultLimit, int maxLimit) {

		String check = "NonDeatilsAttributes";
		if (details == true) {
			check = "deatilsAttributes";
		}
		return getQueryData(queryService, request, "", request.params(), false, false, false, defaultLimit, maxLimit,
				check).onItem().transform(t -> {
					if (t.getEntity().equals(emptyResult1) || t.getEntity().equals(emptyResult2)) {
						return HttpUtils.NOT_FOUND_REPLY;
					} else {
						return t;
					}
				});
	}

	public static Uni<RestResponse<Object>> getAttribute(EntryQueryService queryService, HttpServerRequest request,
			String attribute, boolean details, boolean temporal, int defaultLimit, int maxLimit) {
		String check = "Attribute";
		MultiMap params = request.params();
		params.add(NGSIConstants.QUERY_PARAMETER_ATTRS, attribute);
		return getQueryData(queryService, request, "", params, false, false, false, defaultLimit, maxLimit, check)
				.onItem().transform(t -> {
					if (t.getEntity().equals(emptyResult1) || t.getEntity().equals(emptyResult2)) {
						return HttpUtils.NOT_FOUND_REPLY;
					} else {
						return t;
					}
				});
	}

	private static Uni<RestResponse<Object>> getQueryData(EntryQueryService queryService, HttpServerRequest request,
			String originalQueryParams, MultiMap paramMap, boolean typeRequired, boolean forceArray, boolean temporal,
			int defaultLimit, int maxLimit, String check) {
		logger.trace("getAllEntity() ::");
		if (originalQueryParams == null || originalQueryParams.isEmpty()) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(
					new ResponseException(ErrorType.InvalidRequest, "Empty query parameters")));
		}
		String tenantid = request.getHeader(NGSIConstants.TENANT_HEADER_FOR_INTERNAL_CHECK);
		List<Object> linkHeaders = HttpUtils.getAtContext(request);
		ArrayListMultimap<String, String> headers = HttpUtils.getHeaders(request);
		Context context = JsonLdProcessor.getCoreContextClone().parse(linkHeaders, true);
		QueryParams qp;
		try {
			originalQueryParams = URLDecoder.decode(originalQueryParams, NGSIConstants.ENCODE_FORMAT);
			qp = ParamsResolver.getQueryParamsFromUriQuery(paramMap, context, temporal, typeRequired, defaultLimit,
					maxLimit);
			qp.setTenant(tenantid);
			qp.setCheck(check);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
		}
		return queryService.getData(qp, originalQueryParams, linkHeaders, headers, false).onItem().transform(t -> {
			try {
				return HttpUtils.generateReply(request, t, forceArray, qp.getCountResult(), context, linkHeaders,
						AppConstants.QUERY_ENDPOINT);
			} catch (ResponseException e) {
				return HttpUtils.handleControllerExceptions(e);
			}
		}).onFailure().recoverWithItem(t -> {
			return HttpUtils.handleControllerExceptions(t);
		});
	}

	@SuppressWarnings({ "unchecked" })
	public static RestResponse<Object> postQuery(EntryQueryService queryService, HttpServerRequest request,
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
					queryService.getData(params, payload, linkHeaders, HttpUtils.getHeaders(request), true).await()
							.atMost(Duration.ofMillis(500)),
					true, count, context, linkHeaders, AppConstants.QUERY_ENDPOINT);

		} catch (Exception exception) {
			return HttpUtils.handleControllerExceptions(exception);
		}
	}

}