package eu.neclab.ngsildbroker.commons.controllers;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonParseException;
import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLdError;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.ArrayListMultimap;

import org.jboss.resteasy.reactive.RestResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.EntryQueryService;
import eu.neclab.ngsildbroker.commons.interfaces.PayloadQueryParamParser;
import eu.neclab.ngsildbroker.commons.ngsiqueries.ParamsResolver;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.tuples.Tuple4;
import io.smallrye.mutiny.unchecked.Unchecked;
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
		return HttpUtils.validateUri(entityId).onItem().transformToUni(t -> {
			String originalQuery = entityId;
			MultiMap paramMap = request.params();
			paramMap.add(NGSIConstants.QUERY_PARAMETER_ID, entityId);
			return getQueryData(queryService, request, originalQuery, paramMap, false, false, temporal, defaultLimit,
					maxLimit, null);
		}).onItem().transform(t -> {
			if (t.getEntity().equals(emptyResult1) || t.getEntity().equals(emptyResult2)) {
				return HttpUtils.NOT_FOUND_REPLY;
			} else {
				return t;
			}
		}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);
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
		return getQueryData(queryService, request, request.query(), HttpUtils.getQueryParamMap(request), typeRequired,
				true, temporal, defaultLimit, maxLimit, null).onFailure()
				.recoverWithItem(HttpUtils::handleControllerExceptions);
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
				}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);
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
				}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);
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
				}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);
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
				}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);
	}

	private static Uni<RestResponse<Object>> getQueryData(EntryQueryService queryService, HttpServerRequest request,
			String originalQueryParams, MultiMap paramMap, boolean typeRequired, boolean forceArray, boolean temporal,
			int defaultLimit, int maxLimit, String check) {
		logger.trace("getAllEntity() ::");
		if (check == null && (originalQueryParams == null || originalQueryParams.isEmpty())) {
			return Uni.createFrom().failure(new ResponseException(ErrorType.BadRequestData, "Empty query parameters"));
		}
		return HttpUtils.getAtContext(request).onItem().transformToUni(t -> {
			String tenantid = request.getHeader(NGSIConstants.TENANT_HEADER);
			ArrayListMultimap<String, String> headers = HttpUtils.getHeaders(request);
			Context context = JsonLdProcessor.getCoreContextClone().parse(t, true);
			QueryParams qp;
			String decodedQueryParams;
			try {
				decodedQueryParams = URLDecoder.decode(originalQueryParams, NGSIConstants.ENCODE_FORMAT);
			} catch (UnsupportedEncodingException e) {
				return Uni.createFrom().failure(e);
			}
			try {
				qp = ParamsResolver.getQueryParamsFromUriQuery(paramMap, context, temporal, typeRequired, defaultLimit,
						maxLimit);
			} catch (ResponseException e) {
				return Uni.createFrom().failure(e);
			}
			qp.setTenant(tenantid);
			qp.setCheck(check);
			return queryService.getData(qp, decodedQueryParams, t, headers, false).onItem()
					.transformToUni(t2 -> HttpUtils.generateReply(request, t2, forceArray, qp.getCountResult(), context,
							t, AppConstants.QUERY_ENDPOINT, qp.getConcise()))
					.runSubscriptionOn(Infrastructure.getDefaultExecutor());

		}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);
	}

	@SuppressWarnings({ "unchecked" })
	public static Uni<RestResponse<Object>> postQuery(EntryQueryService queryService, HttpServerRequest request,
			String payload, Integer limit, Integer offset, String qToken, List<String> options, boolean count,
			int defaultLimit, int maxLimit, int payloadType, PayloadQueryParamParser queryParser) {
		return HttpUtils.getAtContext(request).onItem().transformToUni(t -> {
			boolean atContextAllowed;
			try {
				atContextAllowed = HttpUtils.doPreflightCheck(request, t);
			} catch (ResponseException e) {
				return Uni.createFrom().failure(e);
			}
			Map<String, Object> rawPayload;
			try {
				rawPayload = (Map<String, Object>) JsonUtils.fromString(payload);
			} catch (Exception e) {
				return Uni.createFrom().failure(e);
			}

			Map<String, Object> queries;
			try {
				queries = (Map<String, Object>) JsonLdProcessor
						.expand(t, rawPayload, opts, payloadType, atContextAllowed).get(0);
			} catch (Exception e) {
				return Uni.createFrom().failure(e);
			}

			if (rawPayload.containsKey(NGSIConstants.JSON_LD_CONTEXT)) {
				Object payloadContext = rawPayload.get(NGSIConstants.JSON_LD_CONTEXT);
				if (payloadContext instanceof List) {
					t.addAll((List<Object>) payloadContext);
				} else {
					t.add(payloadContext);
				}
			}
			Context context = JsonLdProcessor.getCoreContextClone();

			context = context.parse(t, true);

			QueryParams params;
			try {
				params = queryParser.parse(queries, limit, offset, defaultLimit, maxLimit, count, options, context);
			} catch (ResponseException e) {
				return Uni.createFrom().failure(e);
			}
			ArrayListMultimap<String, String> headers = HttpUtils.getHeaders(request);
			String tenant = HttpUtils.getTenantFromHeaders(headers);
			if (tenant != null) {
				params.setTenant(tenant);
			}
			return Uni.createFrom().item(Tuple4.of(params, t, headers, context));
		}).onItem()
				.transformToUni(
						t -> queryService.getData(t.getItem1(), payload, t.getItem2(), t.getItem3(), true).onItem()
								.transformToUni(t2 -> HttpUtils.generateReply(request, t2, true, count, t.getItem4(),
										t.getItem2(), AppConstants.QUERY_ENDPOINT, null)))
				.onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);

	}

}