package eu.neclab.ngsildbroker.commons.controllers;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.jboss.resteasy.reactive.RestResponse;
import org.slf4j.Logger;

import com.github.jsonldjava.core.JsonLdConsts;
import com.github.jsonldjava.core.JsonLdError;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.SubscriptionCRUDService;
import eu.neclab.ngsildbroker.commons.ngsiqueries.ParamsResolver;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.unchecked.Unchecked;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpServerRequest;
import com.google.common.net.HttpHeaders;

public interface SubscriptionControllerFunctions {
	static final JsonLdOptions opts = new JsonLdOptions(JsonLdOptions.JSON_LD_1_1);

	@SuppressWarnings("unchecked")
	public static Uni<RestResponse<Object>> subscribeRest(SubscriptionCRUDService subscriptionService,
			HttpServerRequest request, String payload, String baseUrl, Logger logger) {
		logger.trace("subscribeRest() :: started");
		return HttpUtils.getAtContext(request).onItem().transformToUni(linkHeaders -> {
			boolean atContextAllowed;
			try {
				atContextAllowed = HttpUtils.doPreflightCheck(request, linkHeaders);
			} catch (ResponseException e) {
				return Uni.createFrom().failure(e);
			}
			List<Object> context = new ArrayList<Object>();
			context.addAll(linkHeaders);
			Map<String, Object> body;
			try {
				body = ((Map<String, Object>) JsonUtils.fromString(payload));
			} catch (IOException e) {
				return Uni.createFrom().failure(e);
			}
			Object bodyContext = body.get(JsonLdConsts.CONTEXT);
			try {
				body = (Map<String, Object>) JsonLdProcessor
						.expand(linkHeaders, body, opts, AppConstants.SUBSCRIPTION_CREATE_PAYLOAD, atContextAllowed)
						.get(0);
			} catch (JsonLdError | ResponseException e) {
				return Uni.createFrom().failure(e);
			}
			if (bodyContext != null) {
				if (bodyContext instanceof List) {
					context.addAll((List<Object>) bodyContext);
				} else {
					context.add(bodyContext);
				}
			}
			Subscription subscription;
			try {
				subscription = Subscription.expandSubscription(body,
						JsonLdProcessor.getCoreContextClone().parse(context, true), false);
			} catch (JsonLdError | ResponseException e) {
				return Uni.createFrom().failure(e);
			}
			if (subscription.isActive() == null) {
				subscription.setActive(true);
			}
			SubscriptionRequest subRequest = new SubscriptionRequest(subscription, context,
					HttpUtils.getHeaders(request), AppConstants.CREATE_REQUEST);
			return subscriptionService.subscribe(subRequest);
		}).onItem().transform(Unchecked.function(t -> {
			logger.trace("subscribeRest() :: completed");
			return RestResponse.created(new URI(baseUrl + t));
		})).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);
	}

	public static Uni<RestResponse<Object>> getAllSubscriptions(SubscriptionCRUDService subscriptionService,
			HttpServerRequest request, int defaultLimit, int maxLimit, Logger logger) {

		logger.trace("getAllSubscriptions() :: started");
		MultiMap params = request.params();
		QueryParams qp;
		try {
			qp = ParamsResolver.getQueryParamsFromUriQuery(params, JsonLdProcessor.getCoreContextClone(), false, false,
					defaultLimit, maxLimit);
		} catch (ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
		}
		int limit = qp.getLimit();
		int offset = qp.getOffSet();
		if (limit > maxLimit) {
			return Uni.createFrom()
					.item(HttpUtils.handleControllerExceptions(new ResponseException(ErrorType.BadRequestData,
							"provided limit exceeds the max limit of " + maxLimit)));
		}
		if (limit < 0 || offset < 0) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(
					new ResponseException(ErrorType.BadRequestData, "offset and limit can not smaller than 0")));
		}
		int actualLimit;
		if (limit == 0) {
			actualLimit = defaultLimit;
		} else {
			actualLimit = limit;
		}
		boolean count = qp.getCountResult();

		return subscriptionService.getAllSubscriptions(HttpUtils.getInternalTenant(request))
				.onItem().transformToUni(result -> {
					int toIndex = offset + actualLimit;
					ArrayList<Object> additionalLinks = new ArrayList<Object>();
					if (limit == 0 || toIndex > result.size() - 1) {
						toIndex = result.size();
						if (toIndex < 0) {
							toIndex = 0;
						}

					} else {
						additionalLinks
								.add(HttpUtils.generateFollowUpLinkHeader(request, toIndex, actualLimit, null, "next"));
					}

					if (offset > 0) {
						int newOffSet = offset - actualLimit;
						if (newOffSet < 0) {
							newOffSet = 0;
						}
						additionalLinks.add(
								HttpUtils.generateFollowUpLinkHeader(request, newOffSet, actualLimit, null, "prev"));
					}

					ArrayListMultimap<String, String> additionalHeaders = ArrayListMultimap.create();
					if (count == true) {
						additionalHeaders.put(NGSIConstants.COUNT_HEADER_RESULT, String.valueOf(result.size()));
					}
					if (!additionalLinks.isEmpty()) {
						for (Object entry : additionalLinks) {
							additionalHeaders.put(HttpHeaders.LINK, (String) entry);
						}
					}
					List<SubscriptionRequest> realResult = result.subList(offset, toIndex);
					logger.trace("getAllSubscriptions() :: completed");

					return HttpUtils.generateReply(request, getSubscriptions(realResult), additionalHeaders,
							AppConstants.SUBSCRIPTION_ENDPOINT, null);
				}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);

	}

	private static List<Map<String, Object>> getSubscriptions(List<SubscriptionRequest> subRequests) {
		List<Map<String, Object>> result = Lists.newArrayList();
		for (SubscriptionRequest subRequest : subRequests) {
			result.add(subRequest.getSubscription().toJson());
		}
		return result;
	}

	public static Uni<RestResponse<Object>> getSubscriptionById(SubscriptionCRUDService subscriptionService,
			HttpServerRequest request, String id, int limit, Logger logger) {
		return HttpUtils.validateUri(id).onItem().transformToUni(t -> {
			logger.trace("call getSubscriptions() ::");

			return subscriptionService.getSubscription(id, HttpUtils.getInternalTenant(request));
		}).onItem().transformToUni(t -> {
			return HttpUtils.generateReply(request, t.getSubscription().toJson(), AppConstants.SUBSCRIPTION_ENDPOINT,
					null);
		}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);

	}

	public static Uni<RestResponse<Object>> deleteSubscription(SubscriptionCRUDService subscriptionService,
			HttpServerRequest request, String id, Logger logger) {
		logger.trace("call deleteSubscription() ::");
		return HttpUtils.validateUri(id).onItem()
				.transformToUni(t -> subscriptionService.unsubscribe(id,
						HttpUtils.getInternalTenant(request)))
				.onItem().transform(t -> RestResponse.noContent()).onFailure()
				.recoverWithItem(HttpUtils::handleControllerExceptions);

	}

	@SuppressWarnings("unchecked")
	public static Uni<RestResponse<Object>> updateSubscription(SubscriptionCRUDService subscriptionService,
			HttpServerRequest request, String id, String payload, Logger logger) {
		logger.trace("call updateSubscription() ::");
		return HttpUtils.validateUri(id).onItem().transformToUni(t -> HttpUtils.getAtContext(request)).onItem()
				.transformToUni(linkHeaders -> {
					boolean atContextAllowed;
					try {
						atContextAllowed = HttpUtils.doPreflightCheck(request, linkHeaders);
					} catch (ResponseException e) {
						return Uni.createFrom().failure(e);
					}
					List<Object> context = new ArrayList<Object>();
					context.addAll(linkHeaders);
					Map<String, Object> body;
					try {
						body = ((Map<String, Object>) JsonUtils.fromString(payload));
					} catch (Exception e) {
						return Uni.createFrom().failure(e);
					}
					Object bodyContext = body.get(JsonLdConsts.CONTEXT);
					try {
						body = (Map<String, Object>) JsonLdProcessor.expand(linkHeaders, body, opts,
								AppConstants.SUBSCRIPTION_UPDATE_PAYLOAD, atContextAllowed).get(0);
					} catch (ResponseException e) {
						return Uni.createFrom().failure(e);
					}

					if (bodyContext != null) {
						if (bodyContext instanceof List) {
							context.addAll((List<Object>) bodyContext);
						} else {
							context.add(bodyContext);
						}
					}
					Subscription subscription;
					try {
						subscription = Subscription.expandSubscription(body,
								JsonLdProcessor.getCoreContextClone().parse(context, true), true);
					} catch (Exception e) {
						return Uni.createFrom().failure(e);
					}
					if (subscription.getId() == null) {
						subscription.setId(id);
					}
					SubscriptionRequest subscriptionRequest = new SubscriptionRequest(subscription, linkHeaders,
							HttpUtils.getHeaders(request), AppConstants.UPDATE_REQUEST);
					if (body == null || subscription == null || !id.equals(subscription.getId())) {
						return Uni.createFrom().item(HttpUtils.handleControllerExceptions(
								new ResponseException(ErrorType.BadRequestData, "empty subscription body")));
					}
					return subscriptionService.updateSubscription(subscriptionRequest).onItem()
							.transform(t -> RestResponse.noContent());
				}).onFailure().recoverWithItem(t -> HttpUtils.handleControllerExceptions(t));

	}

}
