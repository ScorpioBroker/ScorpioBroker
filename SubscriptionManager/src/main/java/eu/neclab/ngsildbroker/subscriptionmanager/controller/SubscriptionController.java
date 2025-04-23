package eu.neclab.ngsildbroker.subscriptionmanager.controller;

import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLDService;
import com.github.jsonldjava.core.JsonLdConsts;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.Lists;
import com.google.common.net.HttpHeaders;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.ViaHeaders;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import eu.neclab.ngsildbroker.subscriptionmanager.service.SubscriptionService;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.impl.headers.HeadersMultiMap;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Inject;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.PATCH;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.QueryParam;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.resteasy.reactive.RestResponse;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Path("/ngsi-ld/v1/subscriptions")
public class SubscriptionController {

	// private final static Logger logger =
	// LoggerFactory.getLogger(SubscriptionController.class);

	@Inject
	SubscriptionService subService;

	@Inject
	MicroServiceUtils microServiceUtils;

	@ConfigProperty(name = "ngsild.corecontext", defaultValue = "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld")
	String coreContext;

	@ConfigProperty(name = "scorpio.subscription.default-limit", defaultValue = "50")
	int defaultLimit;
	@ConfigProperty(name = "scorpio.subscription.max-limit", defaultValue = "1000")
	int maxLimit;

	@Inject
	JsonLDService ldService;

	private String selfViaHeader;

	@PostConstruct
	public void setup() {
		URI gateway = microServiceUtils.getGatewayURL();
		this.selfViaHeader = gateway.getScheme().toUpperCase() + "/1.1 " + gateway.getAuthority();
	}

	@SuppressWarnings("unchecked")
	@POST
	public Uni<RestResponse<Object>> subscribe(HttpServerRequest request, String body) {
		Map<String, Object> map;

		try {
			map = new JsonObject(body).getMap();
		} catch (DecodeException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
		}

//		try {
//			if (!map.containsKey(NGSIConstants.JSONLD_CONTEXT)) {
//				Object contextLink;
//				if (request.getHeader(NGSIConstants.LINK_HEADER) != null) {
//					contextLink = request.getHeader(NGSIConstants.LINK_HEADER).split(";")[0].replace("<", "")
//							.replace(">", "");
//				} else if (map.containsKey(JsonLdConsts.CONTEXT)) {
//					contextLink = map.get(JsonLdConsts.CONTEXT);
//				} else {
//					contextLink = coreContext;
//				}
//				map.put(NGSIConstants.JSONLD_CONTEXT, contextLink);
//			}
//		} catch (Exception e) {
//			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(
//					new ResponseException(ErrorType.BadRequestData), HttpUtils.getTenant(request)));
//		}
		HeadersMultiMap otherHead = new HeadersMultiMap();
		if (request.headers().contains(NGSIConstants.TENANT_HEADER)) {
			otherHead.add(NGSIConstants.TENANT_HEADER, request.headers().get(NGSIConstants.TENANT_HEADER));
		}

		ViaHeaders viaHeaders = new ViaHeaders(request.headers().getAll(HttpHeaders.VIA), this.selfViaHeader);
		
		return HttpUtils.expandBody(request, map, AppConstants.SUBSCRIPTION_CREATE_PAYLOAD, ldService).onItem()
				.transformToUni(tuple -> {
					Uni<Context> contextLink;
					if (map.containsKey(NGSIConstants.JSONLD_CONTEXT)) {
						contextLink = ldService.parse(map.get(NGSIConstants.JSONLD_CONTEXT));
					} else {
						contextLink = Uni.createFrom().item(tuple.getItem1());
					}
					return contextLink.onItem().transformToUni(ctx -> {
						return subService
								.createSubscription(otherHead, HttpUtils.getTenant(request), tuple.getItem2(),
										ctx, viaHeaders)
								.onItem().transform(t -> HttpUtils.generateSubscriptionResult(t, tuple.getItem1()));
					});
				}).onFailure().recoverWithItem(e -> {
					return HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request));
				});
	}

	@GET
	public Uni<RestResponse<Object>> getAllSubscriptions(HttpServerRequest request, @QueryParam("limit") Integer limit,
			@QueryParam("offset") int offset, @QueryParam("options") String options) {
		int acceptHeader = HttpUtils.parseAcceptHeader(request.headers().getAll("Accept"));
		if (acceptHeader != 1 && acceptHeader != 2) {
			return HttpUtils.getInvalidHeader();
		}
		int actualLimit;
		if (limit == null) {
			actualLimit = defaultLimit;
		} else {
			actualLimit = limit;
		}
		if (actualLimit > maxLimit) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(
					new ResponseException(ErrorType.TooManyResults), HttpUtils.getTenant(request)));
		}
		if (offset < 0) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(
					new ResponseException(ErrorType.InvalidRequest, "invalid offset"), HttpUtils.getTenant(request)));
		}
		return ldService.parse(HttpUtils.getAtContext(request)).onItem().transformToUni(ctx -> {
			return subService.getAllSubscriptions(HttpUtils.getTenant(request), actualLimit, offset).onItem()
					.transformToUni(subscriptions -> {
						subscriptions.getData().forEach(sub -> {
							fixSub(sub);
						});
						return HttpUtils.generateQueryResult(request, subscriptions, options, null, acceptHeader, false,
								actualLimit, null, ctx, ldService, false, microServiceUtils.getGatewayURL().toString(),
								NGSIConstants.NGSI_LD_SUB_ENDPOINT);
					});
		}).onFailure().recoverWithItem(e -> {
			return HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request));
		});

	}

	private void fixSub(Map<String, Object> sub) {

		Map<String, Object> notificationParam = ((List<Map<String, Object>>) sub
				.get(NGSIConstants.NGSI_LD_NOTIFICATION)).get(0);
		notificationParam.put(NGSIConstants.NGSI_LD_TIMES_SENT, sub.remove(NGSIConstants.NGSI_LD_TIMES_SENT));
		notificationParam.put(NGSIConstants.NGSI_LD_TIMES_FAILED, sub.remove(NGSIConstants.NGSI_LD_TIMES_FAILED));
		Object lastNotification = sub.remove(NGSIConstants.NGSI_LD_LAST_NOTIFICATION);
		Object lastSuccess = sub.remove(NGSIConstants.NGSI_LD_LAST_SUCCESS);
		Object lastFailure = sub.remove(NGSIConstants.NGSI_LD_LAST_FAILURE);
		if (lastNotification != null) {
			notificationParam.put(NGSIConstants.NGSI_LD_LAST_NOTIFICATION, lastNotification);
			if (lastSuccess != null) {
				if (lastSuccess.equals(lastNotification)) {
					notificationParam.put(NGSIConstants.STATUS, "ok");
				}
				notificationParam.put(NGSIConstants.NGSI_LD_LAST_SUCCESS, lastSuccess);
			}
			if (lastFailure != null) {
				if (lastFailure.equals(lastNotification)) {
					notificationParam.put(NGSIConstants.STATUS, "failed");
				}
				notificationParam.put(NGSIConstants.NGSI_LD_LAST_FAILURE, lastFailure);
			}
		} else {
			notificationParam.put(NGSIConstants.STATUS, "ok");
		}

	}

	@Path("/{id}")
	@GET
	public Uni<RestResponse<Object>> getSubscriptionById(HttpServerRequest request,
			@PathParam(value = "id") String subscriptionId, @QueryParam(value = "options") String options) {
		int acceptHeader = HttpUtils.parseAcceptHeader(request.headers().getAll("Accept"));
		if (acceptHeader != 1 && acceptHeader != 2) {
			return HttpUtils.getInvalidHeader();
		}
		try {
			HttpUtils.validateUri(subscriptionId);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
		}
		List<Object> contextHeader = HttpUtils.getAtContext(request);
		return ldService.parse(contextHeader).onItem().transformToUni(context -> {
			return subService.getSubscription(HttpUtils.getTenant(request), subscriptionId).onItem()
					.transformToUni(subscription -> {
						fixSub(subscription);
						return HttpUtils.generateEntityResult(contextHeader, context, acceptHeader, subscription, null,
								options, null, ldService, null, null, true);
					});
		}).onFailure().recoverWithItem(e -> {
			return HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request));
		});
	}

	@Path("/{id}")
	@DELETE
	public Uni<RestResponse<Object>> deleteSubscription(HttpServerRequest request, @PathParam(value = "id") String id) {
		try {
			HttpUtils.validateUri(id);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
		}
		return subService.deleteSubscription(HttpUtils.getTenant(request), id).onItem()
				.transform(t -> HttpUtils.generateDeleteResult(t)).onFailure().recoverWithItem(e -> {
					return HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request));
				});

	}

	@Path("/{id}")
	@PATCH
	public Uni<RestResponse<Object>> updateSubscription(HttpServerRequest request, @PathParam(value = "id") String id,
			String body) {
		Map<String, Object> map;

		try {
			map = new JsonObject(body).getMap();
		} catch (DecodeException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
		}
		try {
			HttpUtils.validateUri(id);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
		}
		@SuppressWarnings("unchecked")
		List<String> contexts;
		Object ctxObj = map.get("@context");
		if (ctxObj instanceof List) {
			contexts = (List<String>) ctxObj;
		} else if (ctxObj instanceof String s) {
			contexts = Lists.newArrayList(s);
		} else {
			contexts = null;
		}
		List<String> finalContexts = new ArrayList<>();
		if (contexts != null) {
			for (String url : contexts) {
				url = url + "?type=implicitlyCreated";
				finalContexts.add(url);
			}
			map.put("@context", finalContexts);
		}
		ViaHeaders viaHeaders = new ViaHeaders(request.headers().getAll(HttpHeaders.VIA), this.selfViaHeader);
		return HttpUtils.expandBody(request, map, AppConstants.SUBSCRIPTION_UPDATE_PAYLOAD, ldService).onItem()
				.transformToUni(tuple -> {
					return subService
							.updateSubscription(HttpUtils.getTenant(request), id, tuple.getItem2(), tuple.getItem1(),
									viaHeaders)
							.onItem().transform(t -> HttpUtils.generateSubscriptionResult(t, tuple.getItem1()));
				}).onFailure().recoverWithItem(e -> {
					return HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request));
				});

	}
}
