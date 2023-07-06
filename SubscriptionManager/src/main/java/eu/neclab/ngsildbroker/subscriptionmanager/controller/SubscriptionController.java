package eu.neclab.ngsildbroker.subscriptionmanager.controller;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PATCH;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.resteasy.reactive.RestResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.jsonldjava.core.JsonLDService;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.subscriptionmanager.service.SubscriptionService;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpServerRequest;

@Path("/ngsi-ld/v1/subscriptions")
public class SubscriptionController {

	private final static Logger logger = LoggerFactory.getLogger(SubscriptionController.class);

	@Inject
	SubscriptionService subService;

	@ConfigProperty(name = "ngsild.corecontext", defaultValue = "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld")
	String coreContext;

	@ConfigProperty(name = "scorpio.subscription.default-limit", defaultValue = "50")
	int defaultLimit;
	@ConfigProperty(name = "scorpio.subscription.max-limit", defaultValue = "1000")
	int maxLimit;

	@Inject
	JsonLDService ldService;

	@POST
	public Uni<RestResponse<Object>> subscribe(HttpServerRequest request, Map<String, Object> map) {
		return HttpUtils.expandBody(request, map, AppConstants.SUBSCRIPTION_CREATE_PAYLOAD, ldService).onItem()
				.transformToUni(tuple -> {
					return subService
							.createSubscription(HttpUtils.getTenant(request), tuple.getItem2(), tuple.getItem1())
							.onItem().transform(t -> HttpUtils.generateSubscriptionResult(t, tuple.getItem1()))
							.onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);
				});
	}

	@GET
	public Uni<RestResponse<Object>> getAllSubscriptions(HttpServerRequest request, @QueryParam("limit") Integer limit,
			@QueryParam("limit") int offset, @QueryParam("options") String options) {
		int acceptHeader = HttpUtils.parseAcceptHeader(request.headers().getAll("Accept"));
		if (acceptHeader == -1) {
			return HttpUtils.getInvalidHeader();
		}
		int actualLimit;
		if (limit == null) {
			actualLimit = defaultLimit;
		} else {
			actualLimit = limit;
		}
		if (actualLimit > maxLimit) {
			return Uni.createFrom()
					.item(HttpUtils.handleControllerExceptions(new ResponseException(ErrorType.TooManyResults)));
		}
		if (offset < 0) {
			return Uni.createFrom().item(HttpUtils
					.handleControllerExceptions(new ResponseException(ErrorType.InvalidRequest, "invalid offset")));
		}
		return ldService.parse(HttpUtils.getAtContext(request)).onItem().transformToUni(ctx -> {
			return subService.getAllSubscriptions(HttpUtils.getTenant(request), actualLimit, offset).onItem()
					.transformToUni(subscriptions -> {
						return HttpUtils.generateQueryResult(request, subscriptions, options, null, acceptHeader, false,
								actualLimit, null, ctx, ldService);
					});
		});

	}

	@Path("/{id}")
	@GET
	public Uni<RestResponse<Object>> getSubscriptionById(HttpServerRequest request,
			@PathParam(value = "id") String subscriptionId, @QueryParam(value = "options") String options) {
		int acceptHeader = HttpUtils.parseAcceptHeader(request.headers().getAll("Accept"));
		if (acceptHeader == -1) {
			return HttpUtils.getInvalidHeader();
		}
		List<Object> contextHeader = HttpUtils.getAtContext(request);
		return ldService.parse(contextHeader).onItem().transformToUni(context -> {
			return subService.getSubscription(HttpUtils.getTenant(request), subscriptionId).onItem()
					.transformToUni(subscription -> {
						return HttpUtils.generateEntityResult(contextHeader, context, acceptHeader, subscription, null,
								options, null, ldService);
					});
		}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);
	}

	@Path("/{id}")
	@DELETE
	public Uni<RestResponse<Object>> deleteSubscription(HttpServerRequest request, @PathParam(value = "id") String id) {
		return subService.deleteSubscription(HttpUtils.getTenant(request), id).onItem()
				.transform(t -> HttpUtils.generateDeleteResult(t));

	}

	@Path("/{id}")
	@PATCH
	public Uni<RestResponse<Object>> updateSubscription(HttpServerRequest request, @PathParam(value = "id") String id,
			Map<String, Object> map) {
		List<String> contexts = (List<String>) map.get("@context");
		List<String> finalContexts = new ArrayList<>();
		if (contexts != null) {
			for (String url : contexts) {
				url = url + "?type=implicitlyCreated";
				finalContexts.add(url);
			}
			map.put("@context", finalContexts);
		}
		return HttpUtils.expandBody(request, map, AppConstants.SUBSCRIPTION_UPDATE_PAYLOAD, ldService).onItem()
				.transformToUni(tuple -> {
					return subService
							.updateSubscription(HttpUtils.getTenant(request), id, tuple.getItem2(), tuple.getItem1())
							.onItem().transform(t -> HttpUtils.generateSubscriptionResult(t, tuple.getItem1()))
							.onFailure().recoverWithItem(e -> HttpUtils.handleControllerExceptions(e));
				});

	}

}
