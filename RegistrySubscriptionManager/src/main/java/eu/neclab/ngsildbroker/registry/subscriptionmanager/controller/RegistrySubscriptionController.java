package eu.neclab.ngsildbroker.registry.subscriptionmanager.controller;

import java.util.List;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.PATCH;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.QueryParam;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.resteasy.reactive.RestResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.jsonldjava.core.JsonLDService;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.registry.subscriptionmanager.service.RegistrySubscriptionService;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpServerRequest;

@Singleton
@Path("/ngsi-ld/v1/csourceSubscriptions")
public class RegistrySubscriptionController {

	private final static Logger logger = LoggerFactory.getLogger(RegistrySubscriptionController.class);

	@Inject
	RegistrySubscriptionService subService;

	@ConfigProperty(name = "ngsild.corecontext", defaultValue = "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld")
	String coreContext;

	@ConfigProperty(name = "scorpio.subscription.default-limit", defaultValue = "50")
	int defaultLimit;
	@ConfigProperty(name = "scorpio.subscription.max-limit", defaultValue = "1000")
	int maxLimit;

	@Inject
	JsonLDService ldService;

	@POST
	public Uni<RestResponse<Object>> subscribe(HttpServerRequest request, String payload) {
		return HttpUtils.expandBody(request, payload, AppConstants.SUBSCRIPTION_CREATE_PAYLOAD, ldService).onItem()
				.transformToUni(tuple -> {
					return subService
							.createSubscription(HttpUtils.getTenant(request), tuple.getItem2(), tuple.getItem1())
							.onItem().transform(t -> HttpUtils.generateSubscriptionResult(t, tuple.getItem1()));
				}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);
	}

	@GET
	public Uni<RestResponse<Object>> getAllSubscriptions(HttpServerRequest request, @QueryParam("limit") Integer limit,
			@QueryParam("offset") int offset, @QueryParam("options") String options) {
		int acceptHeader = HttpUtils.parseAcceptHeader(request.headers().getAll("Accept"));
		if (acceptHeader == -1) {
			return HttpUtils.getInvalidHeader();
		}
		int limitTBU;
		if (limit == null) {
			limitTBU = defaultLimit;
		} else {
			limitTBU = limit;
		}
		if (limitTBU > maxLimit) {
			return Uni.createFrom()
					.item(HttpUtils.handleControllerExceptions(new ResponseException(ErrorType.TooManyResults)));
		}

		if (offset < 0 || limitTBU < 1) {
			return Uni.createFrom().item(HttpUtils
					.handleControllerExceptions(new ResponseException(ErrorType.BadRequestData, "invalid offset/limit")));
		}
		return ldService.parse(HttpUtils.getAtContext(request)).onItem().transformToUni(ctx -> {
			return subService.getAllSubscriptions(HttpUtils.getTenant(request), limitTBU, offset).onItem()
					.transformToUni(subscriptions -> {
						return HttpUtils.generateQueryResult(request, subscriptions, options, null, acceptHeader, false,
								acceptHeader, null, ctx, ldService,null);
					}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);

		}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);

	}

	@Path("/{id}")
	@GET
	public Uni<RestResponse<Object>> getSubscriptionById(HttpServerRequest request,
			@PathParam(value = "id") String subscriptionId, @QueryParam(value = "options") String options) {
		int acceptHeader = HttpUtils.parseAcceptHeader(request.headers().getAll("Accept"));
		if (acceptHeader == -1) {
			return HttpUtils.getInvalidHeader();
		}
		try {
			HttpUtils.validateUri(subscriptionId);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
		}
		List<Object> contextHeader = HttpUtils.getAtContext(request);
		return ldService.parse(contextHeader).onItem().transformToUni(context -> {
			return subService.getSubscription(HttpUtils.getTenant(request), subscriptionId).onItem()
					.transformToUni(subscription -> {
						return HttpUtils.generateEntityResult(contextHeader, context, acceptHeader, subscription, null,
								options, null, ldService,null);
					});
		}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);

	}

	@Path("/{id}")
	@DELETE
	public Uni<RestResponse<Object>> deleteSubscription(HttpServerRequest request, @PathParam(value = "id") String id) {
		try {
			HttpUtils.validateUri(id);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
		}
		return subService.deleteSubscription(HttpUtils.getTenant(request), id).onItem()
				.transform(t -> HttpUtils.generateDeleteResult(t)).onFailure()
				.recoverWithItem(HttpUtils::handleControllerExceptions);

	}

	@Path("/{id}")
	@PATCH
	public Uni<RestResponse<Object>> updateSubscription(HttpServerRequest request, @PathParam(value = "id") String id,
			String payload) {
		try {
			HttpUtils.validateUri(id);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
		}
		return HttpUtils.expandBody(request, payload, AppConstants.SUBSCRIPTION_UPDATE_PAYLOAD, ldService).onItem()
				.transformToUni(tuple -> {
					return subService
							.updateSubscription(HttpUtils.getTenant(request), id, tuple.getItem2(), tuple.getItem1())
							.onItem().transform(t -> HttpUtils.generateSubscriptionResult(t, tuple.getItem1()));
				}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);

	}

}
