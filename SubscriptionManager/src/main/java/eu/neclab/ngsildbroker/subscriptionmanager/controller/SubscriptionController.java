package eu.neclab.ngsildbroker.subscriptionmanager.controller;

import java.util.Map;

import javax.annotation.PostConstruct;
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

import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLdProcessor;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.subscriptionmanager.service.SubscriptionService;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
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

	@PostConstruct
	public void init() {
		JsonLdProcessor.init(coreContext);
	}

	@POST
	public Uni<RestResponse<Object>> subscribe(HttpServerRequest request, String payload) {
		Tuple2<Context, Map<String, Object>> tuple;
		try {
			tuple = HttpUtils.expandBody(request, payload, AppConstants.SUBSCRIPTION_CREATE_PAYLOAD);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
		}
		return subService.createSubscription(HttpUtils.getTenant(request), tuple.getItem2(), tuple.getItem1()).onItem()
				.transform(t -> HttpUtils.generateSubscriptionResult(t, tuple.getItem1())).onFailure()
				.recoverWithItem(e -> HttpUtils.handleControllerExceptions(e));
	}

	@GET
	public Uni<RestResponse<Object>> getAllSubscriptions(HttpServerRequest request, @QueryParam("limit") Integer limit,
			@QueryParam("limit") int offset, @QueryParam("options") String options) {
		int acceptHeader = HttpUtils.parseAcceptHeader(request.headers().getAll("Accept"));
		if (acceptHeader == -1) {
			return HttpUtils.INVALID_HEADER;
		}
		if (limit == null) {
			limit = defaultLimit;
		}
		if (limit > maxLimit) {
			return Uni.createFrom()
					.item(HttpUtils.handleControllerExceptions(new ResponseException(ErrorType.TooManyResults)));
		}
		if (offset < 0) {
			return Uni.createFrom().item(HttpUtils
					.handleControllerExceptions(new ResponseException(ErrorType.InvalidRequest, "invalid offset")));
		}

		return subService.getAllSubscriptions(HttpUtils.getTenant(request), limit, offset).onItem()
				.transform(subscriptions -> {

					return HttpUtils.generateQueryResult(subscriptions, acceptHeader,
							JsonLdProcessor.getCoreContextClone().parse(HttpUtils.getAtContext(request), true));
				});

	}

	@Path("/{id}")
	@GET
	public Uni<RestResponse<Object>> getSubscriptionById(HttpServerRequest request,
			@PathParam(value = "id") String subscriptionId, @QueryParam(value = "options") String options) {
		int acceptHeader = HttpUtils.parseAcceptHeader(request.headers().getAll("Accept"));
		if (acceptHeader == -1) {
			return HttpUtils.INVALID_HEADER;
		}
		return subService.getSubscription(HttpUtils.getTenant(request), subscriptionId).onItem()
				.transform(subscription -> {
					return HttpUtils.generateSubscriptionResult(subscription, acceptHeader,
							JsonLdProcessor.getCoreContextClone().parse(HttpUtils.getAtContext(request), true));
				});

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
			String payload) {
		Tuple2<Context, Map<String, Object>> tuple;
		try {
			tuple = HttpUtils.expandBody(request, payload, AppConstants.SUBSCRIPTION_UPDATE_PAYLOAD);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
		}
		return subService.updateSubscription(HttpUtils.getTenant(request), id, tuple.getItem2(), tuple.getItem1()).onItem()
				.transform(t -> HttpUtils.generateSubscriptionResult(t, tuple.getItem1())).onFailure()
				.recoverWithItem(e -> HttpUtils.handleControllerExceptions(e));
		
	}


}
