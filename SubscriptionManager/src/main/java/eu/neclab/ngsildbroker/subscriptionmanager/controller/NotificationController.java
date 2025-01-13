package eu.neclab.ngsildbroker.subscriptionmanager.controller;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.resteasy.reactive.RestResponse;

import com.github.jsonldjava.core.JsonLDService;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.subscriptionmanager.service.SubscriptionService;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpServerRequest;

@Singleton
@Path("/remotenotify")
public class NotificationController {

	@ConfigProperty(name = "ngsild.corecontext", defaultValue = "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld")
	String coreContext;

	@Inject
	JsonLDService ldService;

	@Inject
	SubscriptionService subscriptionManager;

	@Path("/{id}")
	@POST
	public Uni<RestResponse<Object>> notify(HttpServerRequest request, String payload,
			@PathParam(value = NGSIConstants.QUERY_PARAMETER_ID) String id) {
		return HttpUtils.expandBody(request, payload, -1, ldService).onItem()
				.transformToUni(tuple -> {
					return subscriptionManager.remoteNotify(id, tuple.getItem2(), tuple.getItem1()).onItem()
							.transform(v -> RestResponse.ok());
				}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);

	}

}
