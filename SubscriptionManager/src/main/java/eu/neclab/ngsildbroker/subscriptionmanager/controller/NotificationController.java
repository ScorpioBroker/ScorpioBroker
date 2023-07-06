package eu.neclab.ngsildbroker.subscriptionmanager.controller;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.resteasy.reactive.RestResponse;

import com.github.jsonldjava.core.JsonLDService;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
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
		return HttpUtils.expandBody(request, payload, AppConstants.NOTIFICAITION_RECEIVED, ldService).onItem()
				.transformToUni(tuple -> {
					return subscriptionManager.remoteNotify(id, tuple.getItem2(), tuple.getItem1()).onItem()
							.transform(v -> RestResponse.ok()).onFailure()
							.recoverWithItem(HttpUtils::handleControllerExceptions);
				});

	}

}
