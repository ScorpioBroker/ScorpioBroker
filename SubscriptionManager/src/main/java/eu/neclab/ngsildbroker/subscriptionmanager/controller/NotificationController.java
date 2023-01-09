package eu.neclab.ngsildbroker.subscriptionmanager.controller;

import java.util.Map;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.resteasy.reactive.RestResponse;

import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLdProcessor;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.subscriptionmanager.service.SubscriptionService;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.vertx.core.http.HttpServerRequest;

@Singleton
@Path("/remotenotify")
public class NotificationController {

	@ConfigProperty(name = "ngsild.corecontext", defaultValue = "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld")
	String coreContext;

	@PostConstruct
	public void init() {
		JsonLdProcessor.init(coreContext);
	}

	@Inject
	SubscriptionService subscriptionManager;

	@Path("/{id}")
	@POST
	public Uni<RestResponse<Object>> notify(HttpServerRequest request, String payload,
			@PathParam(value = NGSIConstants.QUERY_PARAMETER_ID) String id) {
		Tuple2<Context, Map<String, Object>> tuple;
		try {
			tuple = HttpUtils.expandBody(request, payload, AppConstants.NOTIFICAITION_RECEIVED);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
		}
		return subscriptionManager.remoteNotify(id, tuple.getItem2(), tuple.getItem1()).onItem()
				.transform(v -> RestResponse.ok()).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);

	}

}
