package eu.neclab.ngsildbroker.subscriptionmanager.controller;

import java.io.IOException;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.resteasy.reactive.RestResponse;

import com.fasterxml.jackson.core.JsonParseException;
import com.github.jsonldjava.core.JsonLdError;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.subscriptionmanager.service.SubscriptionService;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.unchecked.Unchecked;
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
	private JsonLdOptions opts = new JsonLdOptions(JsonLdOptions.JSON_LD_1_1);

	@Path("/{id}")
	@POST
	public Uni<RestResponse<Object>> notify(HttpServerRequest req, String payload,
			@PathParam(value = NGSIConstants.QUERY_PARAMETER_ID) String id) {
		return HttpUtils.getAtContext(req).onItem().transformToUni(t -> {
			try {
				subscriptionManager
						.remoteNotify(id,
								(Map<String, Object>) JsonLdProcessor
										.expand(t, JsonUtils.fromString(payload), opts,
												AppConstants.NOTIFICAITION_RECEIVED, HttpUtils.doPreflightCheck(req, t))
										.get(0));
			} catch (Exception e) {
				return Uni.createFrom().failure(e);
			}
			return Uni.createFrom().item(RestResponse.ok());
		}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);

	}

}
