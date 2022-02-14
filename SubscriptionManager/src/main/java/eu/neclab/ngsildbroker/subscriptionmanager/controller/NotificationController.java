package eu.neclab.ngsildbroker.subscriptionmanager.controller;

import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.resteasy.reactive.RestResponse;

import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.subscriptionmanager.service.SubscriptionService;
import io.vertx.core.http.HttpServerRequest;

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

	@SuppressWarnings("unchecked")
	@Path("/{id}")
	@POST
	public RestResponse<Object> notify(HttpServerRequest req, String payload, String id) {
		try {
			List<Object> atContextLinks = HttpUtils.getAtContext(req);
			subscriptionManager.remoteNotify(id,
					(Map<String, Object>) JsonLdProcessor.expand(atContextLinks, JsonUtils.fromString(payload), opts,
							AppConstants.NOTIFICAITION_RECEIVED, HttpUtils.doPreflightCheck(req, atContextLinks))
							.get(0));
		} catch (Exception e) {
			return HttpUtils.handleControllerExceptions(e);
		}
		return RestResponse.ok();

	}

}
