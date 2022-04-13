package eu.neclab.ngsildbroker.subscriptionmanager.controller;

import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.subscriptionmanager.service.SubscriptionService;

@RestController
@RequestMapping("/remotenotify")
public class NotificationController {

	@Value("${ngsild.corecontext:https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld}")
	String coreContext;

	@PostConstruct
	public void init() {
		JsonLdProcessor.init(coreContext);
	}

	@Autowired
	SubscriptionService subscriptionManager;
	private JsonLdOptions opts = new JsonLdOptions(JsonLdOptions.JSON_LD_1_1);

	@RequestMapping(method = RequestMethod.POST, value = "/{id}")
	public ResponseEntity<String> notify(HttpServletRequest req, @RequestBody String payload,
			@PathVariable(name = NGSIConstants.QUERY_PARAMETER_ID, required = false) String id) {
		try {
			List<Object> atContextLinks = HttpUtils.getAtContext(req);
			subscriptionManager.remoteNotify(id,
					(Map<String, Object>) JsonLdProcessor.expand(atContextLinks, JsonUtils.fromString(payload), opts,
							AppConstants.NOTIFICAITION_RECEIVED, HttpUtils.doPreflightCheck(req, atContextLinks))
							.get(0));
		} catch (Exception e) {
			return HttpUtils.handleControllerExceptions(e);
		}
		return ResponseEntity.ok().build();

	}

}
