package eu.neclab.ngsildbroker.registrysubscriptionmanager.controller;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.github.jsonldjava.core.JsonLdProcessor;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.controllers.SubscriptionControllerFunctions;
import eu.neclab.ngsildbroker.registrysubscriptionmanager.service.RegistrySubscriptionService;

@RestController
@RequestMapping("/ngsi-ld/v1/csourceSubscriptions")
public class RegistrySubscriptionController {

	private final static Logger logger = LoggerFactory.getLogger(RegistrySubscriptionController.class);

	@Autowired
	RegistrySubscriptionService manager;

	@Value("${ngsild.corecontext:https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld}")
	String coreContext;

	@Value("${scorpio.subscription.default-limit:50}")
	private int defaultLimit;
	@Value("${scorpio.subscription.max-limit:1000}")
	private int maxLimit;

	@PostConstruct
	public void init() {
		JsonLdProcessor.init(coreContext);
	}

	@PostMapping
	public ResponseEntity<String> subscribeRest(HttpServletRequest request, @RequestBody String payload) {
		return SubscriptionControllerFunctions.subscribeRest(manager, request, payload,
				AppConstants.CSOURCE_SUBSCRIPTIONS_URL, logger);
	}

	@GetMapping
	public ResponseEntity<String> getAllSubscriptions(HttpServletRequest request) {
		return SubscriptionControllerFunctions.getAllSubscriptions(manager, request, defaultLimit, maxLimit, logger);
	}

	@GetMapping("/{id}")
	public ResponseEntity<String> getSubscriptionById(HttpServletRequest request,
			@PathVariable(name = "id", required = true) String id,
			@RequestParam(required = false, name = "limit", defaultValue = "0") int limit) {
		return SubscriptionControllerFunctions.getSubscriptionById(manager, request, id, limit, logger);

	}

	@DeleteMapping("/{id}")
	public ResponseEntity<String> deleteSubscription(HttpServletRequest request,
			@PathVariable(name = "id", required = true) String id) {
		return SubscriptionControllerFunctions.deleteSubscription(manager, request, id, logger);
	}

	@PatchMapping("/{id}")
	public ResponseEntity<String> updateSubscription(HttpServletRequest request,
			@PathVariable(name = "id", required = true) String id, @RequestBody String payload) {
		return SubscriptionControllerFunctions.updateSubscription(manager, request, id, payload, logger);
	}

}
