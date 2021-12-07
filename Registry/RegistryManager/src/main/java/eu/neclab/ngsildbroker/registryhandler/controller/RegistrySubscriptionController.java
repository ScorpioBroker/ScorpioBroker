package eu.neclab.ngsildbroker.registryhandler.controller;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.RestResponse;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.datatypes.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;
import eu.neclab.ngsildbroker.commons.stream.service.KafkaOps;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.ValidateURI;
import eu.neclab.ngsildbroker.commons.tools.Validator;
import eu.neclab.ngsildbroker.registryhandler.service.CSourceSubscriptionService;

@RestController
@RequestMapping("/ngsi-ld/v1/csourceSubscriptions")
public class RegistrySubscriptionController {

	private final static Logger logger = LogManager.getLogger(RegistrySubscriptionController.class);

	@Autowired
	CSourceSubscriptionService manager;

	@Autowired
	@Qualifier("rmops")
	KafkaOps kafkaOps;

	@Value("${ngsild.corecontext:https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld}")
	String coreContext;

	@PostConstruct
	public void init() {
		JsonLdProcessor.init(coreContext);
	}

	ResponseException badRequest = new ResponseException(ErrorType.BadRequestData);

	ResponseEntity<byte[]> badRequestResponse = ResponseEntity.status(badRequest.getHttpStatus())
			.body(new RestResponse(badRequest).toJsonBytes());

	private JsonLdOptions opts = new JsonLdOptions(JsonLdOptions.JSON_LD_1_1);
	// @PostConstruct
	// private void setupContextResolver() {
	// this.contextResolver =
	// ContextResolverService.getInstance(producerChannel.atContextWriteChannel(),
	// kafkaOps);
	// }
	// public SubscriptionController(SubscriptionManagerProducerChannel prodChannel)
	// {
	// this.contextResolver = new
	// ContextResolverService(prodChannel.atContextWriteChannel());
	// }

	@PostMapping
	public ResponseEntity<byte[]> subscribeRest(ServerHttpRequest request, @RequestBody String payload) {
		logger.trace("subscribeRest() :: started");
		Subscription subscription = null;
		try {
			List<Object> context = HttpUtils.getAtContext(request);
			String resolved = JsonUtils
					.toString(JsonLdProcessor.expand(HttpUtils.getAtContext(request), JsonUtils.fromString(payload),
							opts, AppConstants.SUBSCRIPTION_CREATE_PAYLOAD, HttpUtils.doPreflightCheck(request)));

			subscription = DataSerializer.getSubscription(resolved);
			if (resolved == null || subscription == null) {
				return badRequestResponse;
			}

			SubscriptionRequest subscriptionRequest = new SubscriptionRequest(subscription, context,
					HttpUtils.getHeaders(request));
			URI subId = manager.subscribe(subscriptionRequest);
			logger.trace("subscribeRest() :: completed");
			// no absolute url only relative url
			return ResponseEntity.created(new URI("/ngsi-ld/v1/csourceSubscriptions/" + subId.toString()))
					.body(subId.toString().getBytes());
		} catch (ResponseException e) {
			logger.error("Exception ::", e);
			return ResponseEntity.status(e.getHttpStatus()).body(new RestResponse(e).toJsonBytes());
		} catch (URISyntaxException e) {
			logger.error("Exception ::", e);
			return ResponseEntity.status(HttpStatus.CONFLICT).body(subscription.getId().toString().getBytes());
		} catch (IOException e) {
			logger.error("Exception ::", e);
			return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(e.getMessage().getBytes());
		}
	}

	@GetMapping
	public ResponseEntity<byte[]> getAllSubscriptions(ServerHttpRequest request,
			@RequestParam(required = false, name = "limit", defaultValue = "0") int limit) throws ResponseException {
		logger.trace("getAllSubscriptions() :: started");
		List<Subscription> result = null;
		result = manager.getAllSubscriptions(HttpUtils.getHeaders(request), limit);
		logger.trace("getAllSubscriptions() :: completed");
		return HttpUtils.generateReply(request, DataSerializer.toJson(result));
	}

	@GetMapping("{id}")
	// (method = RequestMethod.GET, value = "/{id}")
	public ResponseEntity<byte[]> getSubscriptions(ServerHttpRequest request,
			@PathVariable(name = NGSIConstants.QUERY_PARAMETER_ID, required = true) URI id,
			@RequestParam(required = false, name = "limit", defaultValue = "0") int limit) {
		try {
			logger.trace("call getSubscriptions() ::");
			ValidateURI.validateUriInSubs(id);
			return HttpUtils.generateReply(request,
					DataSerializer.toJson(manager.getSubscription(HttpUtils.getHeaders(request), id)));

		} catch (ResponseException e) {
			logger.error("Exception ::", e);
			return ResponseEntity.status(e.getHttpStatus()).body(new RestResponse(e).toJsonBytes());
		}

	}

	@DeleteMapping("{id}")
	@RequestMapping(method = RequestMethod.DELETE, value = "/{id}")
	public ResponseEntity<byte[]> deleteSubscription(ServerHttpRequest request,
			@PathVariable(name = NGSIConstants.QUERY_PARAMETER_ID, required = true) URI id) {
		try {
			ValidateURI.validateUriInSubs(id);
			logger.trace("call deleteSubscription() ::");
			manager.unsubscribe(id, HttpUtils.getHeaders(request));
		} catch (ResponseException e) {
			logger.error("Exception ::", e);
			return ResponseEntity.status(e.getHttpStatus()).body(new RestResponse(e).toJsonBytes());
		}
		return ResponseEntity.noContent().build();
	}

	@PatchMapping("{id}")
	public ResponseEntity<byte[]> updateSubscription(ServerHttpRequest request,
			@PathVariable(name = NGSIConstants.QUERY_PARAMETER_ID, required = true) URI id,
			@RequestBody String payload) {
		logger.trace("call updateSubscription() ::");
		try {
			Validator.subscriptionValidation(payload);
			List<Object> context = new ArrayList<Object>();
			context.addAll(HttpUtils.getAtContext(request));
			String resolved = JsonUtils
					.toString(JsonLdProcessor.expand(HttpUtils.getAtContext(request), JsonUtils.fromString(payload),
							opts, AppConstants.SUBSCRIPTION_UPDATE_PAYLOAD, HttpUtils.doPreflightCheck(request)));
			Subscription subscription = DataSerializer.getSubscription(resolved);
			if (subscription.getId() == null) {
				subscription.setId(id);
			}
			if (resolved == null || subscription == null || !id.equals(subscription.getId())) {
				return badRequestResponse;
			}

			ValidateURI.validateUriInSubs(id);
			manager.updateSubscription(new SubscriptionRequest(subscription, context, HttpUtils.getHeaders(request)));
		} catch (ResponseException e) {
			logger.error("Exception ::", e);
			return ResponseEntity.status(e.getHttpStatus()).body(new RestResponse(e).toJsonBytes());
		} catch (IOException e) {
			logger.error("Exception ::", e);
			return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(e.getMessage().getBytes());
		}
		return ResponseEntity.noContent().build();
	}

}
