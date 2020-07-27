package eu.neclab.ngsildbroker.registryhandler.controller;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.netflix.discovery.EurekaClient;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.RestResponse;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.datatypes.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.ldcontext.ContextResolverBasic;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;
import eu.neclab.ngsildbroker.commons.stream.service.KafkaOps;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.registryhandler.service.CSourceSubscriptionService;


@RestController
@RequestMapping("/ngsi-ld/v1/csourceSubscriptions")
public class RegistrySubscriptionController {
	
	private final static Logger logger = LogManager.getLogger(RegistrySubscriptionController.class);
	
	@Autowired
	CSourceSubscriptionService manager;

	@Autowired
	@Qualifier("rmconRes")
	ContextResolverBasic contextResolver;

	
	@Autowired
	@Qualifier("rmops")
	KafkaOps kafkaOps;

	@Autowired
	EurekaClient eurekaClient;
	
	HttpUtils httpUtils = HttpUtils.getInstance(contextResolver);

	ResponseException badRequest = new ResponseException(ErrorType.BadRequestData);

	ResponseEntity<byte[]> badRequestResponse = ResponseEntity.status(badRequest.getHttpStatus())
			.body(new RestResponse(badRequest).toJsonBytes());
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
	public ResponseEntity<byte[]> subscribeRest(HttpServletRequest request,	@RequestBody String payload) throws ResponseException {
		logger.trace("subscribeRest() :: started");
		Subscription subscription;


		List<Object> context = HttpUtils.getAtContext(request);
		String resolved = contextResolver.expand(payload, context, true, AppConstants.CSOURCE_URL_ID);
		
		subscription = DataSerializer.getSubscription(resolved);
		if (resolved == null || subscription == null) {
			return badRequestResponse;
		}

		try {
			SubscriptionRequest subscriptionRequest = new SubscriptionRequest(subscription, context);
			URI subId = manager.subscribe(subscriptionRequest);
			logger.trace("subscribeRest() :: completed");
			//no absolute url only relative url
			return ResponseEntity.created(new URI("/ngsi-ld/v1/csourceSubscriptions/" + subId.toString())).body(subId.toString().getBytes());
		} catch (ResponseException e) {
			logger.error("Exception ::",e);
			return ResponseEntity.status(e.getHttpStatus()).body(new RestResponse(e).toJsonBytes());
		} catch (URISyntaxException e) {
			logger.error("Exception ::",e);
			return ResponseEntity.status(HttpStatus.CONFLICT).body(subscription.getId().toString().getBytes());
		}
	}

	@GetMapping
	public ResponseEntity<byte[]> getAllSubscriptions(HttpServletRequest request,  @RequestParam(required = false, name = "limit", defaultValue = "0") int limit) throws ResponseException{
		logger.trace("getAllSubscriptions() :: started");
		List<Subscription> result = null;
		result = manager.getAllSubscriptions(limit);
		logger.trace("getAllSubscriptions() :: completed");
		return httpUtils.generateReply(request, DataSerializer.toJson(result));
	}
	
	@GetMapping("{id}")
	//(method = RequestMethod.GET, value = "/{id}")
	public ResponseEntity<byte[]> getSubscriptions(HttpServletRequest request, @PathVariable(name = NGSIConstants.QUERY_PARAMETER_ID, required = true) URI id,
			@RequestParam(required = false, name = "limit", defaultValue = "0") int limit) {
		try {
			logger.trace("call getSubscriptions() ::");
			return httpUtils.generateReply(request, DataSerializer.toJson(manager.getSubscription(id)));
			
		} catch (ResponseException e) {
			logger.error("Exception ::",e);
			return ResponseEntity.status(e.getHttpStatus()).body(new RestResponse(e).toJsonBytes());
		}

		
	}

	@DeleteMapping("{id}")
	@RequestMapping(method = RequestMethod.DELETE, value = "/{id}")
	public ResponseEntity<byte[]> deleteSubscription(@PathVariable(name = NGSIConstants.QUERY_PARAMETER_ID, required = true) URI id) {
		try {
			logger.trace("call deleteSubscription() ::");
			manager.unsubscribe(id);
		} catch (ResponseException e) {
			logger.error("Exception ::",e);
			return ResponseEntity.status(e.getHttpStatus()).body(new RestResponse(e).toJsonBytes());
		}
		return ResponseEntity.noContent().build();
	}

	@PatchMapping("{id}")
	public ResponseEntity<byte[]> updateSubscription(HttpServletRequest request, @PathVariable(name = NGSIConstants.QUERY_PARAMETER_ID, required = true) URI id,
			@RequestBody String payload) throws ResponseException {
		logger.trace("call updateSubscription() ::");
		List<Object> context = HttpUtils.getAtContext(request);
		String resolved = contextResolver.expand(payload, context, true, AppConstants.CSOURCE_URL_ID);
		Subscription subscription = DataSerializer.getSubscription(resolved);
		if(subscription.getId() == null) {
			subscription.setId(id);
		}
		if (resolved == null || subscription == null || !id.equals(subscription.getId())) {
			return badRequestResponse;
		}
		try {
			manager.updateSubscription(subscription);
		} catch (ResponseException e) {
			logger.error("Exception ::",e);
			return ResponseEntity.status(e.getHttpStatus()).body(new RestResponse(e).toJsonBytes());
		}
		return ResponseEntity.noContent().build();
	}

}
