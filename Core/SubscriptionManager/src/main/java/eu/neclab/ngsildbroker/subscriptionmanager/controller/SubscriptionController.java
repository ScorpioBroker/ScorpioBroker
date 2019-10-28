package eu.neclab.ngsildbroker.subscriptionmanager.controller;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.netflix.discovery.EurekaClient;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.EntityInfo;
import eu.neclab.ngsildbroker.commons.datatypes.RestResponse;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.datatypes.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.SubscriptionManager;
import eu.neclab.ngsildbroker.commons.ldcontext.ContextResolverBasic;
import eu.neclab.ngsildbroker.commons.ngsiqueries.ParamsResolver;
import eu.neclab.ngsildbroker.commons.ngsiqueries.QueryParser;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;
import eu.neclab.ngsildbroker.commons.stream.service.KafkaOps;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.subscriptionmanager.config.SubscriptionManagerProducerChannel;

@RestController
public class SubscriptionController {

	private final static Logger logger = LogManager.getLogger(SubscriptionController.class);

	@Autowired
	SubscriptionManager manager;

	@Autowired
	ContextResolverBasic contextResolver;

	@Autowired
	SubscriptionManagerProducerChannel producerChannel;
	@Autowired
	KafkaOps kafkaOps;

	@Autowired
	EurekaClient eurekaClient;

	@Value("${atcontext.url}")
	String atContextServerUrl;

	@Autowired
	QueryParser queryParser;

	@Autowired
	ParamsResolver ldTools;

	ResponseException badRequest = new ResponseException(ErrorType.BadRequestData);

	ResponseEntity<Object> badRequestResponse = ResponseEntity.status(badRequest.getHttpStatus())
			.body(new RestResponse(badRequest));
	

	private HttpUtils httpUtils;

	@PostConstruct
	private void setup() {
		this.httpUtils = HttpUtils.getInstance(contextResolver);
	}

	@RequestMapping(method = RequestMethod.POST, value = "/")
	@ResponseBody
	public ResponseEntity<Object> subscribeRest(HttpServletRequest request, @RequestBody String payload) {
		logger.trace("subscribeRest() :: started");
		Subscription subscription = null;

		try {
			List<Object> context = HttpUtils.getAtContext(request);
			String resolved = contextResolver.expand(payload, context);

			subscription = DataSerializer.getSubscription(resolved);

			if (resolved == null || subscription == null) {
				return badRequestResponse;
			}
			if (subscription.getLdQuery() != null && !subscription.getLdQuery().trim().equals("")) {
				subscription.setQueryTerm(queryParser.parseQuery(subscription.getLdQuery(), context));
			}
			SubscriptionRequest subRequest = new SubscriptionRequest(subscription, context);
			URI subId = manager.subscribe(subRequest);

			logger.trace("subscribeRest() :: completed");
			return ResponseEntity.created(new URI(AppConstants.SUBSCRIPTIONS_URL + subId.toString())).body(subId);
		} catch (ResponseException e) {
			logger.error("Exception ::", e);
			return ResponseEntity.status(e.getHttpStatus()).body(new RestResponse(e));
		} catch (URISyntaxException e) {
			logger.error("Exception ::", e);
			return ResponseEntity.status(HttpStatus.CONFLICT).body(subscription.getId());
		}
	}

	@RequestMapping(method = RequestMethod.GET, value = "/")
	public ResponseEntity<Object> getAllSubscriptions(HttpServletRequest request,
			@RequestParam(required = false, name = "limit", defaultValue = "0") int limit) throws ResponseException {
		logger.trace("getAllSubscriptions() :: started");
		List<Subscription> result = null;
		result = manager.getAllSubscriptions(limit);
		logger.trace("getAllSubscriptions() :: completed");
		
		return httpUtils.generateReply(request, DataSerializer.toJson(result));

	}

	@RequestMapping(method = RequestMethod.GET, value = "/{id}")
	public ResponseEntity<Object> getSubscriptions(HttpServletRequest request,
			@PathVariable(name = NGSIConstants.QUERY_PARAMETER_ID, required = true) String id,
			@RequestParam(required = false, name = "limit", defaultValue = "0") int limit) {
		try {
			logger.trace("call getSubscriptions() ::");
			return httpUtils.generateReply(request, DataSerializer.toJson(manager.getSubscription(id)));

		} catch (ResponseException e) {
			logger.error("Exception ::", e);
			return ResponseEntity.status(e.getHttpStatus()).body(new RestResponse(e));
		}

	}

	@RequestMapping(method = RequestMethod.DELETE, value = "/{id}")
	public ResponseEntity<Object> deleteSubscription(HttpServletRequest request,
			@PathVariable(name = NGSIConstants.QUERY_PARAMETER_ID, required = true) URI id) {
		try {
			logger.trace("call deleteSubscription() ::");
			manager.unsubscribe(id);
		} catch (ResponseException e) {
			logger.error("Exception ::", e);
			return ResponseEntity.status(e.getHttpStatus()).body(new RestResponse(e));
		}
		return ResponseEntity.noContent().build();
	}

	@RequestMapping(method = RequestMethod.PATCH, value = "/{"+NGSIConstants.QUERY_PARAMETER_ID+"}")
	public ResponseEntity<Object> updateSubscription(HttpServletRequest request,
			@PathVariable(name = NGSIConstants.QUERY_PARAMETER_ID, required = true) URI id,
			@RequestBody String payload) {
		logger.trace("call updateSubscription() ::");
		List<Object> context = HttpUtils.getAtContext(request);
		try {
			String resolved = contextResolver.expand(payload, context);
			Subscription subscription = DataSerializer.getSubscription(resolved);
			if(subscription.getId() == null) {
				subscription.setId(id);
			}
			SubscriptionRequest subscriptionRequest = new SubscriptionRequest(subscription, context);
			
//			expandSubscriptionAttributes(subscription, context);
			if (resolved == null || subscription == null || !id.equals(subscription.getId())) {
				return badRequestResponse;
			}
			manager.updateSubscription(subscriptionRequest);
		} catch (ResponseException e) {
			logger.error("Exception ::", e);
			return ResponseEntity.status(e.getHttpStatus()).body(new RestResponse(e));
		}
		return ResponseEntity.noContent().build();
	}


//	private void expandSubscriptionAttributes(Subscription subscription, List<Object> context)
//			throws ResponseException {
//		for (EntityInfo info : subscription.getEntities()) {
//			if (info.getType() != null && !info.getType().trim().equals("")) {
//				info.setType(ldTools.expandAttribute(info.getType(), context));
//			}
//		}
//		if (subscription.getAttributeNames() != null) {
//			ArrayList<String> newAttribNames = new ArrayList<String>();
//			for (String attrib : subscription.getAttributeNames()) {
//				newAttribNames.add(ldTools.expandAttribute(attrib, context));
//			}
//			subscription.setAttributeNames(newAttribNames);
//		}
//		if (subscription.getNotification().getAttributeNames() != null) {
//			ArrayList<String> newAttribNames = new ArrayList<String>();
//			for (String attrib : subscription.getNotification().getAttributeNames()) {
//				newAttribNames.add(ldTools.expandAttribute(attrib, context));
//			}
//			subscription.getNotification().setAttributeNames(newAttribNames);
//
//		}
//
//	}

}
