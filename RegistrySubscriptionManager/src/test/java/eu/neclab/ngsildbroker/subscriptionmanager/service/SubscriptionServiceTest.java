package eu.neclab.ngsildbroker.subscriptionmanager.service;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.agroal.api.AgroalDataSource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.mockito.InjectMock;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.groups.UniAwaitOptional;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.kafka.fault.KafkaIgnoreFailure;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.mutiny.pgclient.PgPool;
import net.bytebuddy.implementation.MethodCall.FieldSetting;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.resteasy.reactive.RestResponse;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.nullable;

import com.github.jsonldjava.core.Context;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

import javax.inject.Inject;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ArrayListMultimap;
import com.google.gson.Gson;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.neclab.ngsildbroker.subscriptionmanager.controller.CustomProfile;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.NotificationParam;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.datatypes.requests.AppendEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CreateEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.EntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.UpdateEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.results.CreateResult;
import eu.neclab.ngsildbroker.commons.datatypes.results.UpdateResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.SubscriptionCRUDService;
import eu.neclab.ngsildbroker.commons.storage.StorageDAO;
import eu.neclab.ngsildbroker.commons.subscriptionbase.BaseSubscriptionService;
import eu.neclab.ngsildbroker.commons.subscriptionbase.IntervalNotificationHandler;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.registry.subscriptionmanager.repository.RegistrySubscriptionInfoDAO;
import eu.neclab.ngsildbroker.registry.subscriptionmanager.service.RegistrySubscriptionService;
import groovy.util.logging.Slf4j;

import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.utils.JsonUtils;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@QuarkusTest
@Slf4j
@TestMethodOrder(OrderAnnotation.class)
@TestProfile(CustomProfile.class)
public class SubscriptionServiceTest {

	@Mock
	SubscriptionRequest subscriptionRequest;

	@Mock
	IntervalNotificationHandler intervalHandlerREST;

	@Mock
	HashMap<String, Subscription> subscriptionId2Subscription;

	@Inject
	RegistrySubscriptionService subscriptionService;

	@Mock
	NotificationParam param;

	@Mock
	Subscription subscriptionMock;

	@Mock
	IntervalNotificationHandler intervalNotificationHandler;

	@Inject
	RegistrySubscriptionInfoDAO subService;

	ArrayListMultimap<String, String> multimaparr = ArrayListMultimap.create();

	// resolved payload of a subscription.

	String payload = "{\r\n  \"https://uri.etsi.org/ngsi-ld/entities\" : [ "
			+ "{\r\n    \"@type\" : [ \"https://uri.etsi.org/ngsi-ld/default-context/Vehicle\" ]"
			+ "\r\n  } ],\r\n  \"@id\" : \"urn:ngsi-ld:Subscription:173223\","
			+ "\r\n  \"https://uri.etsi.org/ngsi-ld/notification\" : [ "
			+ "{\r\n    \"https://uri.etsi.org/ngsi-ld/endpoint\" : [ "
			+ "{\r\n      \"https://uri.etsi.org/ngsi-ld/accept\" : [ "
			+ "{\r\n        \"@value\" : \"application/json\""
			+ "\r\n      } ],\r\n      \"https://uri.etsi.org/ngsi-ld/uri\" : [ {"
			+ "\r\n        \"@value\" : \"http://localhost:8080/acc\"\r\n      } ]"
			+ "\r\n    } ]\r\n  } ],\r\n  \"@type\" : [ \"https://uri.etsi.org/ngsi-ld/Subscription\" ]" + "\r\n}";

	String payload1 = "{\r\n  \"https://uri.etsi.org/ngsi-ld/entities\" : [ "
			+ "{\r\n    \"@type\" : [ \"https://uri.etsi.org/ngsi-ld/default-context/Vehicle\" ]"
			+ "\r\n  } ],\r\n  \"@id\" : \"urn:ngsi-ld:Subscription:173224\","
			+ "\r\n  \"https://uri.etsi.org/ngsi-ld/notification\" : [ "
			+ "{\r\n    \"https://uri.etsi.org/ngsi-ld/endpoint\" : [ "
			+ "{\r\n      \"https://uri.etsi.org/ngsi-ld/accept\" : [ "
			+ "{\r\n        \"@value\" : \"application/json\""
			+ "\r\n      } ],\r\n      \"https://uri.etsi.org/ngsi-ld/uri\" : [ {"
			+ "\r\n        \"@value\" : \"http://localhost:8080/acc\"\r\n      } ]"
			+ "\r\n    } ]\r\n  } ],\r\n  \"@type\" : [ \"https://uri.etsi.org/ngsi-ld/Subscription\" ]" + "\r\n}";

	/**
	 * this method is used to test create subscription test
	 */
	@Test
	@Order(1)
	public void serviceTest() throws ResponseException {

		List<Object> context = new ArrayList<>();
		Context context1 = new Context();
		Subscription subscription = null;
		ArrayListMultimap<String, String> subIds = ArrayListMultimap.create();
		multimaparr.put("content-type", "application/json");
		Gson gson = new Gson();
		Map<String, Object> resolved = gson.fromJson(payload, Map.class);
		Subscription s = Subscription.expandSubscription(resolved, context1, true);
		subscription = new Subscription();
		
		SubscriptionRequest subRequest = new SubscriptionRequest(s, context, subIds, 0);
		subscriptionService.subscribeInternal(subRequest).await().indefinitely();
	}

	
	
	/**
	 * this method is used to test create subscription test for deconstructor
	 */
	
	@Test
	@Order(2)
	public void serviceTestforDeconstructor() throws ResponseException {

		List<Object> context = new ArrayList<>();
		Context context1 = new Context();
		Subscription subscription = null;
		ArrayListMultimap<String, String> subIds = ArrayListMultimap.create();
		multimaparr.put("content-type", "application/json");
		Gson gson = new Gson();
		
		Map<String, Object> resolved = gson.fromJson(payload1, Map.class);
		Subscription s = Subscription.expandSubscription(resolved, context1, true);
		subscription = new Subscription();
		SubscriptionRequest subRequest = new SubscriptionRequest(s, context, subIds, 0);
		
		subscriptionService.subscribeInternal(subRequest).await().indefinitely();
	}

	/**
	 * this method is used to test update subscription test
	 */
	
	@Test
	@Order(3)
	public void updateSubscriptionTest() {

		try {
			List<Object> context = new ArrayList<>();
			Context context1 = new Context();
			Subscription subscription = null;
			ArrayListMultimap<String, String> subIds = ArrayListMultimap.create();
			multimaparr.put("content-type", "application/json");
			Gson gson = new Gson();
			Map<String, Object> resolved = gson.fromJson(payload, Map.class);
			Subscription s = Subscription.expandSubscription(resolved, context1, true);
			subscription = new Subscription();
			SubscriptionRequest subRequest = new SubscriptionRequest(s, context, subIds, 0);
			
			subscriptionService.updateInternal(subRequest).await().indefinitely();
			
		} catch (Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}
	}
	
	

	/**
	 * this method is used to test getAllSubscriptions method of SubscriptionService
	 * class
	 */
	@Test
	@Order(4)
	public void getAllSubscriptionsTest() {

		ArrayListMultimap<String, String> subIds = ArrayListMultimap.create();
		List<SubscriptionRequest> result = subscriptionService.getAllSubscriptions(subIds).await().indefinitely();
		assertNotNull(result);
	}

	/**
	 * this method is used to test unsubscribe method of SubscriptionService class
	 */
	
	@Test
	@Order(5)
	public void unsubscribeTest() throws URISyntaxException, ResponseException {

		Subscription subscription = null;
		Subscription removedSub = new Subscription();
		String id = new String("urn:ngsi-ld:Subscription:173223");
		
		subscriptionService.unsubscribeInternal(id).await().indefinitely();
	}

	/**
	 * this method is used to test deconstructor method of SubscriptionService class
	 */
	@Test
	@Order(6)
	public void deconstructorTest() throws URISyntaxException, ResponseException {

		multimaparr.put("content-type", "application/json");
		MockitoAnnotations.initMocks(this);
		
		
		Subscription removedSub = new Subscription();
		String id = new String("urn:ngsi-ld:Subscription:173224");
		
		subscriptionService.deconstructor();
	}

	/**
	 * this method is used to test getSubscription method of SubscriptionService
	 * class
	 */
	@Test
	@Order(7)
	public void getSubscriptionTest() throws ResponseException {

//		String errorMessage=null;
//		try {
//			manager.getSubscription("", ArrayListMultimap.create());
//		} catch (ResponseException e) {
//			errorMessage=e.getMessage();
//		}
//		Assert.assertEquals(errorMessage, "Resource not found.");	
		
		multimaparr.put("content-type", "application/json");
		MockitoAnnotations.initMocks(this);
		ArrayListMultimap<String, String> entityIds = ArrayListMultimap.create();
		
		entityIds.put(AppConstants.INTERNAL_NULL_KEY, "urn:ngsi-ld:Subscription:173223");
		
		try {
			subscriptionService.getSubscription("urn:ngsi-ld:Subscription:173223", entityIds).await().indefinitely();
			
		} catch (Exception e) {
			Assertions.assertEquals("urn:ngsi-ld:Subscription:173223 not found", e.getMessage());
		}
	}
}