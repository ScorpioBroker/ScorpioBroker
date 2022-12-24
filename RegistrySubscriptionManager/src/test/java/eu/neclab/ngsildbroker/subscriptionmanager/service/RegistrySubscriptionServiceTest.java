package eu.neclab.ngsildbroker.subscriptionmanager.service;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.jsonldjava.core.Context;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

import com.google.common.collect.ArrayListMultimap;
import com.google.gson.Gson;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import eu.neclab.ngsildbroker.subscriptionmanager.controller.CustomProfile;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.NotificationParam;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.subscriptionbase.BaseSubscriptionService;
import eu.neclab.ngsildbroker.commons.subscriptionbase.IntervalNotificationHandler;
import eu.neclab.ngsildbroker.registry.subscriptionmanager.repository.RegistrySubscriptionInfoDAO;
import eu.neclab.ngsildbroker.registry.subscriptionmanager.service.RegistrySubscriptionService;
import eu.neclab.ngsildbroker.registry.subscriptionmanager.service.RegistrySubscriptionServiceKafka;
import groovy.util.logging.Slf4j;

@QuarkusTest
@Slf4j
@TestMethodOrder(OrderAnnotation.class)
@TestProfile(CustomProfile.class)
public class RegistrySubscriptionServiceTest {

	String payload;
	String payload1;
	JsonNode blankNode;
	JsonNode payloadNode;

	@Mock
	SubscriptionRequest subscriptionRequest;

	@Mock
	IntervalNotificationHandler intervalHandlerREST;

	@ConfigProperty(name = "scorpio.directDB", defaultValue = "true")
	boolean directDB;

	@Mock
	HashMap<String, Subscription> subscriptionId2Subscription;

	@InjectMocks
	@Spy
	RegistrySubscriptionService subscriptionService;

	@Mock
	NotificationParam param;

	@Mock
	Subscription subscriptionMock;

	@Mock
	RegistrySubscriptionServiceKafka kafka;

	@Mock
	IntervalNotificationHandler intervalNotificationHandler;

	@Mock
	MutinyEmitter<SubscriptionRequest> internalSubEmitter;

	@Mock
	BaseSubscriptionService baseService;

	@Mock
	RegistrySubscriptionInfoDAO subService;

	ArrayListMultimap<String, String> multimaparr = ArrayListMultimap.create();

	// resolved payload of a subscription.

	@BeforeEach
	public void setUp() throws Exception {
		MockitoAnnotations.initMocks(this);
		ObjectMapper objectMapper = new ObjectMapper();

		payload = "{\r\n  \"https://uri.etsi.org/ngsi-ld/entities\" : [ "
				+ "{\r\n    \"@type\" : [ \"https://uri.etsi.org/ngsi-ld/default-context/Vehicle\" ]"
				+ "\r\n  } ],\r\n  \"@id\" : \"urn:ngsi-ld:Subscription:173223\","
				+ "\r\n  \"https://uri.etsi.org/ngsi-ld/notification\" : [ "
				+ "{\r\n    \"https://uri.etsi.org/ngsi-ld/endpoint\" : [ "
				+ "{\r\n      \"https://uri.etsi.org/ngsi-ld/accept\" : [ "
				+ "{\r\n        \"@value\" : \"application/json\""
				+ "\r\n      } ],\r\n      \"https://uri.etsi.org/ngsi-ld/uri\" : [ {"
				+ "\r\n        \"@value\" : \"http://localhost:8080/acc\"\r\n      } ]"
				+ "\r\n    } ]\r\n  } ],\r\n  \"@type\" : [ \"https://uri.etsi.org/ngsi-ld/Subscription\" ]" + "\r\n}";

		payload1 = "{\r\n  \"https://uri.etsi.org/ngsi-ld/entities\" : [ "
				+ "{\r\n    \"@type\" : [ \"https://uri.etsi.org/ngsi-ld/default-context/Vehicle\" ]"
				+ "\r\n  } ],\r\n  \"@id\" : \"urn:ngsi-ld:Subscription:173224\","
				+ "\r\n  \"https://uri.etsi.org/ngsi-ld/notification\" : [ "
				+ "{\r\n    \"https://uri.etsi.org/ngsi-ld/endpoint\" : [ "
				+ "{\r\n      \"https://uri.etsi.org/ngsi-ld/accept\" : [ "
				+ "{\r\n        \"@value\" : \"application/json\""
				+ "\r\n      } ],\r\n      \"https://uri.etsi.org/ngsi-ld/uri\" : [ {"
				+ "\r\n        \"@value\" : \"http://localhost:8080/acc\"\r\n      } ]"
				+ "\r\n    } ]\r\n  } ],\r\n  \"@type\" : [ \"https://uri.etsi.org/ngsi-ld/Subscription\" ]" + "\r\n}";

		blankNode = objectMapper.createObjectNode();
		payloadNode = objectMapper.readTree(payload);
		payloadNode = objectMapper.readTree(payload1);
		directDB = true;
	}

	@AfterEach
	public void tearDown() {
		payload = null;
		payload1 = null;
	}

	/**
	 * this method is used to test create subscription test
	 */
	@Test
	@Order(1)
	public void createSubscriptionTest() throws ResponseException {

		List<Object> context = new ArrayList<>();
		Context context1 = new Context();
		Subscription subscription = null;
		ArrayListMultimap<String, String> subIds = ArrayListMultimap.create();
		multimaparr.put("content-type", "application/json");
		Gson gson = new Gson();
		Map<String, Object> resolved = gson.fromJson(payload, Map.class);
		Subscription s = Subscription.expandSubscription(resolved, context1, true);
		subscription = new Subscription();
		SubscriptionRequest subRequest = new SubscriptionRequest(s, context, multimaparr, 0);
		Void result = subscriptionService.subscribeInternal(subRequest).await().indefinitely();
		Assertions.assertEquals(null, result);
		Mockito.verify(subscriptionService).subscribeInternal(any());
	}

	/**
	 * this method is used to test create subscription test
	 */
	@Test
	@Order(2)
	public void subscribeInternalTest() throws ResponseException {

		List<Object> context = new ArrayList<>();
		Context context1 = new Context();
		Subscription subscription = null;
		ArrayListMultimap<String, String> subIds = ArrayListMultimap.create();
		multimaparr.put("content-type", "application/json");
		Gson gson = new Gson();
		Map<String, Object> resolved = gson.fromJson(payload1, Map.class);
		Subscription s = Subscription.expandSubscription(resolved, context1, true);
		subscription = new Subscription();
		SubscriptionRequest subRequest = new SubscriptionRequest(s, context, multimaparr, 0);
		Void result = subscriptionService.subscribeInternal(subRequest).await().indefinitely();
		Assertions.assertEquals(null, result);
		Mockito.verify(subscriptionService).subscribeInternal(any());
	}

	/**
	 * this method is used to test update subscription test
	 */
	@Test
	@Order(3)
	public void updateSubscriptionTest1() throws ResponseException {

		MockitoAnnotations.initMocks(this);
		List<Object> context = new ArrayList<>();
		Context context1 = new Context();
		Subscription subscription = null;
		ArrayListMultimap<String, String> subIds = ArrayListMultimap.create();
		multimaparr.put("content-type", "application/json");
		Gson gson = new Gson();
		Map<String, Object> resolved = gson.fromJson(payload, Map.class);
		Subscription s = Subscription.expandSubscription(resolved, context1, true);
		subscription = new Subscription();
		subscriptionRequest = new SubscriptionRequest(s, context, multimaparr, 0);
		Mockito.when(baseService.updateSubscription(any())).thenReturn(Uni.createFrom().voidItem());
		try {
			subscriptionService.updateInternal(subscriptionRequest).await().indefinitely();
		} catch (Exception e) {
			Assertions.assertEquals(e.getLocalizedMessage(), e.getMessage());
			Mockito.verify(subscriptionService).updateInternal(any());
		}
	}

	/**
	 * this method is used to test getAllSubscriptions method
	 */
	@Test
	@Order(4)
	public void getAllSubscriptionsTest() {

		List<SubscriptionRequest> result = subscriptionService.getAllSubscriptions(AppConstants.INTERNAL_NULL_KEY)
				.await().indefinitely();
		assertNotNull(result);
		Mockito.verify(subscriptionService).getAllSubscriptions(any());
	}

	/**
	 * this method is used to test unsubscribe method
	 */
	@Test
	@Order(5)
	public void unsubscribeInternalTest() throws URISyntaxException, ResponseException {

		Subscription subscription = null;
		Subscription removedSub = new Subscription();
		String id = new String("urn:ngsi-ld:Subscription:173223");
		ArrayListMultimap<String, String> subIds = ArrayListMultimap.create();
		subscriptionService.unsubscribeInternal(id).await().indefinitely();
		Mockito.verify(subscriptionService).unsubscribeInternal(any());
	}

	/**
	 * this method is used to test deconstructor method
	 */
	@Test
	@Order(6)
	public void deconstructorTest() throws URISyntaxException, ResponseException {

		multimaparr.put("content-type", "application/json");
		MockitoAnnotations.initMocks(this);
		Subscription removedSub = new Subscription();
		String id = new String("urn:ngsi-ld:Subscription:173224");
		subscriptionService.deconstructor();
		Mockito.verify(subscriptionService).deconstructor();
	}

	/**
	 * this method is used to test getSubscription method
	 */
	@Test
	@Order(7)
	public void getSubscriptionTest() throws ResponseException {

		multimaparr.put("content-type", "application/json");
		MockitoAnnotations.initMocks(this);
		ArrayListMultimap<String, String> entityIds = ArrayListMultimap.create();

		try {
			subscriptionService.getSubscription("urn:ngsi-ld:Subscription:173223", AppConstants.INTERNAL_NULL_KEY)
					.await().indefinitely();
		} catch (Exception e) {
			Assertions.assertEquals("urn:ngsi-ld:Subscription:173223 not found", e.getMessage());
			Mockito.verify(subscriptionService).getSubscription(any(), any());
		}
	}
}