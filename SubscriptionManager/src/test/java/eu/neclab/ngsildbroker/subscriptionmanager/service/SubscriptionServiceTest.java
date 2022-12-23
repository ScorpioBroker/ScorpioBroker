package eu.neclab.ngsildbroker.subscriptionmanager.service;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.reactive.messaging.MutinyEmitter;

import com.github.jsonldjava.core.Context;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

import com.google.common.collect.ArrayListMultimap;
import com.google.gson.Gson;

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
import eu.neclab.ngsildbroker.subscriptionmanager.repository.SubscriptionInfoDAO;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.InternalNotification;
import eu.neclab.ngsildbroker.commons.datatypes.NotificationParam;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.subscriptionbase.IntervalNotificationHandler;
import groovy.util.logging.Slf4j;

@QuarkusTest
@Slf4j
@TestMethodOrder(OrderAnnotation.class)
@TestProfile(CustomProfile.class)
public class SubscriptionServiceTest {

	String payload;

	@Mock
	SubscriptionRequest subscriptionRequest;

	@Mock
	IntervalNotificationHandler intervalHandlerREST;

	@Mock
	InternalNotification notify;

	@Mock
	HashMap<String, Subscription> subscriptionId2Subscription;

	@InjectMocks
	@Spy
	SubscriptionService subscriptionService;

	@Mock
	NotificationParam param;

	@Mock
	Subscription subscriptionMock;

	@Mock
	IntervalNotificationHandler intervalNotificationHandler;

	@Mock
	MutinyEmitter<SubscriptionRequest> internalSubEmitter;

	@Mock
	SubscriptionServiceKafka kafka;

	@Mock
	SubscriptionInfoDAO subService;

	ArrayListMultimap<String, String> multimaparr = ArrayListMultimap.create();

	// resolved payload of a subscription.

	@BeforeEach
	public void setUp() {
		MockitoAnnotations.initMocks(this);

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
	}

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
		SubscriptionRequest subRequest = new SubscriptionRequest(s, context, multimaparr, 0);
		String subId = subscriptionService.subscribe(subRequest).await().indefinitely();
		Assertions.assertEquals("urn:ngsi-ld:Subscription:173223", subId);
		Mockito.verify(subscriptionService).subscribe(any());
	}

	/**
	 * this method is used to test updatesubscription test
	 */
	@Test
	@Order(2)
	public void updateSubscriptionNotFoundTest() throws Exception {

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
		try {
			subscriptionService.updateSubscription(subRequest).await().indefinitely();
		} catch (Exception e) {
			Assertions.assertEquals("urn:ngsi-ld:Subscription:173223 not found", e.getMessage());
			Mockito.verify(subscriptionService).updateSubscription(any());
		}
	}

	/**
	 * this method is used to test getAllSubscriptions method
	 */
	@Test
	@Order(3)
	public void getAllSubscriptionsTest() {

		ArrayListMultimap<String, String> subIds = ArrayListMultimap.create();
		List<SubscriptionRequest> result = subscriptionService.getAllSubscriptions(AppConstants.INTERNAL_NULL_KEY)
				.await().indefinitely();
		assertNotNull(result);
		Mockito.verify(subscriptionService).getAllSubscriptions(any());
	}

	/**
	 * this method is used to test subscribeToRemote method
	 */
	@Test
	@Order(4)
	public void getInternalNotificationTest() throws ResponseException {

		List<Object> context = new ArrayList<>();
		Context context1 = new Context();
		Subscription subscription = null;

		multimaparr.put("content-type", "application/json");
		Gson gson = new Gson();
		Map<String, Object> resolved = gson.fromJson(payload, Map.class);
		Subscription s = Subscription.expandSubscription(resolved, context1, true);
		subscription = new Subscription();
		SubscriptionRequest subRequest = new SubscriptionRequest(s, context, multimaparr, 0);
		subscriptionService.subscribeToRemote(subRequest, notify);
		Mockito.verify(subscriptionService).subscribeToRemote(any(), any());

	}

	/**
	 * this method is used to test subscribeToRemote method when Subscription is
	 * null
	 */
	@Test
	@Order(5)
	public void getInternalNotificationNullTest() throws ResponseException {

		Subscription subscription = null;
		multimaparr.put("content-type", "application/json");
		Gson gson = new Gson();
		subscription = new Subscription();
		SubscriptionRequest subRequest = null;
		subscriptionService.subscribeToRemote(subRequest, notify);
		Mockito.verify(subscriptionService).subscribeToRemote(any(), any());
	}

	/**
	 * this method is used to test remoteNotify method
	 */
	@Test
	@Order(6)
	public void remoteNotificationNullTest() throws ResponseException {

		multimaparr.put("content-type", "application/json");
		Gson gson = new Gson();
		String id = new String("urn:ngsi-ld:Subscription:173223");
		Map<String, Object> resolved = null;
		subscriptionService.remoteNotify(id, resolved);
		Mockito.verify(subscriptionService).remoteNotify(any(), any());
	}

	/**
	 * this method is used to test handleRegistryNotification method
	 */
	@Test
	@Order(7)
	public void handleRegistryNotificationTest() throws ResponseException {

		List<Object> context = new ArrayList<>();
		Context context1 = new Context();
		Subscription subscription = null;
		ArrayListMultimap<String, String> subIds = ArrayListMultimap.create();
		InternalNotification notify = new InternalNotification(payload, payload, null, payload, null, 0, context,
				payload, AppConstants.INTERNAL_NULL_KEY);
		subscriptionService.handleRegistryNotification(notify);
		Mockito.verify(subscriptionService).handleRegistryNotification(any());
		;
	}

	/**
	 * this method is used to test unsubscribe method
	 */
	@Test
	@Order(8)
	public void unsubscribeNotFoundTest() throws URISyntaxException, ResponseException {

		String id = new String("urn:ngsi-ld:Subscription:173223");

		try {
			subscriptionService.unsubscribe(id, AppConstants.INTERNAL_NULL_KEY).await().indefinitely();
		} catch (Exception e) {
			Assertions.assertEquals("urn:ngsi-ld:Subscription:173223 not found", e.getMessage());
			Mockito.verify(subscriptionService).unsubscribe(any(), any());
		}
	}

	/**
	 * this method is used to test getSubscription method
	 */
	@Test
	@Order(9)
	public void getSubscriptionTest() throws ResponseException {

		multimaparr.put("content-type", "application/json");
		MockitoAnnotations.initMocks(this);

		try {
			subscriptionService.getSubscription("urn:ngsi-ld:Subscription:173223", AppConstants.INTERNAL_NULL_KEY)
					.await().indefinitely();
		} catch (Exception e) {
			Assertions.assertEquals("urn:ngsi-ld:Subscription:173223 not found", e.getMessage());
			Mockito.verify(subscriptionService).getSubscription(any(), any());
		}
	}
}
