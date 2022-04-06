package eu.neclab.ngsildbroker.subscriptionmanager.service;

import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.util.ReflectionTestUtils;

import com.github.jsonldjava.core.JsonLdOptions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import eu.neclab.ngsildbroker.commons.datatypes.EndPoint;
import eu.neclab.ngsildbroker.commons.datatypes.NotificationParam;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.enums.Format;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.subscriptionbase.BaseSubscriptionInfoDAO;
import eu.neclab.ngsildbroker.commons.subscriptionbase.BaseSubscriptionService;
import eu.neclab.ngsildbroker.commons.subscriptionbase.SubscriptionInfoDAOInterface;

@SpringBootTest(properties = { "spring.main.allow-bean-definition-overriding=true" })
@RunWith(SpringRunner.class)
@AutoConfigureMockMvc // (secure = false)

public class SubscriptionServiceTest {
	static final JsonLdOptions opts = new JsonLdOptions(JsonLdOptions.JSON_LD_1_1);
	@Mock
	HashMap<String, Subscription> subscriptionId2Subscription;

	@InjectMocks
	private SubscriptionService manager;

	@Mock
	BaseSubscriptionService baseSubscriptionService;

	@Mock
	private SubscriptionInfoDAOInterface subscriptionInfoDAO;

	@Mock
	BaseSubscriptionInfoDAO baseSubscriptionInfoDAO;

	@Mock
	private KafkaTemplate<String, Object> kafkaTemplate;

	@Mock
	Table<String, String, SubscriptionRequest> tenant2subscriptionId2Subscription = HashBasedTable.create();

	@Mock
	Subscription subscription;

	@Mock
	Subscription subscriptionMock;
	ArrayListMultimap<String, String> multimaparr = ArrayListMultimap.create();

	// resolved payload of a subscription.
	String resolvedpayload = "{\r\n  \"https://uri.etsi.org/ngsi-ld/entities\" : [ "
			+ "{\r\n    \"@type\" : [ \"https://uri.etsi.org/ngsi-ld/default-context/Vehicle\" ]"
			+ "\r\n  } ],\r\n  \"@id\" : \"urn:ngsi-ld:Subscription:173223\","
			+ "\r\n  \"https://uri.etsi.org/ngsi-ld/notification\" : [ "
			+ "{\r\n    \"https://uri.etsi.org/ngsi-ld/endpoint\" : [ "
			+ "{\r\n      \"https://uri.etsi.org/ngsi-ld/accept\" : [ "
			+ "{\r\n        \"@value\" : \"application/json\""
			+ "\r\n      } ],\r\n      \"https://uri.etsi.org/ngsi-ld/uri\" : [ {"
			+ "\r\n        \"@value\" : \"http://localhost:8080/acc\"\r\n      } ]"
			+ "\r\n    } ]\r\n  } ],\r\n  \"@type\" : [ \"https://uri.etsi.org/ngsi-ld/Subscription\" ]" + "\r\n}";

	String databasepayload = "{\r\n" + "  \"subscription\": {\r\n"
			+ "    \"@id\": \"urn:ngsi-ld:Subscription:storeSubscription\",\r\n"
			+ "    \"@type\": \"https://uri.etsi.org/ngsi-ld/Subscription\",\r\n"
			+ "    \"https://uri.etsi.org/ngsi-ld/entities\": [\r\n" + "      {\r\n" + "        \"@type\": [\r\n"
			+ "          \"https://uri.etsi.org/ngsi-ld/default-context/Test2\"\r\n" + "        ]\r\n" + "      }\r\n"
			+ "    ],\r\n" + "    \"https://uri.etsi.org/ngsi-ld/notification\": [\r\n" + "      {\r\n"
			+ "        \"https://uri.etsi.org/ngsi-ld/endpoint\": [\r\n" + "          {\r\n"
			+ "            \"https://uri.etsi.org/ngsi-ld/accept\": [\r\n" + "              {\r\n"
			+ "                \"@value\": \"application/json\"\r\n" + "              }\r\n" + "            ],\r\n"
			+ "            \"https://uri.etsi.org/ngsi-ld/uri\": [\r\n" + "              {\r\n"
			+ "                \"@value\": \"http://localhost:8080\"\r\n" + "              }\r\n" + "            ]\r\n"
			+ "          }\r\n" + "        ],\r\n" + "        \"https://uri.etsi.org/ngsi-ld/format\": [\r\n"
			+ "          {\r\n" + "            \"@value\": \"normalized\"\r\n" + "          }\r\n" + "        ]\r\n"
			+ "      }\r\n" + "    ],\r\n" + "    \"https://uri.etsi.org/ngsi-ld/status\": [\r\n" + "      {\r\n"
			+ "        \"@value\": \"active\"\r\n" + "      }\r\n" + "    ],\r\n"
			+ "    \"https://uri.etsi.org/ngsi-ld/isActive\": [\r\n" + "      {\r\n" + "        \"@value\": true\r\n"
			+ "      }\r\n" + "    ]\r\n" + "  },\r\n" + "  \"context\": [],\r\n" + "  \"active\": true,\r\n"
			+ "  \"type\": 0,\r\n" + "  \"headers\": {},\r\n"
			+ "  \"id\": \"urn:ngsi-ld:Subscription:storeSubscription\",\r\n" + "  \"requestType\": 0\r\n" + "}";

	@Before
	public void setUp() throws Exception {
		MockitoAnnotations.initMocks(this);
	}

	/**
	 * this method is used to test create subscription test
	 * 
	 * @throws URISyntaxException
	 */
	@Test
	public void serviceTest() throws ResponseException, URISyntaxException {
		List<Object> context = new ArrayList<>();
		NotificationParam notifyParam = new NotificationParam();
		EndPoint endPoint = new EndPoint();
		URI uri = new URI("http://localhost:8080");
		endPoint.setUri(uri);
		endPoint.setAccept("application/json");
		notifyParam.setEndPoint(endPoint);
		Format format = Format.normalized;
		format = Format.keyValues;
		notifyParam.setFormat(format);
		Subscription subscription = new Subscription();
		subscription.setId("urn:ngsi-ld:Subscription:173223");
		subscription.setType("Subscription");
		subscription.setNotification(notifyParam);
		multimaparr.put("content-type", "application/json");
		SubscriptionRequest subRequest = new SubscriptionRequest(subscription, context, multimaparr, 0);
		String subId = manager.subscribe(subRequest);
		Assert.assertEquals("urn:ngsi-ld:Subscription:173223", subId);
		verify(subscriptionInfoDAO, times(1)).storeSubscription(any());

	}

	/**
	 * this method is used to test update subscription test
	 * 
	 * @throws URISyntaxException
	 */
	@Test
	public void updateSubscriptionTest() throws URISyntaxException {
		List<Object> context = new ArrayList<>();
		NotificationParam notifyParam = new NotificationParam();
		EndPoint endPoint = new EndPoint();
		URI uri = new URI("http://localhost:8080");
		endPoint.setUri(uri);
		endPoint.setAccept("application/json");
		notifyParam.setEndPoint(endPoint);
		Format format = Format.normalized;
		format = Format.keyValues;
		notifyParam.setFormat(format);
		Subscription subscription = new Subscription();
		subscription.setId("urn:ngsi-ld:Subscription:173223");
		subscription.setType("Subscription");
		subscription.setNotification(notifyParam);
		multimaparr.put("content-type", "application/json");
		SubscriptionRequest subRequest = new SubscriptionRequest(subscription, context, multimaparr, 0);
		when(subscriptionId2Subscription.get(any())).thenReturn(new Subscription());
		try {
			manager.updateSubscription(subRequest);
		} catch (Exception e) {
			Assert.assertEquals("urn:ngsi-ld:Subscription:173223 not found", e.getMessage());
		}
	}

	/**
	 * this method is used to test getAllSubscriptions method of SubscriptionService
	 * class
	 */
	@Test
	public void getAllSubscriptionsTest() {
		List<SubscriptionRequest> result = manager.getAllSubscriptions(ArrayListMultimap.create());
		assertNotNull(result);
	}

	/**
	 * this method is used to test unsubscribe method of SubscriptionService class
	 * 
	 * @throws InvocationTargetException
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 * @throws SecurityException
	 * @throws NoSuchMethodException
	 */
	@Test
	public void unsubscribeTest() throws URISyntaxException, ResponseException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
		List<Object> context = new ArrayList<>();
		NotificationParam notifyParam = new NotificationParam();
		EndPoint endPoint = new EndPoint();
		URI uri = new URI("http://localhost:8080");
		endPoint.setUri(uri);
		endPoint.setAccept("application/json");
		notifyParam.setEndPoint(endPoint);
		Format format = Format.normalized;
		format = Format.keyValues;
		notifyParam.setFormat(format);
		Subscription subscription = new Subscription();
		subscription.setId("urn:ngsi-ld:Subscription:173223");
		subscription.setType("Subscription");
		subscription.setNotification(notifyParam);
		multimaparr.put("content-type", "application/json");
		SubscriptionRequest subRequest = new SubscriptionRequest(subscription, context, multimaparr, 0);
		tenant2subscriptionId2Subscription.put(subRequest.getTenant(), subRequest.getId().toString(), subRequest);
		ReflectionTestUtils.setField(baseSubscriptionService, "tenant2subscriptionId2Subscription",
				tenant2subscriptionId2Subscription);
	}

	/**
	 * this method is used to test getSubscription method of SubscriptionService
	 * class
	 * 
	 * @throws URISyntaxException
	 */
	@Test
	public void getSubscriptionTest() throws URISyntaxException {
		String errorMessage = null;
		List<Object> context = new ArrayList<>();
		NotificationParam notifyParam = new NotificationParam();
		EndPoint endPoint = new EndPoint();
		URI uri = new URI("http://localhost:8080");
		endPoint.setUri(uri);
		endPoint.setAccept("application/json");
		notifyParam.setEndPoint(endPoint);
		Format format = Format.normalized;
		format = Format.keyValues;
		notifyParam.setFormat(format);
		Subscription subscription = new Subscription();
		subscription.setId("urn:ngsi-ld:Subscription:173223");
		subscription.setType("Subscription");
		subscription.setNotification(notifyParam);
		multimaparr.put("content-type", "application/json");
		SubscriptionRequest subRequest = new SubscriptionRequest(subscription, context, multimaparr, 0);
		tenant2subscriptionId2Subscription.put(subRequest.getTenant(), subRequest.getId().toString(), subRequest);
		ReflectionTestUtils.setField(baseSubscriptionService, "tenant2subscriptionId2Subscription",
				tenant2subscriptionId2Subscription);
		try {
			manager.getSubscription("urn:ngsi-ld:Subscription:173224", multimaparr);
		} catch (ResponseException e) {
			Assert.assertEquals("urn:ngsi-ld:Subscription:173224 not found", e.getMessage());
		}

	}

}
