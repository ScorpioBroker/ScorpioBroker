package eu.neclab.ngsildbroker.subscriptionmanager.service;

import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
import org.springframework.test.context.junit4.SpringRunner;

import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.datatypes.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;
import eu.neclab.ngsildbroker.commons.stream.service.KafkaOps;

@SpringBootTest(properties= {"spring.main.allow-bean-definition-overriding=true"})
@RunWith(SpringRunner.class)
@AutoConfigureMockMvc//(secure = false)

public class SubscriptionServiceTest {
	
	@Mock
	private KafkaOps kafkaOperations;
	
	@Mock 
	SubscriptionRequest subscriptionRequest;
	
	@Mock
	IntervalNotificationHandler intervalHandlerREST;
	
	@Mock
	HashMap<String, Subscription> subscriptionId2Subscription;
	
	@InjectMocks
	private SubscriptionService manager;
	
	@Mock
	Subscription subscriptionMock;
	
	@Mock
	IntervalNotificationHandler intervalNotificationHandler;

	//resolved payload of a subscription.
	private String resolved="{\r\n  \"https://uri.etsi.org/ngsi-ld/entities\" : [ "
			+ "{\r\n    \"@type\" : [ \"https://uri.etsi.org/ngsi-ld/default-context/Vehicle\" ]"
			+ "\r\n  } ],\r\n  \"@id\" : \"urn:ngsi-ld:Subscription:173223\","
			+ "\r\n  \"https://uri.etsi.org/ngsi-ld/notification\" : [ "
			+ "{\r\n    \"https://uri.etsi.org/ngsi-ld/endpoint\" : [ "
			+ "{\r\n      \"https://uri.etsi.org/ngsi-ld/accept\" : [ "
			+ "{\r\n        \"@value\" : \"application/json\""
			+ "\r\n      } ],\r\n      \"https://uri.etsi.org/ngsi-ld/uri\" : [ {"
			+ "\r\n        \"@value\" : \"http://localhost:8080/acc\"\r\n      } ]"
			+ "\r\n    } ]\r\n  } ],\r\n  \"@type\" : [ \"https://uri.etsi.org/ngsi-ld/Subscription\" ]"
			+ "\r\n}";
	
	@Before
	public void setUp() throws Exception {
		System.out.println("method called");
		MockitoAnnotations.initMocks(this);
	}
	
	/**
	 * this method is used to test create subscription test
	 */
   @Test
   public void serviceTest() throws ResponseException {		 
		List<Object> context = new ArrayList<>();	
	    Subscription subscription = null;
	    subscription = DataSerializer.getSubscription(resolved);
	    SubscriptionRequest subRequest = new SubscriptionRequest(subscription, context);
		URI subId = manager.subscribe(subRequest);
		verify(kafkaOperations, times(1)).pushToKafka(any(),any(),any());
	 
 }
   
	/**
	 * this method is used to test update subscription test
	 */
  @Test
  public void updateSubscriptionTest() {		 
		List<Object> context = new ArrayList<>();	
	    Subscription subscription = null;
	    subscription = DataSerializer.getSubscription(resolved);
	    SubscriptionRequest subRequest = new SubscriptionRequest(subscription, context);
	    when(subscriptionId2Subscription.get(any())).thenReturn(new Subscription());
		try {
			manager.updateSubscription(subRequest);
		} catch (Exception e) {
			verify(subscriptionId2Subscription,times(1)).get(any());
		}
}
   
	
	/**
	 * this method is used to test getAllSubscriptions method of SubscriptionService class
	 */
	@Test
	public void getAllSubscriptionsTest() {
		List<Subscription> result=manager.getAllSubscriptions(0);
		assertNotNull(result);
	}
	
	/**
	 * this method is used to test unsubscribe method of SubscriptionService class
	 */
	@Test
	public void unsubscribeTest() throws URISyntaxException, ResponseException  {
		Subscription removedSub=new Subscription();	
		URI id=new URI("urn:ngsi-ld:Subscription:173223");
		when(subscriptionId2Subscription.remove(any())).thenReturn(removedSub);
		manager.unsubscribe(id);
		//verify(kafkaOperations, times(1)).pushToKafka(any(),any(),any());
		verify(intervalNotificationHandler,times(1)).removeSub(any());
	}
	
	/**
	 * this method is used to test getSubscription method of SubscriptionService class
	 */
	@Test
	public void getSubscriptionTest()  {
		String errorMessage=null;
		try {
			manager.getSubscription("");
		} catch (ResponseException e) {
			errorMessage=e.getMessage();
		}
		Assert.assertEquals(errorMessage, "Resource not found.");	
	}
}
