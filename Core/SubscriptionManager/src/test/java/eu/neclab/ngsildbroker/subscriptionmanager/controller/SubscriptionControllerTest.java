package eu.neclab.ngsildbroker.subscriptionmanager.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.ldcontext.ContextResolverBasic;
import eu.neclab.ngsildbroker.commons.ngsiqueries.ParamsResolver;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;
import eu.neclab.ngsildbroker.subscriptionmanager.service.SubscriptionService;

@SpringBootTest(properties= {"spring.main.allow-bean-definition-overriding=true"})
@RunWith(SpringRunner.class)
@AutoConfigureMockMvc//(secure = false)
public class SubscriptionControllerTest {
	@Autowired
	private MockMvc mockMvc;
	
	@MockBean
	private SubscriptionService subscriptionService;
	
	
	@InjectMocks
	private SubscriptionController subscriptionController;
	
	
	@Autowired
	ContextResolverBasic contextResolver;
   
	@Autowired
	ParamsResolver paramsResolver;

	private String subscriptionEntityPayload;
	

	@Before	
	public void setup() throws Exception {
		MockitoAnnotations.initMocks(this);
		
		//@formatter:off
		
		subscriptionEntityPayload="{"
				+ "\r\n\"id\": \"urn:ngsi-ld:Subscription:211\","
				+ "\r\n\"type\": \"Subscription\","
				+ "\r\n\"entities\": [{"
				+ "\r\n		  \"id\": \"urn:ngsi-ld:Vehicle:A143\","
				+ "\r\n		  \"type\": \"Vehicle\""
				+ "\r\n		}],"
				+ "\r\n\"watchedAttributes\": [\"brandName\"],"
				+ "\r\n		\"q\":\"brandName!=Mercedes\","
				+ "\r\n\"notification\": {"
				+ "\r\n  \"attributes\": [\"brandName\"],"
				+ "\r\n  \"format\": \"keyValues\","
				+ "\r\n  \"endpoint\": {"
				+ "\r\n   \"uri\": \"mqtt://localhost:1883/notify\","
				+ "\r\n   \"accept\": \"application/json\","
				+ "\r\n	\"notifierinfo\": {"
				+ "\r\n	  \"version\" : \"mqtt5.0\","
				+ "\r\n	  \"qos\" : 0"
				+ "\r\n	}"
				+ "\r\n  }"
				+ "\r\n}"
				+ "\r\n}";
		
		//@formatter:on
	}
	
	
	@After
	public void tearDown() {
		subscriptionEntityPayload=null;
		
	}
	
	/**
	 * this method is use for subscribe the entity
	 */
	
	@Test
	public void createSubscriptionEntityTest() {
		try {
			URI uri = new URI("urn:ngsi-ld:Subscription:211");
			when(subscriptionService.subscribe(any())).thenReturn(uri);
			mockMvc.perform(post("/ngsi-ld/v1/subscriptions").contentType(AppConstants.NGB_APPLICATION_JSON)
					.accept(AppConstants.NGB_APPLICATION_JSONLD).content(subscriptionEntityPayload)).
			andExpect(status().isCreated());
			verify(subscriptionService, times(1)).subscribe(any());
		
		} catch (Exception e) {
			Assert.fail();
			e.printStackTrace();
		}
	}
	
	/**
	 * this method is try to subscribe the entity having "BAD REQUEST"
	 */

	@Test
	public void createSubscriptionEntityBadRequestTest() {
		try {
			when(subscriptionService.subscribe(any())).
			thenThrow(new ResponseException(ErrorType.BadRequestData));
			mockMvc.perform(post("/ngsi-ld/v1/subscriptions").contentType(AppConstants.NGB_APPLICATION_JSON)
					.accept(AppConstants.NGB_APPLICATION_JSONLD).content(subscriptionEntityPayload)).andExpect(status().
							isBadRequest()).andExpect(jsonPath("$.title").value("Bad Request Data."));
			
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}
   
	/**
	 * this method is use for the subscribe entity if subscribe entity already exists
	 */
	
	@Test
	public void createSubscriptionEntityAlreadyExistTest() {
		try {
			when(subscriptionService.subscribe(any())).
			thenThrow(new ResponseException(ErrorType.AlreadyExists));
			mockMvc.perform(post("/ngsi-ld/v1/subscriptions").contentType(AppConstants.NGB_APPLICATION_JSON)
					.accept(AppConstants.NGB_APPLICATION_JSONLD).content(subscriptionEntityPayload)).andExpect(status().isConflict()).
							andExpect(jsonPath("$.title").value("Already exists."));
		} catch (Exception e) {
			Assert.fail();
		}
	}
	
	/**
	 * this method is used get the subscribe entity by ID.
	 */

	@Test
	public void getSubscriptionEntityTest() {
		try {
			List<Object> context = new ArrayList<>();	
			Subscription subscription = null;
			subscription = DataSerializer.getSubscription(subscriptionEntityPayload);
			when(subscriptionService.getSubscription(any())).thenReturn(subscription);
			mockMvc.perform(get("/ngsi-ld/v1/subscriptions/urn:ngsi-ld:Subscription:211")
					.accept(AppConstants.NGB_APPLICATION_JSON)).andExpect(status().isOk());
			verify(subscriptionService, times(1)).getSubscription(any());
		   } catch (Exception e) {
			   e.printStackTrace();
		}
	}
	
	/**
	 * this method is used get the subscribe entity.
	 */

	@Test
	public void getSubscriptionEntity() {
		try {

			mockMvc.perform(get("/ngsi-ld/v1/subscriptions/")
					.accept(AppConstants.NGB_APPLICATION_JSON)).andExpect(status().isOk());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}
	
	/**
	 * this method is use for delete subscription
	 */
	
	@Test
	public void deleteSubscriptionTest() {
		try {
			mockMvc.perform(delete("/ngsi-ld/v1/subscriptions/{id}", "urn:ngsi-ld:Subscription:211")
					.contentType(AppConstants.NGB_APPLICATION_JSONLD)).andExpect(status().isNoContent());
			verify(subscriptionService, times(1)).unsubscribe(any());
		} catch (Exception e) {
			Assert.fail();
			e.printStackTrace();
		}
	}
	
	/**
	 * this method is update the subscription
	 */
	
	@Test
	public void updateSubscriptionTest() {
		try {
			mockMvc.perform(patch("/ngsi-ld/v1/subscriptions/urn:ngsi-ld:Subscription:211/")
							.contentType(AppConstants.NGB_APPLICATION_JSON).accept(AppConstants.NGB_APPLICATION_JSONLD)
							.content(subscriptionEntityPayload))
					.andExpect(status().isNoContent());
		} catch (Exception e) {
			Assert.fail();
			e.printStackTrace();
		}
		
	}
	
	/**
	 * this method is update the subscription
	 */
	
	@Test
	public void updateSubscriptionNotFoundTest() {
		try {
			when(subscriptionService.updateSubscription(any())).
			thenThrow(new ResponseException(ErrorType.NotFound));
			mockMvc.perform(patch("/ngsi-ld/v1/subscriptions/urn:ngsi-ld:Subscription:211/")
							.contentType(AppConstants.NGB_APPLICATION_JSON).accept(AppConstants.NGB_APPLICATION_JSONLD)
							.content(subscriptionEntityPayload))
					.andExpect(status().isNotFound());
		} catch (Exception e) {
			Assert.fail();
			e.printStackTrace();
		}
		
	}
	
	/**
	 * this method is used get the subscribe entity by Id.
	 * @throws Exception 
	 */

	@Test
	public void getSubscriptionEntityById() throws Exception {
		when(subscriptionService.getSubscription(any())).
		thenThrow(new ResponseException(ErrorType.NotFound));
			mockMvc.perform(get("/ngsi-ld/v1/subscriptions/urn:ngsi-ld:Subscription:211")
					.accept(AppConstants.NGB_APPLICATION_JSON)).andExpect(status().isNotFound());

	}
}
