
package eu.neclab.ngsildbroker.subscriptionmanager.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.ResultActions;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.subscriptionbase.BaseSubscriptionService;
import eu.neclab.ngsildbroker.subscriptionmanager.service.SubscriptionService;
import junit.framework.Assert;

@SpringBootTest(properties = { "spring.main.allow-bean-definition-overriding=true" })

@RunWith(SpringRunner.class)

@AutoConfigureMockMvc
public class SubscriptionControllerTest {

	@Autowired
	private MockMvc mockMvc;

	@MockBean
	private SubscriptionService subscriptionService;

	@MockBean
	private QueryParams qp;

	@InjectMocks
	private SubscriptionController subscriptionController;

	private String subscriptionEntityPayload;
	private String subscriptionEntityPayload1;

	@Before
	public void setup() throws Exception {
		MockitoAnnotations.initMocks(this);

	// @formatter:off
  
  subscriptionEntityPayload = "{" +
  "\r\n\"id\": \"urn:ngsi-ld:Subscription:211\"," +
  "\r\n\"type\": \"Subscription\"," + "\r\n\"entities\": [{" +
  "\r\n		  \"id\": \"urn:ngsi-ld:Vehicle:A143\"," +
  "\r\n		  \"type\": \"Vehicle\"" + "\r\n		}]," +
  "\r\n\"watchedAttributes\": [\"brandName\"]," +
  "\r\n		\"q\":\"brandName!=Mercedes\"," + "\r\n\"notification\": {" +
  "\r\n  \"attributes\": [\"brandName\"]," + "\r\n  \"format\": \"keyValues\","
  + "\r\n  \"endpoint\": {" +
  "\r\n   \"uri\": \"mqtt://localhost:1883/notify\"," +
  "\r\n   \"accept\": \"application/json\"," + "\r\n	\"notifierinfo\": {" +
  "\r\n	  \"version\" : \"mqtt5.0\"," + "\r\n	  \"qos\" : 0" + "\r\n	}" +
  "\r\n  }" + "\r\n}" + "\r\n}";
  
  
  subscriptionEntityPayload1 = "{" +
		  "\r\n\"id\": \"urn:ngsi-ld:Subscription:211\"," +
		  "\r\n\"type\": \"Subscription\"," + "\r\n\"entities\": [{" +
		  "\r\n		  \"id\": \"urn:ngsi-ld:Vehicle:143\"," +
		  "\r\n		  \"type\": \"Vehicle\"" + "\r\n		}]," +
		  "\r\n\"watchedAttributes\": [\"brandName\"]," +
		  "\r\n		\"q\":\"brandName!=Mercedes\"," + "\r\n\"notification\": {" +
		  "\r\n  \"attributes\": [\"brandName\"]," + "\r\n  \"format\": \"keyValues\","
		  + "\r\n  \"endpoint\": {" +
		  "\r\n   \"uri\": \"mqtt://localhost:1883/notify\"," +
		  "\r\n   \"accept\": \"application/json\"," + "\r\n	\"notifierinfo\": {" +
		  "\r\n	  \"version\" : \"mqtt5.0\"," + "\r\n	  \"qos\" : 0" + "\r\n	}" +
		  "\r\n  }" + "\r\n}" + "\r\n}";
  
  // @formatter:on 
	}

	@After
	public void tearDown() {
		subscriptionEntityPayload = null;
	}

	/**
	 * this method is use for subscribe the entity
	 */

	@Test
	public void createSubscriptionEntityTest() {
		try {
			when(subscriptionService.subscribe(any())).thenReturn("urn:ngsi-ld:Subscription:211");
			ResultActions resultAction = mockMvc
					.perform(post("/ngsi-ld/v1/subscriptions").contentType(AppConstants.NGB_APPLICATION_JSON)
							.accept(AppConstants.NGB_APPLICATION_JSONLD).content(subscriptionEntityPayload))
					.andExpect(status().isCreated());

			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			assertEquals(201, status);
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
			when(subscriptionService.subscribe(any()))
					.thenThrow(new ResponseException(ErrorType.BadRequestData, "Bad Request Data."));
			ResultActions resultAction = mockMvc
					.perform(post("/ngsi-ld/v1/subscriptions").contentType(AppConstants.NGB_APPLICATION_JSON)
							.accept(AppConstants.NGB_APPLICATION_JSONLD).content(subscriptionEntityPayload))
					.andExpect(status().isBadRequest()).andExpect(jsonPath("$.title").value("Bad Request Data."));
			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			assertEquals(400, status);
			verify(subscriptionService, times(1)).subscribe(any());

		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	/**
	 * this method is use for the subscribe entity if subscribe entity already
	 * exists
	 */

	@Test
	public void createSubscriptionEntityAlreadyExistTest() {
		try {
			when(subscriptionService.subscribe(any()))
					.thenThrow(new ResponseException(ErrorType.AlreadyExists, "Already exists."));
			ResultActions resultAction = mockMvc
					.perform(post("/ngsi-ld/v1/subscriptions").contentType(AppConstants.NGB_APPLICATION_JSON)
							.accept(AppConstants.NGB_APPLICATION_JSONLD).content(subscriptionEntityPayload))
					.andExpect(status().isConflict()).andExpect(jsonPath("$.title").value("Already exists."));
			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			assertEquals(409, status);
			verify(subscriptionService, times(1)).subscribe(any());

		} catch (Exception e) {
			Assert.fail();
		}
	}

	/**
	 * this method is used get the subscribe entity by Id.
	 * 
	 * @throws Exception
	 */

	@Test
	public void getSubscriptionEntityByIdNotFoundTest() throws Exception {
		when(subscriptionService.getSubscription(any(), any()))
				.thenThrow(new ResponseException(ErrorType.NotFound, ""));
		ResultActions resultAction = mockMvc.perform(
				get("/ngsi-ld/v1/subscriptions/urn:ngsi-ld:Subscription:211").accept(AppConstants.NGB_APPLICATION_JSON))
				.andExpect(status().isNotFound());

		MvcResult mvcResult = resultAction.andReturn();
		MockHttpServletResponse response = mvcResult.getResponse();
		int status = response.getStatus();
		assertEquals(404, status);
		verify(subscriptionService, times(1)).getSubscription(any(), any());
	}



	/**
	 * this method is use for delete subscription
	 */

	@Test
	public void deleteSubscriptionTest() {
		try {
			ResultActions resultAction = mockMvc
					.perform(delete("/ngsi-ld/v1/subscriptions/{id}", "urn:ngsi-ld:Subscription:211")
							.contentType(AppConstants.NGB_APPLICATION_JSONLD))
					.andExpect(status().isNoContent());
			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			assertEquals(204, status);

			verify(subscriptionService, times(1)).unsubscribe(any(), any());

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
			BaseSubscriptionService bss = mock(BaseSubscriptionService.class);
			doNothing().when(bss).updateSubscription(any());
			ResultActions resultAction = mockMvc
					.perform(patch("/ngsi-ld/v1/subscriptions/urn:ngsi-ld:Subscription:211/")
							.contentType(AppConstants.NGB_APPLICATION_JSON).accept(AppConstants.NGB_APPLICATION_JSONLD)
							.content(subscriptionEntityPayload))
					.andExpect(status().isNoContent());

			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			assertEquals(204, status);

			verify(subscriptionService, times(1)).updateSubscription(any());

		} catch (Exception e) {
			Assert.fail();
			e.printStackTrace();
		}

	}

	/**
	 * this method is update the subscription
	 */

	@Test
	public void updateSubscriptionBadRequestTest() {
		try {
			doNothing().when(subscriptionService).updateSubscription(any());
			ResultActions resultAction = mockMvc
					.perform(patch("/ngsi-ld/v1/subscriptions/urn:ngsi-ld:Subscription:212/")
							.contentType(AppConstants.NGB_APPLICATION_JSON).accept(AppConstants.NGB_APPLICATION_JSONLD)
							.content(subscriptionEntityPayload))
					.andExpect(status().isBadRequest());

			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			assertEquals(400, status);
			verify(subscriptionService, times(0)).updateSubscription(any());

		} catch (Exception e) {
			Assert.fail();
			e.printStackTrace();
		}

	}

}