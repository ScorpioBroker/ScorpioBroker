
package eu.neclab.ngsildbroker.subscriptionmanager.controller;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
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
import eu.neclab.ngsildbroker.commons.ngsiqueries.ParamsResolver;
import eu.neclab.ngsildbroker.subscriptionmanager.service.SubscriptionService;
@SpringBootTest(properties = { "spring.main.allow-bean-definition-overriding=true" })
@RunWith(SpringRunner.class)
@AutoConfigureMockMvc // (secure = false) public class
public class NotificationControllerTest {
	private String qPayload;
	private String ePayload;

	@Autowired
	private MockMvc mockMvc;

	@Mock
	private ParamsResolver paramResolver;

	@MockBean
	private SubscriptionService ss;

	public static boolean checkEntity = false;

	@Before
	public void setup() throws Exception { //@formatter:off
  
			qPayload ="{\r\n"
					+ "        \"id\": \"ngsildbroker:notification:-4670513482090232260\",\r\n"
					+ "        \"type\": \"Notification\",\r\n"
					+ "        \"subscriptionId\": \"urn:ngsi-ld:Subscription:storeSubscription\",\r\n"
					+ "        \"notifiedAt\": \"2022-01-30T21:23:50Z\",\r\n"
					+ "        \"data\": [ {\r\n"
					+ "  \"id\" : \"urn:ngsi-ld:test1\",\r\n"
					+ "  \"type\" : \"Test2\",\r\n"
					+ "  \"createdAt\" : \"2022-01-30T21:21:29Z\",\r\n"
					+ "  \"blubdiblub\" : {\r\n"
					+ "    \"type\" : \"Property\",\r\n"
					+ "    \"value\" : 200\r\n"
					+ "  },\r\n"
					+ "  \"name\" : {\r\n"
					+ "    \"type\" : \"Property\",\r\n"
					+ "    \"value\" : \"6-Stars\"\r\n"
					+ "  },\r\n"
					+ "  \"speed\" : {\r\n"
					+ "    \"type\" : \"Property\",\r\n"
					+ "    \"value\" : 202\r\n"
					+ "  },\r\n"
					+ "  \"location\" : {\r\n"
					+ "    \"type\" : \"GeoProperty\",\r\n"
					+ "    \"value\" : {\r\n"
					+ "      \"type\" : \"Point\",\r\n"
					+ "      \"coordinates\" : [ 100, 100 ]\r\n"
					+ "    }\r\n"
					+ "  },\r\n"
					+ "  \"modifiedAt\" : \"2022-01-30T21:21:29Z\"\r\n"
					+ "} ]\r\n"
					+ "}";
			
			ePayload ="";
			  
			  //@formatter:on 

	}

	@After
	public void tearDown() {
		ePayload = null;
		qPayload = null;
	}
	// setup close

	/**
	 * this method is use for notify the subscription
	 */
	@Test
	public void notifyTest() {
		try {
			SubscriptionService subscriptionService = new SubscriptionService();
			SubscriptionService spy = Mockito.spy(subscriptionService);
			Mockito.doNothing().when(spy).remoteNotify(any(), any());
			ResultActions resultAction = mockMvc.perform(
					post("/remotenotify/{id}", "-4670513482090232260").contentType(AppConstants.NGB_APPLICATION_JSON)
							.accept(AppConstants.NGB_APPLICATION_JSONLD).content(qPayload))
					.andExpect(status().isOk());
			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			Assert.assertEquals(200, status);
			verify(ss, times(1)).remoteNotify(any(), any());

		} catch (Exception e) {
			Assert.fail();
			e.printStackTrace();
		}
	}

	/**
	 * this method is use for validate the BadRequest Data
	 */
	@Test
	public void notifyBadRequestTest() {
		try {
			SubscriptionService subscriptionService = new SubscriptionService();
			SubscriptionService spy = Mockito.spy(subscriptionService);
			Mockito.doNothing().when(spy).remoteNotify(any(), any());
			ResultActions resultAction = mockMvc.perform(
					post("/remotenotify/{id}", "-4670513482090232260").contentType(AppConstants.NGB_APPLICATION_JSON)
							.accept(AppConstants.NGB_APPLICATION_JSONLD).content(ePayload))
					.andExpect(status().isBadRequest());
			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			Assert.assertEquals(400, status);
			verify(ss, times(0)).remoteNotify(any(), any());
		} catch (Exception e) {
			Assert.fail();
			e.printStackTrace();
		}
	}

}
