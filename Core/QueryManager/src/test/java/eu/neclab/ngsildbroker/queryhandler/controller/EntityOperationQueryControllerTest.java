
package eu.neclab.ngsildbroker.queryhandler.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
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
import eu.neclab.ngsildbroker.commons.datatypes.results.QueryResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.ngsiqueries.ParamsResolver;
import eu.neclab.ngsildbroker.queryhandler.services.QueryService;
 

@SpringBootTest(properties = { "spring.main.allow-bean-definition-overriding=true" })

@RunWith(SpringRunner.class)

@AutoConfigureMockMvc // (secure = false) public class
public class EntityOperationQueryControllerTest {
	private String payload;
	private String deletePayload;
	private String qPayload;
	private String ePayload;

	@Autowired
	private MockMvc mockMvc;

	@Mock
	private ParamsResolver paramResolver;

	@MockBean
	private QueryService queryService;

	

	
	public static boolean checkEntity = false;

	@Before
	public void setup() throws Exception { //@formatter:off
  
				  payload= "[  \r\n" + "{  \r\n" +
							  "   \"id\":\"urn:ngsi-ld:Vehicle:A101\",\r\n" +
							  "   \"type\":\"Vehicle\",\r\n" + "   \"brandName\":\r\n" + "      {  \r\n" +
							  "         \"type\":\"Property\",\r\n" + "         \"value\":\"Mercedes\"\r\n"
							  + "      },\r\n" + "   \"speed\":{  \r\n" +
							  "         \"type\":\"Property\",\r\n" + "         \"value\":80\r\n" +
							  "    }\r\n" + "},\r\n" + "{  \r\n" +
							  "   \"id\":\"urn:ngsi-ld:Vehicle:A102\",\r\n" +
							  "   \"type\":\"Vehicle\",\r\n" + "   \"brandName\":\r\n" + "      {  \r\n" +
							  "         \"type\":\"Property\",\r\n" + "         \"value\":\"Mercedes\"\r\n"
							  + "      },\r\n" + "   \"speed\":{  \r\n" +
							  "         \"type\":\"Property\",\r\n" + "         \"value\":81\r\n" +
							  "    }\r\n" + "},\r\n" + "{  \r\n" +
							  "   \"id\":\"urn:ngsi-ld:Vehicle:A103\",\r\n" +
							  "   \"type\":\"Vehicle\",\r\n" + "   \"brandName\":\r\n" + "      {  \r\n" +
							  "         \"type\":\"Property\",\r\n" + "         \"value\":\"Mercedes\"\r\n"
							  + "      },\r\n" + "   \"speed\":{  \r\n" +
							  "         \"type\":\"Property\",\r\n" + "         \"value\":82\r\n" +
							  "    }\r\n" + "}\r\n" + "]";
							  
			  deletePayload = "[  \r\n" + "\"urn:ngsi-ld:Vehicle:A101\",\r\n" +
							    "\"urn:ngsi-ld:Vehicle:A102\"\r\n" + "]"; 
  
			qPayload ="{\r\n"
					+ " \"type\": \"Query\",\r\n"
					+ "  \"entities\": [\r\n"
					+ "    {\r\n"
					+ "   \"type\":\"Vehicle\"\r\n"
					+ "   }]\r\n"
					+ "  \r\n"
					+ "}";
			
			ePayload ="";
			  
			  //@formatter:on 

	}

	@After
	public void tearDown() {
		payload = null;
		deletePayload = null;

	}
	// setup close


	
	@Test
	public void postQueryTest() {
		try {
		   QueryResult resutl = mock(QueryResult.class);
		   when(queryService.getData(any(), any(), any(), any(), any())).thenReturn(resutl);
			ResultActions resultAction = mockMvc.perform(
					post("/ngsi-ld/v1/entityOperations/query").contentType(AppConstants.NGB_APPLICATION_JSON)
							.accept(AppConstants.NGB_APPLICATION_JSONLD).content(qPayload))
					.andExpect(status().isOk());
			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			Assert.assertEquals(200, status);
			verify(queryService, times(1)).getData(any(), any(), any(), any(), any());
		} catch (Exception e) {
			Assert.fail();
			e.printStackTrace();
		}
	}
	
	@Test
	public void postQueryBadRequestTest() {
		try {
		   QueryResult resutl = mock(QueryResult.class);
		   when(queryService.getData(any(), any(), any(), any(), any())).thenThrow(new ResponseException(ErrorType.BadRequestData,""));
			ResultActions resultAction = mockMvc.perform(
					post("/ngsi-ld/v1/entityOperations/query").contentType(AppConstants.NGB_APPLICATION_JSON)
							.accept(AppConstants.NGB_APPLICATION_JSONLD).content(qPayload))
					.andExpect(status().isBadRequest());
			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			Assert.assertEquals(400, status);
			verify(queryService, times(1)).getData(any(), any(), any(), any(), any());
		} catch (Exception e) {
			Assert.fail();
			e.printStackTrace();
		}
	}

}
