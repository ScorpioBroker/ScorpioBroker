package eu.neclab.ngsildbroker.entityhandler.controller;

import static org.mockito.ArgumentMatchers.any;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.BatchFailure;
import eu.neclab.ngsildbroker.commons.datatypes.BatchResult;
import eu.neclab.ngsildbroker.commons.datatypes.RestResponse;
import eu.neclab.ngsildbroker.entityhandler.services.EntityService;

@SpringBootTest(properties= {"spring.main.allow-bean-definition-overriding=true"})
@RunWith(SpringRunner.class)
@AutoConfigureMockMvc//(secure = false)
public class EntityBatchControllerTest {

	@Autowired
    private MockMvc mockMvc;
    @MockBean
    private EntityService entityService;
    
	private String createPayload;
	
	@Before
	public void setup() throws Exception {
		//@formatter:off
		
		createPayload= "[  \r\n" +
					       "{  \r\n" +
							"   \"id\":\"urn:ngsi-ld:Vehicle:A101\",\r\n" + 
							"   \"type\":\"Vehicle\",\r\n" + 
							"   \"brandName\":\r\n" + 
							"      {  \r\n" + 
							"         \"type\":\"Property\",\r\n" + 
							"         \"value\":\"Mercedes\"\r\n" + 
							"      },\r\n" + 
							"   \"speed\":{  \r\n" + 
							"         \"type\":\"Property\",\r\n" + 
							"         \"value\":80\r\n" + 
							"    }\r\n" +
							"},\r\n" +
						    "{  \r\n" +
							"   \"id\":\"urn:ngsi-ld:Vehicle:A102\",\r\n" + 
							"   \"type\":\"Vehicle\",\r\n" + 
							"   \"brandName\":\r\n" + 
							"      {  \r\n" + 
							"         \"type\":\"Property\",\r\n" + 
							"         \"value\":\"Mercedes\"\r\n" + 
							"      },\r\n" + 
							"   \"speed\":{  \r\n" + 
							"         \"type\":\"Property\",\r\n" + 
							"         \"value\":81\r\n" + 
							"    }\r\n" +
							"},\r\n" +
							"{  \r\n" +
							"   \"id\":\"urn:ngsi-ld:Vehicle:A103\",\r\n" + 
							"   \"type\":\"Vehicle\",\r\n" + 
							"   \"brandName\":\r\n" + 
							"      {  \r\n" + 
							"         \"type\":\"Property\",\r\n" + 
							"         \"value\":\"Mercedes\"\r\n" + 
							"      },\r\n" + 
							"   \"speed\":{  \r\n" + 
							"         \"type\":\"Property\",\r\n" + 
							"         \"value\":82\r\n" + 
							"    }\r\n" +
							"}\r\n" +
					"]";
		
				//@formatter:on
	}
	@After
	public void tearDown() {
		createPayload = null;
	}
	
	/**
	 * this method is use for create the multiple entity
	 */
	@Test
	public void createMultipleEntityTest() {
		try {

			BatchResult batchResult = new BatchResult();
			batchResult.addSuccess("urn:ngsi-ld:Vehicle:A101");
			batchResult.addSuccess("urn:ngsi-ld:Vehicle:A102");
			batchResult.addSuccess("urn:ngsi-ld:Vehicle:A103");
			when(entityService.createMultipleMessage(any())).thenReturn(batchResult);
			mockMvc.perform(post("/ngsi-ld/v1/entityOperations/create").contentType(AppConstants.NGB_APPLICATION_JSON)
					.accept(AppConstants.NGB_APPLICATION_JSONLD).content(createPayload))
					.andExpect(status().isCreated());
			verify(entityService, times(1)).createMultipleMessage(any());
		} catch (Exception e) {
			Assert.fail();
			e.printStackTrace();
		}
	}
	
	/**
	 * this method is use for create the multiple entity but someone entity already exist.
	 */
	@Test
	public void createMultipleEntityIfEntityAlreadyExistTest() {
		try {
			
			BatchResult batchResult = new BatchResult();
			batchResult.addSuccess("urn:ngsi-ld:Vehicle:A101");
			batchResult.addSuccess("urn:ngsi-ld:Vehicle:A102");
			RestResponse restResponse = new RestResponse(ErrorType.AlreadyExists,"Already exists.");
			BatchFailure batchFailure = new BatchFailure("urn:ngsi-ld:Vehicle:A103",restResponse);
			batchResult.addFail(batchFailure);
			when(entityService.createMultipleMessage(any())).thenReturn(batchResult);
			mockMvc.perform(post("/ngsi-ld/v1/entityOperations/create").contentType(AppConstants.NGB_APPLICATION_JSON)
					.accept(AppConstants.NGB_APPLICATION_JSONLD).content(createPayload))
					.andExpect(status().isMultiStatus());
			verify(entityService, times(1)).createMultipleMessage(any());	
		} catch (Exception e) {
			Assert.fail();
			e.printStackTrace();
		}
	}

}
