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
    
	private String payload;
	private String deletePayload;
	public static boolean checkEntity = false;
	
	@Before
	public void setup() throws Exception {
		//@formatter:off
		
		payload= "[  \r\n" +
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
		
		deletePayload = "[  \r\n" +
							"\"urn:ngsi-ld:Vehicle:A101\",\r\n" + 
						    "\"urn:ngsi-ld:Vehicle:A102\"\r\n" + 
						"]";
				//@formatter:on
	}
	@After
	public void tearDown() {
		payload = null;
		deletePayload = null;
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
					.accept(AppConstants.NGB_APPLICATION_JSONLD).content(payload))
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
					.accept(AppConstants.NGB_APPLICATION_JSONLD).content(payload))
					.andExpect(status().isMultiStatus());
			verify(entityService, times(1)).createMultipleMessage(any());	
		} catch (Exception e) {
			Assert.fail();
			e.printStackTrace();
		}
	}
	
	/**
	 * this method is validate the bad request if create the multiple entity 
	 * but some entity request is not valid
	 */
	@Test
	public void createMultipleEntityBadRequestTest() {
		try {

			BatchResult batchResult = new BatchResult();
			when(entityService.createMultipleMessage(any())).thenReturn(batchResult);
			mockMvc.perform(post("/ngsi-ld/v1/entityOperations/create").contentType(AppConstants.NGB_APPLICATION_JSON)
					.accept(AppConstants.NGB_APPLICATION_JSONLD).content(payload))
					.andExpect(status().isBadRequest());
			verify(entityService, times(1)).createMultipleMessage(any());
		} catch (Exception e) {
			Assert.fail();
			e.printStackTrace();
		}
	}
	
	/**
	 * this method is use for update the multiple entity.
	 */
	@Test
	public void updateMultipleEntityTest() {
		try {
			
			BatchResult batchResult = new BatchResult();
			batchResult.addSuccess("urn:ngsi-ld:Vehicle:A101");
			batchResult.addSuccess("urn:ngsi-ld:Vehicle:A102");
			when(entityService.updateMultipleMessage(any())).thenReturn(batchResult);
			mockMvc.perform(post("/ngsi-ld/v1/entityOperations/update").contentType(AppConstants.NGB_APPLICATION_JSON)
					.accept(AppConstants.NGB_APPLICATION_JSONLD).content(payload))
					.andExpect(status().isNoContent());
			verify(entityService, times(1)).updateMultipleMessage(any());	
		} catch (Exception e) {
			Assert.fail();
			e.printStackTrace();
		}
	}
	
	/**
	 * this method is use for create the multiple entity but someone entity not exist.
	 */
	@Test
	public void updateMultipleEntityIfEntityNotExistTest() {
		try {
			
			BatchResult batchResult = new BatchResult();
			batchResult.addSuccess("urn:ngsi-ld:Vehicle:A101");
			batchResult.addSuccess("urn:ngsi-ld:Vehicle:A110");
			RestResponse restResponse = new RestResponse(ErrorType.NotFound,"Resource not found.");
			BatchFailure batchFailure = new BatchFailure("urn:ngsi-ld:Vehicle:A110",restResponse);
			batchResult.addFail(batchFailure);
			when(entityService.updateMultipleMessage(any())).thenReturn(batchResult);
			mockMvc.perform(post("/ngsi-ld/v1/entityOperations/update").contentType(AppConstants.NGB_APPLICATION_JSON)
					.accept(AppConstants.NGB_APPLICATION_JSONLD).content(payload))
					.andExpect(status().isMultiStatus());
			verify(entityService, times(1)).updateMultipleMessage(any());	
		} catch (Exception e) {
			Assert.fail();
			e.printStackTrace();
		}
	}
	
	/**
	 * this method is validate the bad request if update the multiple entity 
	 * but some entity request is not valid
	 */
	@Test
	public void updateMultipleEntityBadRequestTest() {
		try {

			BatchResult batchResult = new BatchResult();
			when(entityService.updateMultipleMessage(any())).thenReturn(batchResult);
			mockMvc.perform(post("/ngsi-ld/v1/entityOperations/update").contentType(AppConstants.NGB_APPLICATION_JSON)
					.accept(AppConstants.NGB_APPLICATION_JSONLD).content(payload))
					.andExpect(status().isBadRequest());
			verify(entityService, times(1)).updateMultipleMessage(any());
		} catch (Exception e) {
			Assert.fail();
			e.printStackTrace();
		}
	}
    
	/**
	 * this method is use for upsert the multiple entity if all entities already exist.
	 */
	@Test
	public void upsertMultipleEntityTest() {
		try {
			
			BatchResult batchResult = new BatchResult();
			batchResult.addSuccess("urn:ngsi-ld:Vehicle:A101");
			batchResult.addSuccess("urn:ngsi-ld:Vehicle:A102");
			EntityService.checkEntity = false;
			when(entityService.upsertMultipleMessage(any())).thenReturn(batchResult);
			mockMvc.perform(post("/ngsi-ld/v1/entityOperations/upsert").contentType(AppConstants.NGB_APPLICATION_JSON)
					.accept(AppConstants.NGB_APPLICATION_JSONLD).content(payload))
					.andExpect(status().isNoContent());
			verify(entityService, times(1)).upsertMultipleMessage(any());	
		} catch (Exception e) {
			Assert.fail();
			e.printStackTrace();
		}
	}
	
	/**
	 * this method is use for upsert the multiple entity if someone entity not exist
	 */
	@Test
	public void upsertMultipleEntityIfEntityNotExistTest() {
		try {
			
			BatchResult batchResult = new BatchResult();
			batchResult.addSuccess("urn:ngsi-ld:Vehicle:A101");
			EntityService.checkEntity = true;
			when(entityService.upsertMultipleMessage(any())).thenReturn(batchResult);
			mockMvc.perform(post("/ngsi-ld/v1/entityOperations/upsert").contentType(AppConstants.NGB_APPLICATION_JSON)
					.accept(AppConstants.NGB_APPLICATION_JSONLD).content(payload))
					.andExpect(status().isCreated());
			verify(entityService, times(1)).upsertMultipleMessage(any());	
		} catch (Exception e) {
			Assert.fail();
			e.printStackTrace();
		}
	}

	/**
	 * this method is use for delete the multiple entity
	 */
	@Test
	public void deleteMultipleEntityTest() {
		try {

			BatchResult batchResult = new BatchResult();
			batchResult.addSuccess("urn:ngsi-ld:Vehicle:A101");
			batchResult.addSuccess("urn:ngsi-ld:Vehicle:A102");
			when(entityService.deleteMultipleMessage(any())).thenReturn(batchResult);
			mockMvc.perform(post("/ngsi-ld/v1/entityOperations/delete").contentType(AppConstants.NGB_APPLICATION_JSON)
					.accept(AppConstants.NGB_APPLICATION_JSONLD).content(deletePayload))
					.andExpect(status().isNoContent());
			verify(entityService, times(1)).deleteMultipleMessage(any());
		} catch (Exception e) {
			Assert.fail();
			e.printStackTrace();
		}
	}
	
	/**
	 * this method is use for delete the multiple entity but someone entity not exist.
	 */
	@Test
	public void deleteMultipleEntityIfEntityNotExistTest() {
		try {
			
			BatchResult batchResult = new BatchResult();
			batchResult.addSuccess("urn:ngsi-ld:Vehicle:A101");
			RestResponse restResponse = new RestResponse(ErrorType.NotFound,"Resource not found.");
			BatchFailure batchFailure = new BatchFailure("urn:ngsi-ld:Vehicle:A104",restResponse);
			batchResult.addFail(batchFailure);
			when(entityService.deleteMultipleMessage(any())).thenReturn(batchResult);
			mockMvc.perform(post("/ngsi-ld/v1/entityOperations/delete").contentType(AppConstants.NGB_APPLICATION_JSON)
					.accept(AppConstants.NGB_APPLICATION_JSONLD).content(deletePayload))
					.andExpect(status().isMultiStatus());
			verify(entityService, times(1)).deleteMultipleMessage(any());
		} catch (Exception e) {
			Assert.fail();
			e.printStackTrace();
		}
	}
}
