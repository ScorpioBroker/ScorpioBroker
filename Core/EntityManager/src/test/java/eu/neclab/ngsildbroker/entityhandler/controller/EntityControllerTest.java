package eu.neclab.ngsildbroker.entityhandler.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.redirectedUrl;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import org.junit.After;
import org.junit.Before;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.AppendResult;
import eu.neclab.ngsildbroker.commons.datatypes.UpdateResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.entityhandler.services.EntityService;


@SpringBootTest(properties= {"spring.main.allow-bean-definition-overriding=true"})
@RunWith(SpringRunner.class)
@AutoConfigureMockMvc//(secure = false)
public class EntityControllerTest {
	@Autowired
    private MockMvc mockMvc;
    @MockBean
    private EntityService entityService;
    
	private String appendPayload;
	private String updatePayload;
	private String entityPayload;
	private String partialUpdatePayload;
	private String partialUpdateDefaultCasePayload;

	@Before
	public void setup() throws Exception {
		//@formatter:off
		
		appendPayload="{\r\n" +
				"	\"brandName1\": {\r\n" + 
				"		\"type\": \"Property\",\r\n" + 
				"		\"value\": \"BMW\"\r\n" + 
				"	}\r\n" + 
				"}";
		
		updatePayload="{\r\n" + 
				"	\"brandName1\": {\r\n" + 
				"		\"type\": \"Property\",\r\n" + 
				"		\"value\": \"Audi\"\r\n" + 
				"	}\r\n" + 
				"}";
		partialUpdatePayload= "{\r\n" + 
				"		\"value\": 20,\r\n" + 
				"		\"datasetId\": \"urn:ngsi-ld:Property:speedometerA4567-speed\"\r\n" + 
				"}";
		
		partialUpdateDefaultCasePayload= "{\r\n" + 
				"		\"value\": 11\r\n" +
				"}";
		
		entityPayload= "{  \r\n" + 
				"   \"id\":\"urn:ngsi-ld:Vehicle:A101\",\r\n" + 
				"   \"type\":\"Vehicle\",\r\n" + 
				"   \"brandName\":\r\n" + 
				"      {  \r\n" + 
				"         \"type\":\"Property\",\r\n" + 
				"         \"value\":\"Mercedes\"\r\n" + 
				"      },\r\n" + 
				"   \"speed\":[{  \r\n" + 
				"         \"type\":\"Property\",\r\n" + 
				"         \"value\":55,\r\n" + 
				"         \"datasetId\":\"urn:ngsi-ld:Property:speedometerA4567-speed\",\r\n" + 
				"   \"source\":\r\n" + 
				"      {  \r\n" + 
				"         \"type\":\"Property\",\r\n" + 
				"         \"value\":\"Speedometer\"\r\n" + 
				"      }\r\n" + 
				"      },\r\n" + 
				"      {  \r\n" + 
				"         \"type\":\"Property\",\r\n" + 
				"         \"value\":60,\r\n" +
				"   \"source\":\r\n" + 
				"      {  \r\n" + 
				"         \"type\":\"Property\",\r\n" + 
				"         \"value\":\"GPS\"\r\n" + 
				"      }\r\n" + 
				"      },\r\n" + 
				"      {  \r\n" + 
				"         \"type\":\"Property\",\r\n" + 
				"         \"value\":52.5,\r\n" +
				"   \"source\":\r\n" + 
				"      {  \r\n" + 
				"         \"type\":\"Property\",\r\n" + 
				"         \"value\":\"GPS_NEW\"\r\n" + 
				"      }\r\n" + 
				"      }],\r\n" +
				"      \"createdAt\":\"2017-07-29T12:00:04Z\",\r\n" + 
				"      \"modifiedAt\":\"2017-07-29T12:00:04Z\",\r\n" + 
				"   \"location\":\r\n" + 
				"      {  \r\n" + 
				"      \"type\":\"GeoProperty\",\r\n" +
				"	\"value\": \"{ \\\"type\\\": \\\"Point\\\", \\\"coordinates\\\": [ -8.5, 41.2]}\""+ 
				"      }\r\n" +
				"}";
		
		//@formatter:on
	}
	
	
	@After
	public void tearDown() {
		appendPayload=null;
		updatePayload=null;
		entityPayload=null;
		partialUpdatePayload=null;
		partialUpdateDefaultCasePayload=null;
	}
    
	/**
	 * this method is use for create the entity
	 */
	@Test
	public void createEntityTest() {
		try {
			when(entityService.createMessage(any())).thenReturn("urn:ngsi-ld:Vehicle:A101");
			mockMvc.perform(post("/ngsi-ld/v1/entities/").contentType(AppConstants.NGB_APPLICATION_JSON)
					.accept(AppConstants.NGB_APPLICATION_JSONLD).content(entityPayload)).andExpect(status().isCreated())
					.andExpect(redirectedUrl("/ngsi-ld/v1/entities/urn:ngsi-ld:Vehicle:A101"));
			verify(entityService, times(1)).createMessage(any());
		} catch (Exception e) {
			Assert.fail();
			e.printStackTrace();
		}
	}

	/**
	 * this method is use for the entity if entity already exist
	 */
	@Test
	public void createEntityAlreadyExistTest() {
		try {
			when(entityService.createMessage(any())).thenThrow(new ResponseException(ErrorType.AlreadyExists));
			mockMvc.perform(post("/ngsi-ld/v1/entities/").contentType(AppConstants.NGB_APPLICATION_JSON)
					.accept(AppConstants.NGB_APPLICATION_JSONLD).content(entityPayload))
					.andExpect(status().isConflict()).andExpect(jsonPath("$.title").value("Already exists."));
			verify(entityService, times(1)).createMessage(any());
		} catch (Exception e) {
			Assert.fail();
		}
	}

	/**
	 * this method is validate for the bad request if create the entity 
	 */
	@Test
	public void createEntityBadRequestTest() {
		try {
			when(entityService.createMessage(any())).thenThrow(new ResponseException(ErrorType.BadRequestData));
			mockMvc.perform(post("/ngsi-ld/v1/entities/").contentType(AppConstants.NGB_APPLICATION_JSON)
					.accept(AppConstants.NGB_APPLICATION_JSONLD).content(entityPayload))
					.andExpect(status().isBadRequest()).andExpect(jsonPath("$.title").value("Bad Request Data."));
			verify(entityService, times(1)).createMessage(any());
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}

	/**
	 * this method is validate for throw the exception if create the entity
	 */
	@Test
	public void createEntity500ExceptionTest() {
		try {
			when(entityService.createMessage(any())).thenThrow(new Exception());
			mockMvc.perform(post("/ngsi-ld/v1/entities/").contentType(AppConstants.NGB_APPLICATION_JSON)
					.accept(AppConstants.NGB_APPLICATION_JSONLD).content(entityPayload))
					.andExpect(status().isInternalServerError()).andExpect(jsonPath("$.title").value("Internal error"));
			verify(entityService, times(1)).createMessage(any());
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}

	/**
	 * this method is use for append  the attribute in entity
	 */
	@Test
	public void appendEntityNoContentTest() {
		try {
			AppendResult appendResult = Mockito.mock(AppendResult.class);
			when(entityService.appendMessage(any(), any(), any())).thenReturn(appendResult);
			when(appendResult.getAppendResult()).thenReturn(true);
			mockMvc.perform(post("/ngsi-ld/v1/entities/{entityId}/attrs", "urn:ngsi-ld:Vehicle:A101")
					.contentType(AppConstants.NGB_APPLICATION_JSON).accept(AppConstants.NGB_APPLICATION_JSONLD)
					.content(appendPayload)).andExpect(status().isNoContent());
			verify(entityService, times(1)).appendMessage(any(), any(), any());
		} catch (Exception e) {
			Assert.fail();
			e.printStackTrace();
		}
	}

	/**
	 * this method is use for append the attribute in entity for multi status
	 */
	@Test
	public void appendEntityMultiStatusTest() {
		try {
			AppendResult appendResult = Mockito.mock(AppendResult.class);
			when(entityService.appendMessage(any(), any(), any())).thenReturn(appendResult);
			mockMvc.perform(post("/ngsi-ld/v1/entities/{entityId}/attrs", "urn:ngsi-ld:Vehicle:A101")
					.contentType(AppConstants.NGB_APPLICATION_JSON).accept(AppConstants.NGB_APPLICATION_JSONLD)
					.content(appendPayload)).andExpect(status().isMultiStatus());
			verify(entityService, times(1)).appendMessage(any(), any(), any());
		} catch (Exception e) {
			Assert.fail();
			e.printStackTrace();
		}
	}

	/**
	 * this method is validate the bad request if append the attribute in entity
	 */
	@Test
	public void appendEntityBadRequestTest() {
		try {
			when(entityService.appendMessage(any(), any(), any()))
					.thenThrow(new ResponseException(ErrorType.BadRequestData));
			mockMvc.perform(post("/ngsi-ld/v1/entities/{entityId}/attrs", "urn:ngsi-ld:Vehicle:A101")
					.contentType(AppConstants.NGB_APPLICATION_JSON).accept(AppConstants.NGB_APPLICATION_JSONLD)
					.content(appendPayload)).andExpect(status().isBadRequest())
					.andExpect(jsonPath("$.title").value("Bad Request Data."));
			verify(entityService, times(1)).appendMessage(any(), any(), any());
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}

	/**
	 * this method is validate the data not found  if append the attribute in entity
	 */
	@Test
	public void appendEntityNotFoundTest() {
		try {
			when(entityService.appendMessage(any(), any(), any())).thenThrow(new ResponseException(ErrorType.NotFound));
			mockMvc.perform(post("/ngsi-ld/v1/entities/{entityId}/attrs", "urn:ngsi-ld:Vehicle:A101")
					.contentType(AppConstants.NGB_APPLICATION_JSON).accept(AppConstants.NGB_APPLICATION_JSONLD)
					.content(appendPayload)).andExpect(status().isNotFound())
					.andExpect(jsonPath("$.title").value("Resource not found."));
			verify(entityService, times(1)).appendMessage(any(), any(), any());
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}

	/**
	 * this method is validate throw the exception if append the attribute in entity
	 */
	@Test
	public void appendEntity500Test() {
		try {
			when(entityService.appendMessage(any(), any(), any())).thenThrow(new Exception());
			mockMvc.perform(post("/ngsi-ld/v1/entities/{entityId}/attrs", "urn:ngsi-ld:Vehicle:A101")
					.contentType(AppConstants.NGB_APPLICATION_JSON).accept(AppConstants.NGB_APPLICATION_JSONLD)
					.content(appendPayload)).andExpect(status().isInternalServerError())
					.andExpect(jsonPath("$.title").value("Internal error"));
			verify(entityService, times(1)).appendMessage(any(), any(), any());
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}

	/**
	 * this method is use for update the entity
	 */
	@Test
	public void updateEntityNoContentTest() {
		try {
			UpdateResult updateResult = Mockito.mock(UpdateResult.class);
			when(entityService.updateMessage(any(), any())).thenReturn(updateResult);
			when(updateResult.getUpdateResult()).thenReturn(true);
			mockMvc.perform(patch("/ngsi-ld/v1/entities/{entityId}/attrs", "urn:ngsi-ld:Vehicle:A101")
					.contentType(AppConstants.NGB_APPLICATION_JSON).accept(AppConstants.NGB_APPLICATION_JSONLD)
					.content(updatePayload)).andExpect(status().isNoContent());
			verify(entityService, times(1)).updateMessage(any(), any());
		} catch (Exception e) {
			Assert.fail();
			e.printStackTrace();
		}
	}

	/**
	 * this method is use for update the entity for multi status
	 */
	@Test
	public void updateEntityMultiStatusTest() {
		try {
			UpdateResult updateResult = Mockito.mock(UpdateResult.class);
			when(entityService.updateMessage(any(), any())).thenReturn(updateResult);
			mockMvc.perform(patch("/ngsi-ld/v1/entities/{entityId}/attrs", "urn:ngsi-ld:Vehicle:A101")
					.contentType(AppConstants.NGB_APPLICATION_JSON).accept(AppConstants.NGB_APPLICATION_JSONLD)
					.content(updatePayload)).andExpect(status().isMultiStatus());
			verify(entityService, times(1)).updateMessage(any(), any());
		} catch (Exception e) {
			Assert.fail();
			e.printStackTrace();
		}
	}

	/**
	 * this method is validate the bad request if entity update
	 */
	@Test
	public void updateEntityBadRequestTest() {
		try {
			when(entityService.updateMessage(any(), any())).thenThrow(new ResponseException(ErrorType.BadRequestData));
			mockMvc.perform(patch("/ngsi-ld/v1/entities/{entityId}/attrs", "urn:ngsi-ld:Vehicle:A101")
					.contentType(AppConstants.NGB_APPLICATION_JSON).accept(AppConstants.NGB_APPLICATION_JSONLD)
					.content(updatePayload)).andExpect(status().isBadRequest())
					.andExpect(jsonPath("$.title").value("Bad Request Data."));
			verify(entityService, times(1)).updateMessage(any(), any());
		} catch (Exception e) {
			Assert.fail();
		}
	}

	/**
	 * this method is validate the data not found if entity update
	 */
	@Test
	public void updateEntityNotFoundTest() {
		try {
			when(entityService.updateMessage(any(), any())).thenThrow(new ResponseException(ErrorType.NotFound));
			mockMvc.perform(patch("/ngsi-ld/v1/entities/{entityId}/attrs", "urn:ngsi-ld:Vehicle:A101")
					.contentType(AppConstants.NGB_APPLICATION_JSON).accept(AppConstants.NGB_APPLICATION_JSONLD)
					.content(updatePayload)).andExpect(status().isNotFound())
					.andExpect(jsonPath("$.title").value("Resource not found."));
			verify(entityService, times(1)).updateMessage(any(), any());
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}
	
	/**
	 * this method is validate throw the exception if entity update
	 */
	@Test
	public void updateEntity500Test() {
		try {
			when(entityService.updateMessage(any(), any())).thenThrow(new Exception());
			mockMvc.perform(patch("/ngsi-ld/v1/entities/{entityId}/attrs", "urn:ngsi-ld:Vehicle:A101")
					.contentType(AppConstants.NGB_APPLICATION_JSON).accept(AppConstants.NGB_APPLICATION_JSONLD)
					.content(updatePayload)).andExpect(status().isInternalServerError())
					.andExpect(jsonPath("$.title").value("Internal error"));
			verify(entityService, times(1)).updateMessage(any(), any());
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}

	/**
	 * this method is use for partial update the attribute
	 */
	@Test
	public void partialUpdateAttributeIfDatasetIdExistNoContentTest() {
		try {
			UpdateResult updateResult = Mockito.mock(UpdateResult.class);
			when(entityService.partialUpdateEntity(any(), any(), any())).thenReturn(updateResult);
			when(updateResult.getStatus()).thenReturn(true);
			mockMvc.perform(patch("/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}", "urn:ngsi-ld:Vehicle:A101", "speed")
					.contentType(AppConstants.NGB_APPLICATION_JSON).accept(AppConstants.NGB_APPLICATION_JSONLD)
					.content(partialUpdatePayload)).andExpect(status().isNoContent());
			verify(entityService, times(1)).partialUpdateEntity(any(), any(), any());
		} catch (Exception e) {
			Assert.fail();
			e.printStackTrace();
		}
	}

	/**
	 * this method is use for partial update the attribute if datasetId is not exist 
	 */
	@Test
	public void partialUpdateAttributeIfDatasetIdIsNotExistTest() {
		try {
			when(entityService.partialUpdateEntity(any(), any(), any()))
					.thenThrow(new ResponseException(ErrorType.NotFound));
			mockMvc.perform(patch("/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}", "urn:ngsi-ld:Vehicle:A101", "speed")
					.contentType(AppConstants.NGB_APPLICATION_JSON).accept(AppConstants.NGB_APPLICATION_JSONLD)
					.content(partialUpdatePayload)).andExpect(status().isNotFound())
					.andExpect(jsonPath("$.title").value("Resource not found."));
			verify(entityService, times(1)).partialUpdateEntity(any(), any(), any());
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}

	/**
	 * this method is validate the bad request in partial update attribute 
	 */
	@Test
	public void partialUpdateAttributeBadRequestTest() {
		try {
			when(entityService.partialUpdateEntity(any(), any(), any()))
					.thenThrow(new ResponseException(ErrorType.BadRequestData));
			mockMvc.perform(patch("/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}", "urn:ngsi-ld:Vehicle:A101", "speed")
					.contentType(AppConstants.NGB_APPLICATION_JSON).accept(AppConstants.NGB_APPLICATION_JSONLD)
					.content(updatePayload)).andExpect(status().isBadRequest())
					.andExpect(jsonPath("$.title").value("Bad Request Data."));
			verify(entityService, times(1)).partialUpdateEntity(any(), any(), any());
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}

	/**
	 * this method is use for partial update attribute default datasetId
	 */
	@Test
	public void partialUpdateAttributeDefaultDatasetIdCaseTest() {
		try {
			UpdateResult updateResult = Mockito.mock(UpdateResult.class);
			when(entityService.partialUpdateEntity(any(), any(), any())).thenReturn(updateResult);
			when(updateResult.getStatus()).thenReturn(true);
			mockMvc.perform(patch("/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}", "urn:ngsi-ld:Vehicle:A101", "speed")
					.contentType(AppConstants.NGB_APPLICATION_JSON).accept(AppConstants.NGB_APPLICATION_JSONLD)
					.content(partialUpdateDefaultCasePayload)).andExpect(status().isNoContent());
			verify(entityService, times(1)).partialUpdateEntity(any(), any(), any());
		} catch (Exception e) {
			Assert.fail();
			e.printStackTrace();
		}
	}

	/**
	 * this method is validate for the not found in partial update attribute default datasetId 
	 */
	@Test
	public void partialUpdateAttributeDefaultDatasetIdCaseNotFoundTest() {
		try {
			when(entityService.partialUpdateEntity(any(), any(), any()))
					.thenThrow(new ResponseException(ErrorType.NotFound));
			mockMvc.perform(patch("/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}", "urn:ngsi-ld:Vehicle:A101", "speed")
					.contentType(AppConstants.NGB_APPLICATION_JSON).accept(AppConstants.NGB_APPLICATION_JSONLD)
					.content(partialUpdateDefaultCasePayload)).andExpect(status().isNotFound())
					.andExpect(jsonPath("$.title").value("Resource not found."));
			verify(entityService, times(1)).partialUpdateEntity(any(), any(), any());
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}

	/**
	 * this method is validate throw the exception in case of partial update attribute
	 */
	@Test
	public void partialUpdateAttribute500Test() {
		try {
			when(entityService.partialUpdateEntity(any(), any(), any())).thenThrow(new Exception());
			mockMvc.perform(patch("/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}", "urn:ngsi-ld:Vehicle:A101", "speed")
					.contentType(AppConstants.NGB_APPLICATION_JSON).accept(AppConstants.NGB_APPLICATION_JSONLD)
					.content(partialUpdateDefaultCasePayload)).andExpect(status().isInternalServerError())
					.andExpect(jsonPath("$.title").value("Internal error"));
			verify(entityService, times(1)).partialUpdateEntity(any(), any(), any());
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}

	/**
	 * this method is use for delete the entity
	 */
	@Test
	public void deleteEntityTest() {
		try {
			when(entityService.deleteEntity(any())).thenReturn(true);
			mockMvc.perform(delete("/ngsi-ld/v1/entities/{entityId}", "urn:ngsi-ld:Vehicle:A101")
					.contentType(AppConstants.NGB_APPLICATION_JSONLD)).andExpect(status().isNoContent());
			verify(entityService, times(1)).deleteEntity(any());
		} catch (Exception e) {
			Assert.fail();
			e.printStackTrace();
		}
	}

	/**
	 * this method is validate the not found if delete the entity
	 */
	@Test
	public void deleteEntityNotFoundTest() {
		try {
			when(entityService.deleteEntity(any())).thenThrow(new ResponseException(ErrorType.NotFound));
			mockMvc.perform(delete("/ngsi-ld/v1/entities/{entityId}", "urn:ngsi-ld:Vehicle:A101")
					.contentType(AppConstants.NGB_APPLICATION_JSONLD)).andExpect(status().isNotFound())
					.andExpect(jsonPath("$.title").value("Resource not found."));
			verify(entityService, times(1)).deleteEntity(any());
		} catch (Exception e) {
			Assert.fail();
		}
	}

	/**
	 * this method is validate the bad request if delete the entity
	 */
	@Test
	public void deleteEntityBadRequestTest() {
		try {
			when(entityService.deleteEntity(any())).thenThrow(new ResponseException(ErrorType.BadRequestData));
			mockMvc.perform(delete("/ngsi-ld/v1/entities/{entityId}", "urn:ngsi-ld:Vehicle:A101")
					.contentType(AppConstants.NGB_APPLICATION_JSONLD)).andExpect(status().isBadRequest())
					.andExpect(jsonPath("$.title").value("Bad Request Data."));
			verify(entityService, times(1)).deleteEntity(any());
		} catch (Exception e) {
			Assert.fail();
		}
	}

	/**
	 * this method is validate throw exception if delete the entity
	 */
	@Test
	public void deleteEntity500Test() {
		try {
			when(entityService.deleteEntity(any())).thenThrow(new Exception());
			mockMvc.perform(delete("/ngsi-ld/v1/entities/{entityId}", "urn:ngsi-ld:Vehicle:A101")
					.contentType(AppConstants.NGB_APPLICATION_JSONLD)).andExpect(status().isInternalServerError())
					.andExpect(jsonPath("$.title").value("Internal error"));
			verify(entityService, times(1)).deleteEntity(any());
		} catch (Exception e) {
			Assert.fail();
		}
	}

	/**
	 * this method is validate for the datasetId is not exist in case of delete attribute instance
	 */
	@Test
	public void deleteAttributeInstanceIfDatasetIdExistTest() {
		try {
			when(entityService.deleteAttribute(any(), any(), any(), any())).thenReturn(true);
			mockMvc.perform(delete(
					"/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}?datasetId=urn:ngsi-ld:Property:speedometerA4567-speed",
					"urn:ngsi-ld:Vehicle:A101", "speed").contentType(AppConstants.NGB_APPLICATION_JSONLD))
					.andExpect(status().isNoContent());
			verify(entityService, times(1)).deleteAttribute(any(), any(), any(), any());
		} catch (Exception e) {
			Assert.fail();
			e.printStackTrace();
		}
	}

	/**
	 * this method is validate for the datasetId is not exist in case of delete attribute instance
	 */
	@Test
	public void deleteAttributeInstanceIfDatasetIdNotExistTest() {
		try {
			when(entityService.deleteAttribute(any(), any(), any(), any()))
					.thenThrow(new ResponseException(ErrorType.NotFound));
			mockMvc.perform(delete(
					"/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}?datasetId=urn:ngsi-ld:Property:speedometerA4567-speed",
					"urn:ngsi-ld:Vehicle:A101", "speed").contentType(AppConstants.NGB_APPLICATION_JSONLD))
					.andExpect(status().isNotFound()).andExpect(jsonPath("$.title").value("Resource not found."));
			verify(entityService, times(1)).deleteAttribute(any(), any(), any(), any());
		} catch (Exception e) {
			Assert.fail();
		}
	}

	/**
     * this method is validate  for bad request in case of delete attribute
     */
	@Test
	public void deleteAttributeInstanceBadRequestTest() {
		try {
			when(entityService.deleteAttribute(any(), any(), any(), any()))
					.thenThrow(new ResponseException(ErrorType.BadRequestData));
			mockMvc.perform(delete(
					"/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}?datasetId=urn:ngsi-ld:Property:speedometerA4567-speed",
					"urn:ngsi-ld:Vehicle:A101", "speed").contentType(AppConstants.NGB_APPLICATION_JSONLD))
					.andExpect(jsonPath("$.title").value("Bad Request Data."));
			verify(entityService, times(1)).deleteAttribute(any(), any(), any(), any());
		} catch (Exception e) {
			Assert.fail();
		}
	}

	/**
     * this method is validate default instance exist in case of delete attribute
     */
	@Test
	public void deleteAttributeDefaultInstanceIfExistTest() {
		try {
			when(entityService.deleteAttribute(any(), any(), any(), any())).thenReturn(true);
			mockMvc.perform(
					delete("/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}", "urn:ngsi-ld:Vehicle:A101", "speed")
							.contentType(AppConstants.NGB_APPLICATION_JSONLD))
					.andExpect(status().isNoContent());
			verify(entityService, times(1)).deleteAttribute(any(), any(), any(), any());
		} catch (Exception e) {
			Assert.fail();
			e.printStackTrace();
		}
	}
	
    /**
     * this method is validate default instance not exist in case of delete attribute
     */
	@Test
	public void deleteAttributeDefaultInstanceNotExistTest() {
		try {
			when(entityService.deleteAttribute(any(), any(), any(), any()))
					.thenThrow(new ResponseException(ErrorType.NotFound));
			mockMvc.perform(
					delete("/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}", "urn:ngsi-ld:Vehicle:A101", "speed")
							.contentType(AppConstants.NGB_APPLICATION_JSONLD))
					.andExpect(status().isNotFound()).andExpect(jsonPath("$.title").value("Resource not found."));
			verify(entityService, times(1)).deleteAttribute(any(), any(), any(), any());
		} catch (Exception e) {
			Assert.fail();
		}
	}

	/**
	 * this method is validate not found in case of delete attribute
	 */
	@Test
	public void deleteAttributeNotFoundTest() {
		try {
			when(entityService.deleteAttribute(any(), any(), any(), any()))
					.thenThrow(new ResponseException(ErrorType.NotFound));
			mockMvc.perform(
					delete("/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}", "urn:ngsi-ld:Vehicle:A101", "speed")
							.contentType(AppConstants.NGB_APPLICATION_JSONLD))
					.andExpect(status().isNotFound()).andExpect(jsonPath("$.title").value("Resource not found."));
			verify(entityService, times(1)).deleteAttribute(any(), any(), any(), any());
		} catch (Exception e) {
			Assert.fail();
		}
	}

	/**
	 * this method is use for all the delete attribute 
	 */
	@Test
	public void deleteAllAttributeInstanceTest() {
		try {
			when(entityService.deleteAttribute(any(), any(), any(), any())).thenReturn(true);
			mockMvc.perform(delete("/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}?deleteAll=true",
					"urn:ngsi-ld:Vehicle:A101", "speed").contentType(AppConstants.NGB_APPLICATION_JSONLD))
					.andExpect(status().isNoContent());
			verify(entityService, times(1)).deleteAttribute(any(), any(), any(), any());
		} catch (Exception e) {
			Assert.fail();
			e.printStackTrace();
		}
	}

	/**
	 * this method is validate the bad request in case of delete all attribute
	 */
	@Test
	public void deleteAllAttributeInstanceBadRequestTest() {
		try {
			when(entityService.deleteAttribute(any(), any(), any(), any()))
					.thenThrow(new ResponseException(ErrorType.BadRequestData));
			mockMvc.perform(delete("/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}?deleteAll=true",
					"urn:ngsi-ld:Vehicle:A101", "speed").contentType(AppConstants.NGB_APPLICATION_JSONLD))
					.andExpect(status().isBadRequest()).andExpect(jsonPath("$.title").value("Bad Request Data."));
			verify(entityService, times(1)).deleteAttribute(any(), any(), any(), any());
		} catch (Exception e) {
			Assert.fail();
		}
	}

	/**
	 * this method is validate for bad request in case of delete attribute
	 */
	@Test
	public void deleteAttributeBadRequestTest() {
		try {
			when(entityService.deleteAttribute(any(), any(), any(), any()))
					.thenThrow(new ResponseException(ErrorType.BadRequestData));
			mockMvc.perform(
					delete("/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}", "urn:ngsi-ld:Vehicle:A101", "speed")
							.contentType(AppConstants.NGB_APPLICATION_JSONLD))
					.andExpect(status().isBadRequest()).andExpect(jsonPath("$.title").value("Bad Request Data."));
			verify(entityService, times(1)).deleteAttribute(any(), any(), any(), any());
		} catch (Exception e) {
			Assert.fail();
		}
	}

	/**
	 * this method is validate throw the exception in case of delete attribute
	 */
	@Test
	public void deleteAttribute500Test() {
		try {
			when(entityService.deleteAttribute(any(), any(), any(), any())).thenThrow(new Exception());
			mockMvc.perform(
					delete("/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}", "urn:ngsi-ld:Vehicle:A101", "speed")
							.contentType(AppConstants.NGB_APPLICATION_JSONLD))
					.andExpect(status().isInternalServerError()).andExpect(jsonPath("$.title").value("Internal error"));
			verify(entityService, times(1)).deleteAttribute(any(), any(), any(), any());
		} catch (Exception e) {
			Assert.fail();
		}
	}
	 
}
