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
    
	private String payload;
	private String appendPayload;
	private String updatePayload;

	@Before
	public void setup() throws Exception {
		//@formatter:off
		payload = "{  \r\n" + 
				"   \"id\":\"urn:ngsi-ld:Vehicle:B9211\",\r\n" + 
				"   \"type\":\"Vehicle\",\r\n" + 
				"   \"brandName\":\"Volvo\",\r\n" + 
				"   \"speed\":[  \r\n" + 
				"      {  \r\n" + 
				"         \"type\":\"Property\",\r\n" + 
				"         \"instanceId\":\"be664aaf-a7af-4a99-bebc-e89528238abf\",\r\n" + 
				"         \"observedAt\":\"2018-06-01T12:03:00Z\",\r\n" + 
				"         \"value\":\"120\"\r\n" + 
				"      },\r\n" + 
				"      {  \r\n" + 
				"         \"type\":\"Property\",\r\n" + 
				"         \"instanceId\":\"d3ac28df-977f-4151-a432-dc088f7400d7\",\r\n" + 
				"         \"observedAt\":\"2018-08-01T12:05:00Z\",\r\n" + 
				"         \"value\":\"80\"\r\n" + 
				"      }\r\n" + 
				"   ],\r\n" + 
				"   \"@context\":{  \r\n" + 
				"      \"id\":\"@id\",\r\n" + 
				"      \"type\":\"@type\",\r\n" + 
				"      \"Property\":\"https://uri.etsi.org/ngsi-ld/Property\",\r\n" + 
				"      \"Relationship\":\"https://uri.etsi.org/ngsi-ld/Relationship\",\r\n" + 
				"      \"value\":\"https://uri.etsi.org/ngsi-ld/hasValue\",\r\n" + 
//				"      \"observedAt\":{  \r\n" + 
//				"         \"@type\":\"https://uri.etsi.org/ngsi-ld/DateTime\",\r\n" + 
//				"         \"@id\":\"https://uri.etsi.org/ngsi-ld/observedAt\"\r\n" + 
//				"      },\r\n" + 
				"      \"Vehicle\":\"http://example.org/vehicle/Vehicle\",\r\n" + 
				"      \"brandName\":\"http://example.org/vehicle/brandName\",\r\n" + 
//				"      \"instanceId\":\"https://uri.etsi.org/ngsi-ld/instanceId\",\r\n" + 
				"      \"speed\":\"http://example.org/vehicle/speed\"\r\n" + 
				"   }\r\n" + 
				"}";
		
		appendPayload="{\r\n" + 
				"	\"@context\": {\r\n" + 
				"		\"type\": \"@type\",\r\n" + 
				"		\"Property\": \"https://uri.etsi.org/ngsi-ld/Property\",\r\n" + 
				"		\"value\": \"https://uri.etsi.org/ngsi-ld/hasValue\",\r\n" + 
				"		\"brandName1\": \"http://example.org/vehicle/brandName1\"\r\n" + 
				"	},\r\n" + 
				"	\"brandName1\": {\r\n" + 
				"		\"type\": \"Property\",\r\n" + 
				"		\"value\": \"BMW\"\r\n" + 
				"	}\r\n" + 
				"}";
		
		updatePayload="{\r\n" + 
				"	\"@context\": {\r\n" + 
				"		\"type\": \"@type\",\r\n" + 
				"		\"Property\": \"https://uri.etsi.org/ngsi-ld/Property\",\r\n" + 
				"		\"value\": \"https://uri.etsi.org/ngsi-ld/hasValue\",\r\n" + 
				"		\"brandName1\": \"http://example.org/vehicle/brandName1\"\r\n" + 
				"	},\r\n" + 
				"	\"brandName1\": {\r\n" + 
				"		\"type\": \"Property\",\r\n" + 
				"		\"value\": \"Audi\"\r\n" + 
				"	}\r\n" + 
				"}";
		//@formatter:on
	}
	
	
	@After
	public void tearDown() {
		payload=null;
		appendPayload=null;
		updatePayload=null;
	}

	@Test
	public void createEntityTest() {
		try {
			when(entityService.createMessage(any())).thenReturn("urn:ngsi-ld:Vehicle:B9211");
			mockMvc.perform(post("/ngsi-ld/v1/entities/")
					.contentType(AppConstants.NGB_APPLICATION_JSONLD)
					.content(payload))
					.andExpect(status().isCreated())
					.andExpect(redirectedUrl("/ngsi-ld/v1/entities/urn:ngsi-ld:Vehicle:B9211"));
			verify(entityService,times(1)).createMessage(any());
			verify(entityService,times(1)).validateEntity(any(), any());
		}catch(Exception e) {
			Assert.fail();
			e.printStackTrace();
		}
	}
	
	@Test
	public void createEntityAlreadyExistTest(){
		try {
			when(entityService.createMessage(any())).thenThrow(new ResponseException(ErrorType.AlreadyExists));
			mockMvc.perform(post("/ngsi-ld/v1/entities/")
					.contentType(AppConstants.NGB_APPLICATION_JSONLD)
					.content(payload))
					.andExpect(status().isConflict())
					.andExpect(jsonPath("$.title").value("Already exists."));
			verify(entityService,times(1)).createMessage(any());
			verify(entityService,times(1)).validateEntity(any(), any());
		}catch(Exception e) {
			Assert.fail();
		}
	}
	
	@Test
	public void createEntityBadRequestTest(){
		try {
			when(entityService.createMessage(any())).thenThrow(new ResponseException(ErrorType.BadRequestData));
			mockMvc.perform(post("/ngsi-ld/v1/entities/")
					.contentType(AppConstants.NGB_APPLICATION_JSONLD)
					.content(payload))
					.andExpect(status().isBadRequest())
					.andExpect(jsonPath("$.title").value("Bad Request Data."));
			verify(entityService,times(1)).createMessage(any());
			verify(entityService,times(1)).validateEntity(any(), any());
		}catch(Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}
	
	@Test
	public void createEntity500ExceptionTest(){
		try {
			when(entityService.createMessage(any())).thenThrow(new Exception());
			mockMvc.perform(post("/ngsi-ld/v1/entities/")
					.contentType(AppConstants.NGB_APPLICATION_JSONLD)
					.content(payload))
					.andExpect(status().isInternalServerError())
					.andExpect(jsonPath("$.title").value("Internal server error"));
			verify(entityService,times(1)).createMessage(any());
			verify(entityService,times(1)).validateEntity(any(), any());
		}catch(Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}
	
	@Test
	public void appendEntityNoContentTest() {
		try {
			AppendResult appendResult=Mockito.mock(AppendResult.class);
			when(entityService.appendMessage(any(), any(), any())).thenReturn(appendResult);
			when(appendResult.getAppendResult()).thenReturn(true);
			mockMvc.perform(post("/ngsi-ld/v1/entities/{entityId}/attrs","urn:ngsi-ld:Vehicle:B9211")
					.contentType(AppConstants.NGB_APPLICATION_JSONLD)
					.content(appendPayload))
					.andExpect(status().isNoContent());
			verify(entityService,times(1)).appendMessage(any(), any(), any());
		}catch(Exception e) {
			Assert.fail();
			e.printStackTrace();
		}
	}
	
	@Test
	public void appendEntityMultiStatusTest()  {
		try {
			AppendResult appendResult=Mockito.mock(AppendResult.class);
			when(entityService.appendMessage(any(), any(), any())).thenReturn(appendResult);
			mockMvc.perform(post("/ngsi-ld/v1/entities/{entityId}/attrs","urn:ngsi-ld:Vehicle:B9211")
					.contentType(AppConstants.NGB_APPLICATION_JSONLD)
					.content(appendPayload))
					.andExpect(status().isMultiStatus());
			verify(entityService,times(1)).appendMessage(any(), any(), any());
		}catch(Exception e) {
			Assert.fail();
			e.printStackTrace();
		}
	}
	
	@Test
	public void appendEntityBadRequestTest()  {
		try {
			when(entityService.appendMessage(any(), any(), any())).thenThrow(new ResponseException(ErrorType.BadRequestData));
			mockMvc.perform(post("/ngsi-ld/v1/entities/{entityId}/attrs","urn:ngsi-ld:Vehicle:B9211")
					.contentType(AppConstants.NGB_APPLICATION_JSONLD)
					.content(appendPayload))
					.andExpect(status().isBadRequest())
					.andExpect(jsonPath("$.title").value("Bad Request Data."));
			verify(entityService,times(1)).appendMessage(any(), any(), any());
		}catch(Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}
	
	@Test
	public void appendEntityNotFoundTest() {
		try {
			when(entityService.appendMessage(any(), any(), any())).thenThrow(new ResponseException(ErrorType.NotFound));
			mockMvc.perform(post("/ngsi-ld/v1/entities/{entityId}/attrs","urn:ngsi-ld:Vehicle:B9211")
					.contentType(AppConstants.NGB_APPLICATION_JSONLD)
					.content(appendPayload))
					.andExpect(status().isNotFound())
					.andExpect(jsonPath("$.title").value("Not Found."));
			verify(entityService,times(1)).appendMessage(any(), any(), any());
		}catch(Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}
	
	@Test
	public void appendEntity500Test() {
		try {
			when(entityService.appendMessage(any(), any(), any())).thenThrow(new Exception());
			mockMvc.perform(post("/ngsi-ld/v1/entities/{entityId}/attrs","urn:ngsi-ld:Vehicle:B9211")
					.contentType(AppConstants.NGB_APPLICATION_JSONLD)
					.content(appendPayload))
					.andExpect(status().isInternalServerError())
					.andExpect(jsonPath("$.title").value("Internal server error"));
			verify(entityService,times(1)).appendMessage(any(), any(), any());
		}catch(Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}
	
	@Test
	public void updateEntityNoContentTest()  {
		try {
			UpdateResult updateResult=Mockito.mock(UpdateResult.class);
			when(entityService.updateMessage(any(), any())).thenReturn(updateResult);
			when(updateResult.getUpdateResult()).thenReturn(true);
			mockMvc.perform(patch("/ngsi-ld/v1/entities/{entityId}/attrs","urn:ngsi-ld:Vehicle:B9211")
					.contentType(AppConstants.NGB_APPLICATION_JSONLD)
					.content(updatePayload))
					.andExpect(status().isNoContent());
			verify(entityService,times(1)).updateMessage(any(), any());
		}catch(Exception e) {
			Assert.fail();
			e.printStackTrace();
		}
	}
	
	@Test
	public void updateEntityMultiStatusTest(){
		try {
			UpdateResult updateResult=Mockito.mock(UpdateResult.class);
			when(entityService.updateMessage(any(), any())).thenReturn(updateResult);
			mockMvc.perform(patch("/ngsi-ld/v1/entities/{entityId}/attrs","urn:ngsi-ld:Vehicle:B9211")
					.contentType(AppConstants.NGB_APPLICATION_JSONLD)
					.content(updatePayload))
					.andExpect(status().isMultiStatus());
			verify(entityService,times(1)).updateMessage(any(), any());
		}catch(Exception e) {
			Assert.fail();
			e.printStackTrace();
		}
	}
	
	@Test
	public void updateEntityBadRequestTest(){
		try {
			when(entityService.updateMessage(any(), any())).thenThrow(new ResponseException(ErrorType.BadRequestData));
			mockMvc.perform(patch("/ngsi-ld/v1/entities/{entityId}/attrs","urn:ngsi-ld:Vehicle:B9211")
					.contentType(AppConstants.NGB_APPLICATION_JSONLD)
					.content(updatePayload))
					.andExpect(status().isBadRequest())
					.andExpect(jsonPath("$.title").value("Bad Request Data."));
			verify(entityService,times(1)).updateMessage(any(), any());
		}catch(Exception e) {
			Assert.fail();
		}
	}
	
	@Test
	public void updateEntityNotFoundTest() {
		try {
			when(entityService.updateMessage(any(), any())).thenThrow(new ResponseException(ErrorType.NotFound));
			mockMvc.perform(patch("/ngsi-ld/v1/entities/{entityId}/attrs","urn:ngsi-ld:Vehicle:B9211")
					.contentType(AppConstants.NGB_APPLICATION_JSONLD)
					.content(updatePayload))
					.andExpect(status().isNotFound())
					.andExpect(jsonPath("$.title").value("Not Found."));
			verify(entityService,times(1)).updateMessage(any(), any());
		}catch(Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}
	
	@Test
	public void updateEntity500Test() {
		try {
			when(entityService.updateMessage(any(), any())).thenThrow(new Exception());
			mockMvc.perform(patch("/ngsi-ld/v1/entities/{entityId}/attrs","urn:ngsi-ld:Vehicle:B9211")
					.contentType(AppConstants.NGB_APPLICATION_JSONLD)
					.content(updatePayload))
					.andExpect(status().isInternalServerError())
					.andExpect(jsonPath("$.title").value("Internal server error"));
			verify(entityService,times(1)).updateMessage(any(), any());
		}catch(Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}
	
	@Test
	public void partialUpdateEntityNoContentTest() {
		try {
			UpdateResult updateResult=Mockito.mock(UpdateResult.class);
			when(entityService.partialUpdateEntity(any(), any(), any())).thenReturn(updateResult);
			when(updateResult.getStatus()).thenReturn(true);
			mockMvc.perform(patch("/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}","urn:ngsi-ld:Vehicle:B9211","brandName")
					.contentType(AppConstants.NGB_APPLICATION_JSONLD)
					.content(updatePayload))
					.andExpect(status().isNoContent());
			verify(entityService,times(1)).partialUpdateEntity(any(), any(), any());
		}catch(Exception e) {
			Assert.fail();
			e.printStackTrace();
		}
	}
	
	
	@Test
	public void partialUpdateEntityNotFoundTest(){
		try {
			when(entityService.partialUpdateEntity(any(), any(), any())).thenThrow(new ResponseException(ErrorType.NotFound));
			mockMvc.perform(patch("/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}","urn:ngsi-ld:Vehicle:B9211","brandName")
					.contentType(AppConstants.NGB_APPLICATION_JSONLD)
					.content(updatePayload))
					.andExpect(status().isNotFound())
					.andExpect(jsonPath("$.title").value("Not Found."));
			verify(entityService,times(1)).partialUpdateEntity(any(), any(), any());
		}catch(Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}
	
	@Test
	public void partialUpdateEntityBadRequestTest(){
		try {
			when(entityService.partialUpdateEntity(any(), any(), any())).thenThrow(new ResponseException(ErrorType.BadRequestData));
			mockMvc.perform(patch("/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}","urn:ngsi-ld:Vehicle:B9211","brandName")
					.contentType(AppConstants.NGB_APPLICATION_JSONLD)
					.content(updatePayload))
					.andExpect(status().isBadRequest())
					.andExpect(jsonPath("$.title").value("Bad Request Data."));
			verify(entityService,times(1)).partialUpdateEntity(any(), any(), any());
		}catch(Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}
	
	@Test
	public void partialUpdateEntity500Test() {
		try {
			when(entityService.partialUpdateEntity(any(), any(), any())).thenThrow(new Exception());
			mockMvc.perform(patch("/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}","urn:ngsi-ld:Vehicle:B9211","brandName")
					.contentType(AppConstants.NGB_APPLICATION_JSONLD)
					.content(updatePayload))
					.andExpect(status().isInternalServerError())
					.andExpect(jsonPath("$.title").value("Internal server error"));
			verify(entityService,times(1)).partialUpdateEntity(any(), any(), any());
		}catch(Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}
	
	
	@Test
	public void deleteEntityTest(){
		try {
			when(entityService.deleteEntity(any())).thenReturn(true);
			mockMvc.perform(delete("/ngsi-ld/v1/entities/{entityId}","urn:ngsi-ld:Vehicle:B9211")
					.contentType(AppConstants.NGB_APPLICATION_JSONLD))
			        .andExpect(status().isNoContent());
			verify(entityService,times(1)).deleteEntity(any());
		}catch(Exception e) {
			Assert.fail();
			e.printStackTrace();
		}
	}
	
	@Test
	public void deleteEntityNotFoundTest(){
		try {
			when(entityService.deleteEntity(any())).thenThrow(new ResponseException(ErrorType.NotFound));
			mockMvc.perform(delete("/ngsi-ld/v1/entities/{entityId}","urn:ngsi-ld:Vehicle:B9211")
					.contentType(AppConstants.NGB_APPLICATION_JSONLD))
			        .andExpect(status().isNotFound())
			        .andExpect(jsonPath("$.title").value("Not Found."));
			verify(entityService,times(1)).deleteEntity(any());
		}catch(Exception e) {
			Assert.fail();
		}
	}
	
	@Test
	public void deleteEntityBadRequestTest(){
		try {
			when(entityService.deleteEntity(any())).thenThrow(new ResponseException(ErrorType.BadRequestData));
			mockMvc.perform(delete("/ngsi-ld/v1/entities/{entityId}","urn:ngsi-ld:Vehicle:B9211")
					.contentType(AppConstants.NGB_APPLICATION_JSONLD))
			        .andExpect(status().isBadRequest())
			        .andExpect(jsonPath("$.title").value("Bad Request Data."));
			verify(entityService,times(1)).deleteEntity(any());
		}catch(Exception e) {
			Assert.fail();
		}
	}
	
	@Test
	public void deleteEntity500Test(){
		try {
			when(entityService.deleteEntity(any())).thenThrow(new Exception());
			mockMvc.perform(delete("/ngsi-ld/v1/entities/{entityId}","urn:ngsi-ld:Vehicle:B9211")
					.contentType(AppConstants.NGB_APPLICATION_JSONLD))
			        .andExpect(status().isInternalServerError())
			        .andExpect(jsonPath("$.title").value("Internal server error"));
			verify(entityService,times(1)).deleteEntity(any());
		}catch(Exception e) {
			Assert.fail();
		}
	}
	
	@Test
	public void deleteAttributeTest(){
		try {
			when(entityService.deleteAttribute(any(), any())).thenReturn(true);
			mockMvc.perform(delete("/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}","urn:ngsi-ld:Vehicle:B9211","BrandName")
					.contentType(AppConstants.NGB_APPLICATION_JSONLD))
			        .andExpect(status().isNoContent());
			verify(entityService,times(1)).deleteAttribute(any(),any());
		}catch(Exception e) {
			Assert.fail();
			e.printStackTrace();
		}
	}
	
	@Test
	public void deleteAttributeNotFoundTest(){
		try {
			when(entityService.deleteAttribute(any(),any())).thenThrow(new ResponseException(ErrorType.NotFound));
			mockMvc.perform(delete("/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}","urn:ngsi-ld:Vehicle:B9211","BrandName")
					.contentType(AppConstants.NGB_APPLICATION_JSONLD))
			        .andExpect(status().isNotFound())
			        .andExpect(jsonPath("$.title").value("Not Found."));
			verify(entityService,times(1)).deleteAttribute(any(),any());
		}catch(Exception e) {
			Assert.fail();
		}
	}
	
	@Test
	public void deleteAttributeBadRequestTest(){
		try {
			when(entityService.deleteAttribute(any(),any())).thenThrow(new ResponseException(ErrorType.BadRequestData));
			mockMvc.perform(delete("/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}","urn:ngsi-ld:Vehicle:B9211","BrandName")
					.contentType(AppConstants.NGB_APPLICATION_JSONLD))
			        .andExpect(status().isBadRequest())
			        .andExpect(jsonPath("$.title").value("Bad Request Data."));
			verify(entityService,times(1)).deleteAttribute(any(),any());
		}catch(Exception e) {
			Assert.fail();
		}
	}
	
	@Test
	public void deleteAttribute500Test(){
		try {
			when(entityService.deleteAttribute(any(),any())).thenThrow(new Exception());
			mockMvc.perform(delete("/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}","urn:ngsi-ld:Vehicle:B9211","BrandName")
					.contentType(AppConstants.NGB_APPLICATION_JSONLD))
			        .andExpect(status().isInternalServerError())
			        .andExpect(jsonPath("$.title").value("Internal server error"));
			verify(entityService,times(1)).deleteAttribute(any(),any());
		}catch(Exception e) {
			Assert.fail();
		}
	}
}
