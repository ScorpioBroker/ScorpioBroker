
package eu.neclab.ngsildbroker.entityhandler.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.ResultActions;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.results.CreateResult;
import eu.neclab.ngsildbroker.commons.datatypes.results.UpdateResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.entityhandler.services.EntityInfoDAO;
import eu.neclab.ngsildbroker.entityhandler.services.EntityService;

@SpringBootTest(properties = { "spring.main.allow-bean-definition-overriding=true" })
@RunWith(SpringRunner.class)
@AutoConfigureMockMvc
public class EntityControllerTest {

	@Autowired
	private MockMvc mockMvc;

	@MockBean
	private EntityService entityService;

    @MockBean
	private EntityInfoDAO entityInfoDAO;

	MockHttpServletRequest mockRequest = new MockHttpServletRequest();

	private final static Logger logger = LoggerFactory.getLogger(EntityController.class);

	private String appendPayload;
	private String updatePayload;
	private String entityPayload;
	private String partialUpdatePayload;
	private String partialUpdateDefaultCasePayload;
	private String payload;

	@Mock
	private Logger loggerMock;

	@Before
	public void setup() throws Exception { //@formatter:off
		
		
		mockRequest.addHeader("Accept", "application/json");
		mockRequest.addHeader("Content-Type", "application/json");
		mockRequest.addHeader("Link","<https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/feature/add-json-ld-context-for-ngsi-ld-test-suite/ngsi-ld-test-suite/ngsi-ld-test-suite-context.jsonld>; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json\"");
		
		
  appendPayload= "{\r\n" + "	\"brandName1\": {\r\n" +
				  "		\"type\": \"Property\",\r\n" + "		\"value\": \"BMW\"\r\n" +
				  "	}\r\n" + "}";
				  
  updatePayload="{\r\n" + "	\"brandName1\": {\r\n" +
				  "		\"type\": \"Property\",\r\n" + "		\"value\": \"Audi\"\r\n" +
				  "	}\r\n" + "}"; 
 
  partialUpdatePayload= "{\r\n" + "		\"value\": 20,\r\n"
				  + "		\"datasetId\": \"urn:ngsi-ld:Property:speedometerA4567-speed\"\r\n"
				  + "}";
				  
  partialUpdateDefaultCasePayload= "{\r\n" + "		\"value\": 11\r\n" + "}";
				  
  entityPayload= "{  \r\n" + "   \"id\":\"urn:ngsi-ld:Vehicle:A101\",\r\n" +
				  "   \"type\":\"Vehicle\",\r\n" + "   \"brandName\":\r\n" + "      {  \r\n" +
				  "         \"type\":\"Property\",\r\n" + "         \"value\":\"Mercedes\"\r\n"
				  + "      },\r\n" + "   \"speed\":[{  \r\n" +
				  "         \"type\":\"Property\",\r\n" + "         \"value\":55,\r\n" +
				  "         \"datasetId\":\"urn:ngsi-ld:Property:speedometerA4567-speed\",\r\n"
				  + "   \"source\":\r\n" + "      {  \r\n" +
				  "         \"type\":\"Property\",\r\n" +
				  "         \"value\":\"Speedometer\"\r\n" + "      }\r\n" + "      },\r\n" +
				  "      {  \r\n" + "         \"type\":\"Property\",\r\n" +
				  "         \"value\":60,\r\n" + "   \"source\":\r\n" + "      {  \r\n" +
				  "         \"type\":\"Property\",\r\n" + "         \"value\":\"GPS\"\r\n" +
				  "      }\r\n" + "      },\r\n" + "      {  \r\n" +
				  "         \"type\":\"Property\",\r\n" + "         \"value\":52.5,\r\n" +
				  "   \"source\":\r\n" + "      {  \r\n" +
				  "         \"type\":\"Property\",\r\n" + "         \"value\":\"GPS_NEW\"\r\n"
				  + "      }\r\n" + "      }],\r\n" +
				  "   \"location\":\r\n"
				  + "      {  \r\n" + "      \"type\":\"GeoProperty\",\r\n" +
				  "	\"value\": \"{ \\\"type\\\": \\\"Point\\\", \\\"coordinates\\\": [ -8.5, 41.2]}\""
				  + "      }\r\n" + "}";
  
  payload = "";
   //@formatter:on 
	}

	@After
	public void tearDown() {
		appendPayload = null;
		updatePayload = null;
		entityPayload = null;
		partialUpdatePayload = null;
		partialUpdateDefaultCasePayload = null;
		payload = null;
	}

	/**
	 * this method is use for create the entity
	 */

	@Test
	public void createEntityTest() {

		try {
			when(entityService.createEntry(any(), any())).thenReturn(new CreateResult("urn:ngsi-ld:Vehicle:A101", true));

			ResultActions resultAction = mockMvc.perform(post("/ngsi-ld/v1/entities/").contentType(AppConstants.NGB_APPLICATION_JSON)
							.accept(AppConstants.NGB_APPLICATION_JSONLD).content(entityPayload))
					.andExpect(status().isCreated())
					.andExpect(redirectedUrl("/ngsi-ld/v1/entities/urn:ngsi-ld:Vehicle:A101"));

			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			assertEquals(201, status);
			verify(entityService, times(1)).createEntry(any(), any());
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
			when(entityService.createEntry(any(), any())).thenThrow(
					new ResponseException(ErrorType.AlreadyExists, "urn:ngsi-ld:Vehicle:A101 already exists"));
			ResultActions resultAction = mockMvc.perform(post("/ngsi-ld/v1/entities/").contentType(AppConstants.NGB_APPLICATION_JSON)
							.accept(AppConstants.NGB_APPLICATION_JSONLD).content(entityPayload))
					.andExpect(status().isConflict()).andExpect(jsonPath("$.title").value("Already exists."));

			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			assertEquals(409, status);
			verify(entityService, times(1)).createEntry(any(), any());

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

			when(entityService.createEntry(any(), any()))
					.thenThrow(new ResponseException(ErrorType.BadRequestData, "You have to provide a valid payload"));
			ResultActions resultAction = mockMvc.perform(post("/ngsi-ld/v1/entities/").contentType(AppConstants.NGB_APPLICATION_JSON)
							.accept(AppConstants.NGB_APPLICATION_JSONLD).content(entityPayload))
					.andExpect(status().isBadRequest()).andExpect(jsonPath("$.title").value("Bad Request Data."));

			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			assertEquals(400, status);
			verify(entityService, times(1)).createEntry(any(), any());
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}

	/**
	 * this method is use for append the attribute in entity
	 */

	@Test
	public void appendEntityNoContentTest() {
		try {

			UpdateResult updateResult = Mockito.mock(UpdateResult.class);
			when(entityService.appendToEntry(any(), any(), any(), any())).thenReturn(updateResult);
			ResultActions resultAction = mockMvc.perform(post("/ngsi-ld/v1/entities/{entityId}/attrs", "urn:ngsi-ld:Vehicle:A101")
							.contentType(AppConstants.NGB_APPLICATION_JSON).accept(AppConstants.NGB_APPLICATION_JSONLD)
							.content(appendPayload))
					.andExpect(status().isNoContent());

			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse res = mvcResult.getResponse();
			int status = res.getStatus();
			assertEquals(204, status);
			verify(entityService, times(1)).appendToEntry(any(), any(), any(), any());

		} catch (Exception e) {
			Assert.fail();
			e.printStackTrace();
		}
	}

	/**
	 * this method is use for append the attribute in entity for multi status
	 */

	@Test
	public void appendEntityMultiStatusTest() throws Exception {
		try {
			UpdateResult updateResult = Mockito.mock(UpdateResult.class);
			when(entityService.appendToEntry(any(), any(), any(), any()))
					.thenThrow(new ResponseException(ErrorType.MultiStatus, "Multi status result"));

			ResultActions resultAction = mockMvc.perform(post("/ngsi-ld/v1/entities/{entityId}/attrs", "urn:ngsi-ld:Vehicle:A101")
							.contentType(AppConstants.NGB_APPLICATION_JSON).accept(AppConstants.NGB_APPLICATION_JSONLD)
							.content(appendPayload))
					.andExpect(status().isMultiStatus());

			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse res = mvcResult.getResponse();
			int status = res.getStatus();
			assertEquals(207, status);
			verify(entityService, times(1)).appendToEntry(any(), any(), any(), any());

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
			when(entityService.appendToEntry(any(), any(), any(), any()))
					.thenThrow(new ResponseException(ErrorType.BadRequestData, "You have to provide a valid payload"));
			ResultActions resultAction = mockMvc.perform(post("/ngsi-ld/v1/entities/{entityId}/attrs", "urn:ngsi-ld:Vehicle:A101")
							.contentType(AppConstants.NGB_APPLICATION_JSON).accept(AppConstants.NGB_APPLICATION_JSONLD)
							.content(appendPayload))
					.andExpect(status().isBadRequest()).andExpect(jsonPath("$.title").value("Bad Request Data."));

			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse res = mvcResult.getResponse();
			int status = res.getStatus();
			assertEquals(400, status);
			verify(entityService, times(1)).appendToEntry(any(), any(), any(), any());

		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}
	}

	/**
	 * this method is validate the data not found if append the attribute in entity
	 */

	@Test
	public void appendEntityNotFoundTest() {
		try {
			when(entityService.appendToEntry(any(), any(), any(), any()))
					.thenThrow(new ResponseException(ErrorType.NotFound, "Resource not found."));
			ResultActions resultAction = mockMvc.perform(post("/ngsi-ld/v1/entities/{entityId}/attrs", "urn:ngsi-ld:Vehicle:A101")
							.contentType(AppConstants.NGB_APPLICATION_JSON).accept(AppConstants.NGB_APPLICATION_JSONLD)
							.content(appendPayload))
					.andExpect(status().isNotFound()).andExpect(jsonPath("$.title").value("Resource not found."));

			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse res = mvcResult.getResponse();
			int status = res.getStatus();
			assertEquals(404, status);
			verify(entityService, times(1)).appendToEntry(any(), any(), any(), any());

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
			when(entityService.updateEntry(any(), any(), any())).thenReturn(updateResult);

			ResultActions resultAction = mockMvc.perform(patch("/ngsi-ld/v1/entities/{entityId}/attrs", "urn:ngsi-ld:Vehicle:A101")
							.contentType(AppConstants.NGB_APPLICATION_JSON).accept(AppConstants.NGB_APPLICATION_JSONLD)
							.content(updatePayload))
					.andExpect(status().isNoContent());

			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse res = mvcResult.getResponse();
			int status = res.getStatus();
			assertEquals(204, status);

			verify(entityService, times(1)).updateEntry(any(), any(), any());
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

			when(entityService.updateEntry(any(), any(), any()))
					.thenThrow(new ResponseException(ErrorType.MultiStatus, "Multi status result"));
			;

			ResultActions resultAction = mockMvc.perform(patch("/ngsi-ld/v1/entities/{entityId}/attrs", "urn:ngsi-ld:Vehicle:A101")
							.contentType(AppConstants.NGB_APPLICATION_JSON).accept(AppConstants.NGB_APPLICATION_JSONLD)
							.content(updatePayload))
					.andExpect(status().isMultiStatus());

			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse res = mvcResult.getResponse();
			int status = res.getStatus();
			assertEquals(207, status);

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
			when(entityService.updateEntry(any(), any(), any()))
					.thenThrow(new ResponseException(ErrorType.BadRequestData, "Bad Request Data."));
			ResultActions resultAction = mockMvc.perform(patch("/ngsi-ld/v1/entities/{entityId}/attrs", "urn:ngsi-ld:Vehicle:A101")
							.contentType(AppConstants.NGB_APPLICATION_JSON).accept(AppConstants.NGB_APPLICATION_JSONLD)
							.content(updatePayload))
					.andExpect(status().isBadRequest()).andExpect(jsonPath("$.title").value("Bad Request Data."));
			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse res = mvcResult.getResponse();
			int status = res.getStatus();
			assertEquals(400, status);
			verify(entityService, times(1)).updateEntry(any(), any(), any());
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
			when(entityService.updateEntry(any(), any(), any()))
					.thenThrow(new ResponseException(ErrorType.NotFound, "Resource not found."));
			ResultActions resultAction = mockMvc.perform(patch("/ngsi-ld/v1/entities/{entityId}/attrs", "urn:ngsi-ld:Vehicle:A101")
							.contentType(AppConstants.NGB_APPLICATION_JSON).accept(AppConstants.NGB_APPLICATION_JSONLD)
							.content(updatePayload))
					.andExpect(status().isNotFound()).andExpect(jsonPath("$.title").value("Resource not found."));

			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse res = mvcResult.getResponse();
			int status = res.getStatus();
			assertEquals(404, status);
			verify(entityService, times(1)).updateEntry(any(), any(), any());
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
			when(entityService.partialUpdateEntity(any(), any(), any(), any())).thenReturn(updateResult);
			// when(updateResult.getStatus()).thenReturn(true);
			ResultActions resultAction = mockMvc.perform(
					patch("/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}", "urn:ngsi-ld:Vehicle:A101", "speed")
							.with(request -> {
								request.setServletPath("/ngsi-ld/v1/entities/urn:ngsi-ld:Vehicle:A101/attrs/speed");
								return request;
							}).contentType(AppConstants.NGB_APPLICATION_JSON)
							.accept(AppConstants.NGB_APPLICATION_JSONLD).content(partialUpdatePayload))
					.andExpect(status().isNoContent());

			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse res = mvcResult.getResponse();
			int status = res.getStatus();
			assertEquals(204, status);
			verify(entityService, times(1)).partialUpdateEntity(any(), any(), any(), any());
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
			when(entityService.partialUpdateEntity(any(), any(), any(), any()))
					.thenThrow(new ResponseException(ErrorType.NotFound, "Resource not found."));
			ResultActions resultAction = mockMvc.perform(
					patch("/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}", "urn:ngsi-ld:Vehicle:A101", "speed")
							.with(request -> {
								request.setServletPath("/ngsi-ld/v1/entities/urn:ngsi-ld:Vehicle:A101/attrs/speed");
								return request;
							}).contentType(AppConstants.NGB_APPLICATION_JSON)
							.accept(AppConstants.NGB_APPLICATION_JSONLD).content(partialUpdatePayload))
					.andExpect(status().isNotFound()).andExpect(jsonPath("$.title").value("Resource not found."));
			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse res = mvcResult.getResponse();
			int status = res.getStatus();
			assertEquals(404, status);

			verify(entityService, times(1)).partialUpdateEntity(any(), any(), any(), any());
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
			when(entityService.partialUpdateEntity(any(), any(), any(), any()))
					.thenThrow(new ResponseException(ErrorType.BadRequestData, "Bad Request Data."));
			ResultActions resultAction = mockMvc.perform(
					patch("/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}", "urn:ngsi-ld:Vehicle:A101", "speed")
							.with(request -> {
								request.setServletPath("/ngsi-ld/v1/entities/urn:ngsi-ld:Vehicle:A101/attrs/speed");
								return request;
							}).contentType(AppConstants.NGB_APPLICATION_JSON)
							.accept(AppConstants.NGB_APPLICATION_JSONLD).with(request -> {
								request.setServletPath("/ngsi-ld/v1/entities/urn:ngsi-ld:Vehicle:A101/attrs/speed");
								return request;
							}).content(partialUpdatePayload))
					.andExpect(status().isBadRequest()).andExpect(jsonPath("$.title").value("Bad Request Data."));

			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse res = mvcResult.getResponse();
			int status = res.getStatus();
			assertEquals(400, status);
			verify(entityService, times(1)).partialUpdateEntity(any(), any(), any(), any());
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
			when(entityService.partialUpdateEntity(any(), any(), any(), any())).thenReturn(updateResult);
			// when(updateResult.getStatus()).thenReturn(true);
			ResultActions resultAction = mockMvc.perform(
					patch("/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}", "urn:ngsi-ld:Vehicle:A101", "speed")
							.with(request -> {
								request.setServletPath("/ngsi-ld/v1/entities/urn:ngsi-ld:Vehicle:A101/attrs/speed");
								return request;
							}).contentType(AppConstants.NGB_APPLICATION_JSON)
							.accept(AppConstants.NGB_APPLICATION_JSONLD).content(partialUpdateDefaultCasePayload))
					.andExpect(status().isNoContent());

			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse res = mvcResult.getResponse();
			int status = res.getStatus();
			assertEquals(204, status);
			verify(entityService, times(1)).partialUpdateEntity(any(), any(), any(), any());
		} catch (Exception e) {
			Assert.fail();
			e.printStackTrace();
		}
	}

	/**
	 * this method is validate for the not found in partial update attribute default
	 * datasetId
	 */

	@Test
	public void partialUpdateAttributeDefaultDatasetIdCaseNotFoundTest() {
		try {
			when(entityService.partialUpdateEntity(any(), any(), any(), any()))
					.thenThrow(new ResponseException(ErrorType.NotFound, "Resource not found."));
			ResultActions resultAction = mockMvc.perform(
					patch("/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}", "urn:ngsi-ld:Vehicle:A101", "speed")
							.with(request -> {
								request.setServletPath("/ngsi-ld/v1/entities/urn:ngsi-ld:Vehicle:A101/attrs/speed");
								return request;
							}).contentType(AppConstants.NGB_APPLICATION_JSON)
							.accept(AppConstants.NGB_APPLICATION_JSONLD).content(partialUpdateDefaultCasePayload))
					.andExpect(status().isNotFound()).andExpect(jsonPath("$.title").value("Resource not found."));
			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse res = mvcResult.getResponse();
			int status = res.getStatus();
			assertEquals(404, status);
			verify(entityService, times(1)).partialUpdateEntity(any(), any(), any(), any());
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
			when(entityService.deleteEntry(any(), any())).thenReturn(true);
			ResultActions resultAction = mockMvc.perform(delete("/ngsi-ld/v1/entities/{entityId}", "urn:ngsi-ld:Vehicle:A101")
							.contentType(AppConstants.NGB_APPLICATION_JSONLD))
					.andExpect(status().isNoContent());
			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse res = mvcResult.getResponse();
			int status = res.getStatus();
			assertEquals(204, status);
			verify(entityService, times(1)).deleteEntry(any(), any());
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
			when(entityService.deleteEntry(any(), any()))
					.thenThrow(new ResponseException(ErrorType.NotFound, "Resource not found."));
			ResultActions resultAction = mockMvc.perform(delete("/ngsi-ld/v1/entities/{entityId}", "urn:ngsi-ld:Vehicle:A101")
							.contentType(AppConstants.NGB_APPLICATION_JSONLD))
					.andExpect(status().isNotFound()).andExpect(jsonPath("$.title").value("Resource not found."));
			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse res = mvcResult.getResponse();
			int status = res.getStatus();
			assertEquals(404, status);
			verify(entityService, times(1)).deleteEntry(any(), any());

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
			when(entityService.deleteEntry(any(), any()))
					.thenThrow(new ResponseException(ErrorType.BadRequestData, "Bad Request Data."));
			ResultActions resultAction = mockMvc.perform(delete("/ngsi-ld/v1/entities/{entityId}", "urn:ngsi-ld:Vehicle:A101")
							.contentType(AppConstants.NGB_APPLICATION_JSONLD))
					.andExpect(status().isBadRequest()).andExpect(jsonPath("$.title").value("Bad Request Data."));
			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse res = mvcResult.getResponse();
			int status = res.getStatus();
			assertEquals(400, status);

			verify(entityService, times(1)).deleteEntry(any(), any());
		} catch (Exception e) {
			Assert.fail();
		}
	}

	/**
	 * this method is validate for the datasetId is exist in case of delete
	 * attribute instance
	 */

	@Test
	public void deleteAttributeInstanceIfDatasetIdExistTest() {
		try {
			when(entityService.deleteAttribute(any(), any(), any(), any(), any())).thenReturn(true);
			ResultActions resultAction = mockMvc.perform(delete(
					"/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}?datasetId=urn:ngsi-ld:Property:speedometerA4567-speed",
					"urn:ngsi-ld:Vehicle:A101", "speed").with(request -> {
						request.setServletPath(
								"/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}?datasetId=urn:ngsi-ld:Property:speedometerA4567-speed\",\r\n"
										+ "					\"urn:ngsi-ld:Vehicle:A101\", \"speed\"");
						return request;
					}).contentType(AppConstants.NGB_APPLICATION_JSONLD)).andExpect(status().isNoContent());
			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse res = mvcResult.getResponse();
			int status = res.getStatus();
			assertEquals(204, status);
			verify(entityService, times(1)).deleteAttribute(any(), any(), any(), any(), any());
		} catch (Exception e) {
			Assert.fail();
			e.printStackTrace();
		}
	}

	/**
	 * this method is validate for the datasetId is not exist in case of delete
	 * attribute instance
	 */

	@Test
	public void deleteAttributeInstanceIfDatasetIdNotExistTest() {
		try {
			when(entityService.deleteAttribute(any(), any(), any(), any(), any()))
					.thenThrow(new ResponseException(ErrorType.NotFound, "Resource not found."));
			ResultActions resultAction = mockMvc.perform(delete(
					"/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}?datasetId=urn:ngsi-ld:Property:speedometerA4567-speed",
					"urn:ngsi-ld:Vehicle:A101", "speed").with(request -> {
						request.setServletPath(
								"/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}?datasetId=urn:ngsi-ld:Property:speedometerA4567-speed\",\r\n"
										+ "					\"urn:ngsi-ld:Vehicle:A101\", \"speed\"");
						return request;
					}).contentType(AppConstants.NGB_APPLICATION_JSONLD)).andExpect(status().isNotFound())
					.andExpect(jsonPath("$.title").value("Resource not found."));
			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse res = mvcResult.getResponse();
			int status = res.getStatus();
			assertEquals(404, status);

			verify(entityService, times(1)).deleteAttribute(any(), any(), any(), any(), any());
		} catch (Exception e) {
			Assert.fail();
		}
	}

	/**
	 * this method is validate for bad request in case of delete attribute
	 */

	@Test
	public void deleteAttributeInstanceBadRequestTest() {
		try {
			when(entityService.deleteAttribute(any(), any(), any(), any(), any()))
					.thenThrow(new ResponseException(ErrorType.BadRequestData, "Bad Request Data."));
			ResultActions resultAction = mockMvc.perform(delete(
					"/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}?datasetId=urn:ngsi-ld:Property:speedometerA4567-speed",
					"urn:ngsi-ld:Vehicle:A101", "speed").with(request -> {
						request.setServletPath(
								"/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}?datasetId=urn:ngsi-ld:Property:speedometerA4567-speed\",\r\n"
										+ "					\"urn:ngsi-ld:Vehicle:A101\", \"speed\"");
						return request;
					}).contentType(AppConstants.NGB_APPLICATION_JSONLD))
					.andExpect(jsonPath("$.title").value("Bad Request Data."));
			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse res = mvcResult.getResponse();
			int status = res.getStatus();
			assertEquals(400, status);
			verify(entityService, times(1)).deleteAttribute(any(), any(), any(), any(), any());
		} catch (Exception e) {
			Assert.fail();
		}
	}

	/**
	 * this method is validate not found in case of delete attribute
	 */

	@Test
	public void deleteAttributeInstanceNotFoundTest() {
		try {
			when(entityService.deleteAttribute(any(), any(), any(), any(), any()))
					.thenThrow(new ResponseException(ErrorType.NotFound, "Resource not found."));
			ResultActions resultAction = mockMvc.perform(
					delete("/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}", "urn:ngsi-ld:Vehicle:A101", "speed")
							.with(request -> {
								request.setServletPath(
										"/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}\", \"urn:ngsi-ld:Vehicle:A101\", \"speed");
								return request;
							}).contentType(AppConstants.NGB_APPLICATION_JSONLD))
					.andExpect(status().isNotFound()).andExpect(jsonPath("$.title").value("Resource not found."));
			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse res = mvcResult.getResponse();
			int status = res.getStatus();
			assertEquals(404, status);
			verify(entityService, times(1)).deleteAttribute(any(), any(), any(), any(), any());
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
			when(entityService.deleteAttribute(any(), any(), any(), any(), any())).thenReturn(true);
			ResultActions resultAction = mockMvc.perform(delete("/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}?deleteAll=true",
							"urn:ngsi-ld:Vehicle:A101", "speed").with(request -> {
								request.setServletPath(
										"\"/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}?deleteAll=true\",\r\n"
												+ "				 	\"urn:ngsi-ld:Vehicle:A101\", \"speed\"");
								return request;
							}).contentType(AppConstants.NGB_APPLICATION_JSONLD))
					.andExpect(status().isNoContent());
			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse res = mvcResult.getResponse();
			int status = res.getStatus();
			assertEquals(204, status);

			verify(entityService, times(1)).deleteAttribute(any(), any(), any(), any(), any());
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
			when(entityService.deleteAttribute(any(), any(), any(), any(), any()))
					.thenThrow(new ResponseException(ErrorType.BadRequestData, "Bad Request Data."));
			ResultActions resultAction = mockMvc.perform(delete("/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}?deleteAll=true",
							"urn:ngsi-ld:Vehicle:A101", "speed").with(request -> {
								request.setServletPath(
										"\"/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}?deleteAll=true\",\r\n"
												+ "					\"urn:ngsi-ld:Vehicle:A101\", \"speed\"");
								return request;
							}).contentType(AppConstants.NGB_APPLICATION_JSONLD))
					.andExpect(status().isBadRequest()).andExpect(jsonPath("$.title").value("Bad Request Data."));
			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse res = mvcResult.getResponse();
			int status = res.getStatus();
			assertEquals(400, status);
			verify(entityService, times(1)).deleteAttribute(any(), any(), any(), any(), any());
		} catch (Exception e) {
			Assert.fail();
		}
	}

}
