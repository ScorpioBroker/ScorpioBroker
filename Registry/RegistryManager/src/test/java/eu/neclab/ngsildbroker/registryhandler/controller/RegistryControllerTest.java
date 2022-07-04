package eu.neclab.ngsildbroker.registryhandler.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.redirectedUrl;
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
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.ResultActions;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.results.CreateResult;
import eu.neclab.ngsildbroker.commons.datatypes.results.QueryResult;
import eu.neclab.ngsildbroker.commons.datatypes.results.UpdateResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.registryhandler.service.CSourceService;

@SpringBootTest(properties = { "spring.main.allow-bean-definition-overriding=true" })
@RunWith(SpringRunner.class)
@AutoConfigureMockMvc
public class RegistryControllerTest {

	@Autowired
	private MockMvc mockMvc;
	@MockBean
	private CSourceService csourceService;
	private String payload;
	private String updatePayload;
	private String updatePayload1;
	private String CORE_CONTEXT_URL_STR = "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld";

	MockHttpServletRequest mockRequest = new MockHttpServletRequest();

	@Before
	public void setup() {
		// @formatter:off
		
		mockRequest.addHeader("Accept", "application/json");
		mockRequest.addHeader("Content-Type", "application/json");
		mockRequest.addHeader("Link","<https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/feature/add-json-ld-context-for-ngsi-ld-test-suite/ngsi-ld-test-suite/ngsi-ld-test-suite-context.jsonld>; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json\"");
		
		
		payload = "{\r\n" + "	\"id\": \"urn:ngsi-ld:ContextSourceRegistration:csr1a3458\",\r\n"
				+ "	\"type\": \"ContextSourceRegistration\",\r\n" + "	\"name\": \"NameExample\",\r\n"
				+ "	\"description\": \"DescriptionExample\",\r\n" + "	\"information\": [{\r\n"
				+ "		\"entities\": [{\r\n" + "			\"id\": \"urn:ngsi-ld:Vehicle:A456\",\r\n"
				+ "			\"type\": \"Vehicle\"\r\n" + "		}],\r\n" + "		\"properties\": [\"brandName\",\r\n"
				+ "		\"speed\"],\r\n" + "		\"relationships\": [\"isParked\"]\r\n" + "	},\r\n" + "	{\r\n"
				+ "		\"entities\": [{\r\n" + "			\"idPattern\": \".*downtown$\",\r\n"
				+ "			\"type\": \"OffStreetParking\"\r\n" + "		}]\r\n" + "	}],\r\n"
				+ "	\"endpoint\": \"http://my.csource.org:1026\",\r\n"
				+ "	\"location\": \"{ \\\"type\\\": \\\"Polygon\\\", \\\"coordinates\\\": [[[8.686752319335938,49.359122687528746],[8.742027282714844,49.3642654834877],[8.767433166503904,49.398462568451485],[8.768119812011719,49.42750021620163],[8.74305725097656,49.44781634951542],[8.669242858886719,49.43754770762113],[8.63525390625,49.41968407776289],[8.637657165527344,49.3995797187007],[8.663749694824219,49.36851347448498],[8.686752319335938,49.359122687528746]]] }\",\r\n"
				+ "	\"timestamp\": {\r\n" + "		\"start\": \"2017-11-29T14:53:15Z\"\r\n" + "	},\r\n"
				+ "	\"expires\": \"2030-11-29T14:53:15Z\",\r\n"
				+ "	\"@context\": [ \""+CORE_CONTEXT_URL_STR+"\",\r\n"
				+ "	{\r\n" + "		\"Vehicle\": \"http://example.org/vehicle/Vehicle\",\r\n"
				+ "		\"brandName\": \"http://example.org/vehicle/brandName\",\r\n"
				+ "		\"speed\": \"http://example.org/vehicle/speed\",\r\n"
				+ "		\"OffStreetParking\": \"http://example.org/parking/OffStreetParking\",\r\n"
				+ "		\"isParked\": {\r\n" + "			\"@type\": \"@id\",\r\n"
				+ "			\"@id\": \"http://example.org/common/isParked\"\r\n" + "		}\r\n" + "	}]\r\n" + "}";
		// @formatter:on
		updatePayload = "{\r\n" + "	\"id\": \"urn:ngsi-ld:ContextSourceRegistration:csr1a3458\",\r\n"
				+ "	\"type\": \"ContextSourceRegistration\",\r\n" + "	\"name\": \"NameExample\",\r\n"
				+ "	\"description\": \"DescriptionExample\",\r\n" + "	\"information\": [{\r\n"
				+ "		\"entities\": [{\r\n" + "			\"id\": \"urn:ngsi-ld:Vehicle:A456\",\r\n"
				+ "			\"type\": \"Vehicle\"\r\n" + "		}],\r\n" + "		\"properties\": [\"brandName\",\r\n"
				+ "		\"speed\",\r\n" + "\"speed1\"],\r\n" + "		\"relationships\": [\"isParked\",\r\n"
				+ "\"isParked_New\"]\r\n" + "	},\r\n" + "	{\r\n" + "		\"entities\": [{\r\n"
				+ "			\"idPattern\": \".*downtown$\",\r\n" + "			\"type\": \"OffStreetParking\"\r\n"
				+ "		}]\r\n" + "	}],\r\n" + "	\"endpoint\": \"http://my.csource.org:1026\",\r\n"
				+ "	\"location\": \"{ \\\"type\\\": \\\"Polygon\\\", \\\"coordinates\\\": [[[8.686752319335938,49.359122687528746],[8.742027282714844,49.3642654834877],[8.767433166503904,49.398462568451485],[8.768119812011719,49.42750021620163],[8.74305725097656,49.44781634951542],[8.669242858886719,49.43754770762113],[8.63525390625,49.41968407776289],[8.637657165527344,49.3995797187007],[8.663749694824219,49.36851347448498],[8.686752319335938,49.359122687528746]]] }\",\r\n"
				+ "	\"timestamp\": {\r\n" + "		\"start\": \"2017-11-29T14:53:15Z\"\r\n" + "	},\r\n"
				+ "	\"expires\": \"2030-11-29T14:53:15Z\",\r\n" + "	\"@context\": [ \"" + CORE_CONTEXT_URL_STR
				+ "\",\r\n" + "	{\r\n" + "		\"Vehicle\": \"http://example.org/vehicle/Vehicle\",\r\n"
				+ "		\"brandName\": \"http://example.org/vehicle/brandName\",\r\n"
				+ "		\"speed\": \"http://example.org/vehicle/speed\",\r\n"
				+ "		\"OffStreetParking\": \"http://example.org/parking/OffStreetParking\",\r\n"
				+ "		\"isParked\": {\r\n" + "			\"@type\": \"@id\",\r\n"
				+ "			\"@id\": \"http://example.org/common/isParked\"\r\n" + "		}\r\n" + "	}]\r\n" + "}";

	}

	@After
	public void teardown() {
		payload = null;
		updatePayload = null;
		updatePayload1 = null;
	}

	/**
	 * this method is use for the csource registration
	 */

	@Test
	public void registerCSourceTest() {
		try {
			when(csourceService.createEntry(any(), any()))
					.thenReturn(new CreateResult("urn:ngsi-ld:ContextSourceRegistration:csr1a3458", true));
			ResultActions resultAction = mockMvc
					.perform(post("/ngsi-ld/v1/csourceRegistrations/")
							.contentType(AppConstants.NGB_APPLICATION_JSONLD).content(payload))
					.andExpect(status().isCreated())
					.andExpect(redirectedUrl(
							"/ngsi-ld/v1/csourceRegistrations/urn:ngsi-ld:ContextSourceRegistration:csr1a3458"))
					.andDo(print());
			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			assertEquals(201, status);
			verify(csourceService, times(1)).createEntry(any(), any());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
			e.printStackTrace();
		}

	}

	/**
	 * this method is use to validate for csource registration Already Exist
	 */

	@Test
	public void registerCSourceAlreadyExistTest() {
		try {
			when(csourceService.createEntry(any(), any())).thenThrow(
					new ResponseException(ErrorType.AlreadyExists, "urn:ngsi-ld:ContextSourceRegistration:csr1a3458"));
			ResultActions resultAction = mockMvc
					.perform(post("/ngsi-ld/v1/csourceRegistrations/").contentType(AppConstants.NGB_APPLICATION_JSONLD)
							.content(payload))
					.andExpect(status().isConflict()).andExpect(jsonPath("$.title").value("Already exists."))
					.andDo(print());

			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			assertEquals(409, status);

			verify(csourceService, times(1)).createEntry(any(), any());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
			e.printStackTrace();
		}
	}

	/**
	 * this method is use validate for csource registration BadRequest
	 */

	@Test
	public void registerCSourceBadRequestTest() {
		try {
			when(csourceService.createEntry(any(), any()))
					.thenThrow(new ResponseException(ErrorType.BadRequestData, "You have to provide a valid payload"));
			ResultActions resultAction = mockMvc
					.perform(post("/ngsi-ld/v1/csourceRegistrations/").contentType(AppConstants.NGB_APPLICATION_JSONLD)
							.content(payload))
					.andExpect(status().isBadRequest()).andExpect(jsonPath("$.title").value("Bad Request Data."))
					.andDo(print());

			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			assertEquals(400, status);

			verify(csourceService, times(1)).createEntry(any(), any());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
			e.printStackTrace();
		}
	}

	/**
	 * this method is use for delete the Csource registration
	 */

	@Test
	public void deleteCsourceTest() {
		try {
			when(csourceService.deleteEntry(any(), any())).thenReturn(true);
			ResultActions resultAction = mockMvc
					.perform(delete("/ngsi-ld/v1/csourceRegistrations/{registrationId}",
							"urn:ngsi-ld:ContextSourceRegistration:csr1a3458")
									.contentType(AppConstants.NGB_APPLICATION_JSONLD))
					.andExpect(status().isNoContent());

			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			assertEquals(204, status);
		} catch (Exception e) {
			Assert.fail(e.getMessage());
			e.printStackTrace();
		}
	}

	/**
	 * this method is validate the not found if delete the Csource registration
	 */

	@Test
	public void deleteCsourceNotFoundTest() {
		try {
			when(csourceService.deleteEntry(any(), any()))
					.thenThrow(new ResponseException(ErrorType.NotFound, "Resource not found."));
			ResultActions resultAction = mockMvc
					.perform(delete("/ngsi-ld/v1/csourceRegistrations/{registrationId}",
							"urn:ngsi-ld:ContextSourceRegistration:csr1a3458")
									.contentType(AppConstants.NGB_APPLICATION_JSONLD))
					.andExpect(status().isNotFound()).andExpect(jsonPath("$.title").value("Resource not found."));
			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse res = mvcResult.getResponse();
			int status = res.getStatus();
			assertEquals(404, status);
			verify(csourceService, times(1)).deleteEntry(any(), any());
		} catch (Exception e) {
			Assert.fail();
		}
	}

	/**
	 * this method is validate the bad request if delete the Csource registration
	 */

	@Test
	public void deleteCsourceBadRequestTest() {
		try {
			when(csourceService.deleteEntry(any(), any()))
					.thenThrow(new ResponseException(ErrorType.BadRequestData, "Bad Request Data."));
			ResultActions resultAction = mockMvc
					.perform(delete("/ngsi-ld/v1/csourceRegistrations/{registrationId}/",
							"urn:ngsi-ld:ContextSourceRegistration:csr1a3458")
									.contentType(AppConstants.NGB_APPLICATION_JSONLD))
					.andExpect(status().isBadRequest()).andExpect(jsonPath("$.title").value("Bad Request Data."));
			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse res = mvcResult.getResponse();
			int status = res.getStatus();
			assertEquals(400, status);
			verify(csourceService, times(1)).deleteEntry(any(), any());
		} catch (Exception e) {
			Assert.fail();
		}
	}

	/**
	 * this method is use for update the csource registration
	 */

	@Test
	public void updateCSourceTest() {
		try {
			UpdateResult result = mock(UpdateResult.class);
			when(csourceService.appendToEntry(any(), any(), any(), any())).thenReturn(result);
			ResultActions resultAction = mockMvc
					.perform(patch("/ngsi-ld/v1/csourceRegistrations/{registrationId}",
							"urn:ngsi-ld:ContextSourceRegistration:csr1a3458")
									.contentType(AppConstants.NGB_APPLICATION_JSONLD).content(updatePayload))
					.andExpect(status().isNoContent()).andDo(print());
			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse res = mvcResult.getResponse();
			int status = res.getStatus();
			assertEquals(204, status);
			verify(csourceService, times(1)).appendToEntry(any(), any(), any(), any());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
			e.printStackTrace();
		}
	}

	/**
	 * this method is use for validate the csource registration Not found
	 */

	@Test
	public void updateCSourceNotFoundTest() {
		try {
			UpdateResult result = mock(UpdateResult.class);
			when(csourceService.appendToEntry(any(), any(), any(), any()))
					.thenThrow(new ResponseException(ErrorType.NotFound, "Resource not found."));
			ResultActions resultAction = mockMvc
					.perform(patch("/ngsi-ld/v1/csourceRegistrations/{registrationId}",
							"urn:ngsi-ld:ContextSourceRegistration:csr1a3458")
									.contentType(AppConstants.NGB_APPLICATION_JSONLD).content(updatePayload))
					.andExpect(status().isNotFound()).andExpect(jsonPath("$.title").value("Resource not found."))
					.andDo(print());
			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse res = mvcResult.getResponse();
			int status = res.getStatus();
			assertEquals(404, status);
			verify(csourceService, times(1)).appendToEntry(any(), any(), any(), any());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
			e.printStackTrace();
		}
	}

	/**
	 * this method is use for validate the csource registration Bad Request
	 */

	@Test
	public void updateCSourceBadRequestTest() {
		try {
			UpdateResult result = mock(UpdateResult.class);
			when(csourceService.appendToEntry(any(), any(), any(), any()))
					.thenThrow(new ResponseException(ErrorType.BadRequestData, "Bad Request Data."));
			ResultActions resultAction = mockMvc
					.perform(patch("/ngsi-ld/v1/csourceRegistrations/{registrationId}",
							"urn:ngsi-ld:ContextSourceRegistration:csr1a3458")
									.contentType(AppConstants.NGB_APPLICATION_JSONLD).content(updatePayload))
					.andExpect(status().isBadRequest()).andExpect(jsonPath("$.title").value("Bad Request Data."));
			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse res = mvcResult.getResponse();
			int status = res.getStatus();
			assertEquals(400, status);
			verify(csourceService, times(1)).appendToEntry(any(), any(), any(), any());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
			e.printStackTrace();
		}
	}
	
	/**
	 * this method is use for get the csource registration by ID
	 */

	@Test
	public void getCSourceByIdTest() throws Exception {
		when(csourceService.getCSourceRegistrationById(any(), any())).thenReturn(updatePayload);
		ResultActions resultAction = mockMvc
				.perform(get("/ngsi-ld/v1/csourceRegistrations/{registrationId}/",
						"urn:ngsi-ld:ContextSourceRegistration:csr1a3458").accept(AppConstants.NGB_APPLICATION_JSON))
				.andExpect(status().isOk());

		MvcResult mvcResult = resultAction.andReturn();
		MockHttpServletResponse response = mvcResult.getResponse();
		int status = response.getStatus();
		assertEquals(200, status);
		verify(csourceService, times(1)).getCSourceRegistrationById(any(), any());
	}

	/**
	 * this method is use for validate the csource registration Id not found
	 */
	@Test
	public void getCSourceByIdNotFoundTest() throws Exception {

		when(csourceService.getCSourceRegistrationById(any(), any()))
				.thenThrow(new ResponseException(ErrorType.NotFound, "Resource not found."));
		ResultActions resultAction = mockMvc
				.perform(get("/ngsi-ld/v1/csourceRegistrations/{registrationId}/",
						"urn:ngsi-ld:ContextSourceRegistration:csr1a3451").accept(AppConstants.NGB_APPLICATION_JSON))
				.andExpect(status().isNotFound());

		MvcResult mvcResult = resultAction.andReturn();
		MockHttpServletResponse response = mvcResult.getResponse();
		int status = response.getStatus();
		assertEquals(404, status);
		verify(csourceService, times(1)).getCSourceRegistrationById(any(), any());
	}

	
	/**
	 * this method is use for validate the csource registration by ID Bad Request
	 */
	@Test
	public void getCSourceByIdBadRequestTest() throws Exception {

		when(csourceService.getCSourceRegistrationById(any(), any()))
				.thenThrow(new ResponseException(ErrorType.BadRequestData, "Bad Request Data."));
		ResultActions resultAction = mockMvc
				.perform(get("/ngsi-ld/v1/csourceRegistrations/{registrationId}/",
						"urn:ngsi-ld:ContextSourceRegistration:csr1a3451").accept(AppConstants.NGB_APPLICATION_JSON))
				.andExpect(status().isBadRequest());

		MvcResult mvcResult = resultAction.andReturn();
		MockHttpServletResponse response = mvcResult.getResponse();
		int status = response.getStatus();
		assertEquals(400, status);
		verify(csourceService, times(1)).getCSourceRegistrationById(any(), any());
	}
	
	/**
	 * this method is use for get the discover csource registration
	 */

	@Test
	public void getDiscoverCSourceTest() throws Exception {

		QueryResult result = mock(QueryResult.class);
		when(csourceService.getData(any(), any(), any(), any(), any())).thenReturn(result);

		ResultActions resultAction = mockMvc
				.perform(get("/ngsi-ld/v1/csourceRegistrations?type=Test1").accept(AppConstants.NGB_APPLICATION_JSON))
				.andExpect(status().isOk());

		MvcResult mvcResult = resultAction.andReturn();
		MockHttpServletResponse response = mvcResult.getResponse();
		int status = response.getStatus();
		assertEquals(200, status);
		verify(csourceService, times(1)).getData(any(), any(), any(), any(), any());
	}
	
	/**
	 * this method is use for validate the discover csource registration Bad Request 
	 */

	@Test
	public void getDiscoverCSourceBadRequestTest() throws Exception {

		QueryResult result = mock(QueryResult.class);
		when(csourceService.getData(any(), any(), any(), any(), any()))
				.thenThrow(new ResponseException(ErrorType.BadRequestData, "Bad Request Data."));

		ResultActions resultAction = mockMvc
				.perform(get("/ngsi-ld/v1/csourceRegistrations").accept(AppConstants.NGB_APPLICATION_JSON))
				.andExpect(status().isBadRequest());

		MvcResult mvcResult = resultAction.andReturn();
		MockHttpServletResponse response = mvcResult.getResponse();
		int status = response.getStatus();
		assertEquals(400, status);
		verify(csourceService, times(0)).getData(any(), any(), any(), any(), any());
	}

	/**
	 * this method is use for validate the discover csource registration Not Found
	 */

	@Test
	public void getDiscoverCSourceNotFoundTest() throws Exception {
		QueryResult result = mock(QueryResult.class);
		when(csourceService.getData(any(), any(), any(), any(), any()))
				.thenThrow(new ResponseException(ErrorType.NotFound, "Resource not found."));
		ResultActions resultAction = mockMvc
				.perform(get("/ngsi-ld/v1/csourceRegistrations?type=Test1").accept(AppConstants.NGB_APPLICATION_JSON))
				.andExpect(status().isNotFound());
		MvcResult mvcResult = resultAction.andReturn();
		MockHttpServletResponse response = mvcResult.getResponse();
		int status = response.getStatus();
		assertEquals(404, status);
		verify(csourceService, times(1)).getData(any(), any(), any(), any(), any());
	}

}
