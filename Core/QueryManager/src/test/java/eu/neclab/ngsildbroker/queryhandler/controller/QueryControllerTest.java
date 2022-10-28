
package eu.neclab.ngsildbroker.queryhandler.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunnerDelegate;
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
import eu.neclab.ngsildbroker.commons.datatypes.results.QueryResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.queryhandler.services.QueryService;

@AutoConfigureMockMvc
@PowerMockRunnerDelegate(SpringRunner.class)
@SuppressWarnings("unchecked")
@RunWith(SpringRunner.class)
@PrepareForTest(QueryController.class)
@SpringBootTest
public class QueryControllerTest {
	@Autowired
	private MockMvc mockMvc;
	@MockBean
	private QueryService queryService;
	MockHttpServletRequest mockRequest = new MockHttpServletRequest();
	private String entity;
	private String response = "";
	private String linkHeader = "<<http://localhost:9090/ngsi-ld/contextes/urn:ngsi-ld:Vehicle:A100>; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json\">";
	private List<String> entities;
	private List<String> entitiesBadRequest;

	@Before
	public void setup() {
		mockRequest.addHeader("Accept", "application/json");
		mockRequest.addHeader("Content-Type", "application/json");
		mockRequest.addHeader("Link",
				"<https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/feature/add-json-ld-context-for-ngsi-ld-test-suite/ngsi-ld-test-suite/ngsi-ld-test-suite-context.jsonld>; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json\"");

		entity = "[{\r\n" + "	\"http://example.org/vehicle/brandName\": [{\r\n"
				+ "		\"@type\": [\"https://uri.etsi.org/ngsi-ld/Property\"],\r\n"
				+ "		\"https://uri.etsi.org/ngsi-ld/hasValue\": [{\r\n" + "			\"@value\": \"Mercedes\"\r\n"
				+ "		}]\r\n" + "	}],\r\n" + "	\"https://uri.etsi.org/ngsi-ld/createdAt\": [{\r\n"
				+ "		\"@type\": \"https://uri.etsi.org/ngsi-ld/DateTime\",\r\n"
				+ "		\"@value\": \"2017-07-29T12:00:04Z\"\r\n" + "	}],\r\n"
				+ "	\"@id\": \"urn:ngsi-ld:Vehicle:A100\",\r\n" + "	\"http://example.org/common/isParked\": [{\r\n"
				+ "		\"https://uri.etsi.org/ngsi-ld/hasObject\": [{\r\n"
				+ "			\"@id\": \"urn:ngsi-ld:OffStreetParking:Downtown1\"\r\n" + "		}],\r\n"
				+ "		\"https://uri.etsi.org/ngsi-ld/observedAt\": [{\r\n"
				+ "			\"@type\": \"https://uri.etsi.org/ngsi-ld/DateTime\",\r\n"
				+ "			\"@value\": \"2017-07-29T12:00:04Z\"\r\n" + "		}],\r\n"
				+ "		\"http://example.org/common/providedBy\": [{\r\n"
				+ "			\"https://uri.etsi.org/ngsi-ld/hasObject\": [{\r\n"
				+ "				\"@id\": \"urn:ngsi-ld:Person:Bob\"\r\n" + "			}],\r\n"
				+ "			\"@type\": [\"https://uri.etsi.org/ngsi-ld/Relationship\"]\r\n" + "		}],\r\n"
				+ "		\"@type\": [\"https://uri.etsi.org/ngsi-ld/Relationship\"]\r\n" + "	}],\r\n"
				+ "	\"https://uri.etsi.org/ngsi-ld/location\": [{\r\n"
				+ "		\"@type\": [\"https://uri.etsi.org/ngsi-ld/GeoProperty\"],\r\n"
				+ "		\"https://uri.etsi.org/ngsi-ld/hasValue\": [{\r\n"
				+ "			\"@value\": \"{ \\\"type\\\":\\\"Point\\\", \\\"coordinates\\\":[ -8.5, 41.2 ] }\"\r\n"
				+ "		}]\r\n" + "	}],\r\n" + "	\"http://example.org/vehicle/speed\": [{\r\n"
				+ "		\"@type\": [\"https://uri.etsi.org/ngsi-ld/Property\"],\r\n"
				+ "		\"https://uri.etsi.org/ngsi-ld/hasValue\": [{\r\n" + "			\"@value\": 80\r\n"
				+ "		}]\r\n" + "	}],\r\n" + "	\"@type\": [\"http://example.org/vehicle/Vehicle\"]\r\n" + "}]";

		response = "{\r\n" + "	\"id\": \"urn:ngsi-ld:Vehicle:A100\",\r\n" + "	\"type\": \"Vehicle\",\r\n"
				+ "	\"brandName\": {\r\n" + "		\"type\": \"Property\",\r\n" + "		\"value\": \"Mercedes\"\r\n"
				+ "	},\r\n" + "	\"isParked\": {\r\n" + "		\"type\": \"Relationship\",\r\n"
				+ "		\"object\": \"urn:ngsi-ld:OffStreetParking:Downtown1\",\r\n"
				+ "		\"observedAt\": \"2017-07-29T12:00:04Z\",\r\n" + "		\"providedBy\": {\r\n"
				+ "			\"type\": \"Relationship\",\r\n" + "			\"object\": \"urn:ngsi-ld:Person:Bob\"\r\n"
				+ "		}\r\n" + "	},\r\n" + "	\"speed\": {\r\n" + "		\"type\": \"Property\",\r\n"
				+ "		\"value\": 80\r\n" + "	},\r\n" + "	\"createdAt\": \"2017-07-29T12:00:04Z\",\r\n"
				+ "	\"location\": {\r\n" + "		\"type\": \"GeoProperty\",\r\n"
				+ "		\"value\": { \"type\":\"Point\", \"coordinates\":[ -8.5, 41.2 ] }\r\n" + "	}\r\n" + "}";

		entities = new ArrayList<String>(Arrays.asList("{\r\n" + "    \"id\": \"urn:ngsi-ld:Vehicle:A101\",\r\n"
				+ "    \"type\": \"Vehicle\",\r\n" + "    \"brandName\": {\r\n" + "        \"type\": \"Property\",\r\n"
				+ "        \"value\": \"Mercedes\"\r\n" + "    },\r\n" + "    \"speed\": [\r\n" + "        {\r\n"
				+ "            \"type\": \"Property\",\r\n"
				+ "            \"datasetId\": \"urn:ngsi-ld:Property:speedometerA4567-speed\",\r\n"
				+ "            \"source\": {\r\n" + "                \"type\": \"Property\",\r\n"
				+ "                \"value\": \"Speedometer\"\r\n" + "            },\r\n"
				+ "            \"value\": 55\r\n" + "        },\r\n" + "        {\r\n"
				+ "            \"type\": \"Property\",\r\n" + "            \"source\": {\r\n"
				+ "                \"type\": \"Property\",\r\n" + "                \"value\": \"GPS\"\r\n"
				+ "            },\r\n" + "            \"value\": 60\r\n" + "        },\r\n" + "        {\r\n"
				+ "            \"type\": \"Property\",\r\n" + "            \"source\": {\r\n"
				+ "                \"type\": \"Property\",\r\n" + "                \"value\": \"GPS_NEW\"\r\n"
				+ "            },\r\n" + "            \"value\": 52.5\r\n" + "        }\r\n" + "    ],\r\n"
				+ "    \"location\": {\r\n" + "        \"type\": \"GeoProperty\",\r\n" + "        \"value\": {\r\n"
				+ "            \"type\": \"Point\",\r\n" + "            \"coordinates\": [\r\n"
				+ "                -8.5,\r\n" + "                41.2\r\n" + "            ]\r\n" + "        }\r\n"
				+ "    }\r\n" + "}"));

		entitiesBadRequest = new ArrayList<String>(Arrays.asList());
	}

	@After
	public void tearDown() {
		entity = null;
		response = null;
		entitiesBadRequest = null;
	}

	/**
	 * this method is use for get Entity By Id
	 */

	@Test
	public void getEntityByIdTest() throws Exception {
		try {
			QueryResult result = new QueryResult(entities, null, ErrorType.None, -1, true);
			result.setActualDataString(entities);
			when(queryService.getData(any(), any(), any(), any(), any())).thenReturn(result);
			ResultActions resultAction = mockMvc
					.perform(get("/ngsi-ld/v1/entities/{entityId}", "urn:ngsi-ld:Vehicle:A101")
							.accept(AppConstants.NGB_APPLICATION_JSON))
					.andExpect(status().isOk()).andExpect(jsonPath("$.id").value("urn:ngsi-ld:Vehicle:A101"));
			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			verify(queryService, times(1)).getData(any(), any(), any(), any(), any());
			assertEquals(200, status);
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}

	}

	/**
	 * this method is use for validate the Id Not Found
	 */
	@Test
	public void getEntityByIdNotFoundTest() throws Exception {
		try {

			QueryResult result = new QueryResult(entities, null, ErrorType.None, -1, true);
			result.setActualDataString(entities);
			when(queryService.getData(any(), any(), any(), any(), any()))
					.thenThrow(new ResponseException(ErrorType.NotFound, "Resource not found.")).thenReturn(result);

			ResultActions resultAction = mockMvc
					.perform(get("/ngsi-ld/v1/entities/{entityId}", "urn:ngsi-ld:Vehicle:A101")
							.accept(AppConstants.NGB_APPLICATION_JSON))
					.andExpect(status().isNotFound());

			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			assertEquals(404, status);
			verify(queryService, times(1)).getData(any(), any(), any(), any(), any());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	/**
	 * this method is use for validate the Bad Request Data
	 */
	@Test
	public void getEntityByIdBadRequestTest() throws Exception {
		try {

			QueryResult result = new QueryResult(entitiesBadRequest, null, ErrorType.None, -1, true);
			result.setActualDataString(entitiesBadRequest);
			when(queryService.getData(any(), any(), any(), any(), any()))
					.thenThrow(new ResponseException(ErrorType.BadRequestData, "invalid query")).thenReturn(result);

			ResultActions resultAction = mockMvc
					.perform(get("/ngsi-ld/v1/entities/{entityId}", "").accept(AppConstants.NGB_APPLICATION_JSON))
					.andExpect(status().isBadRequest());

			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			assertEquals(400, status);
			verify(queryService, times(0)).getData(any(), any(), any(), any(), any());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	/**
	 * this method is use for validate the Id Not Found in getEntity Method
	 */
	@Test
	public void getEntityByIdNotFoundOutTest() throws Exception {
		try {

			QueryResult result = mock(QueryResult.class);
			when(queryService.getData(any(), any(), any(), any(), any())).thenReturn(result);
			ResultActions resultAction = mockMvc
					.perform(get("/ngsi-ld/v1/entities/{entityId}", "urn:ngsi-ld:Vehicle:A101")
							.accept(AppConstants.NGB_APPLICATION_JSON))
					.andExpect(status().isNotFound());
			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			assertEquals(404, status);
			verify(queryService, times(1)).getData(any(), any(), any(), any(), any());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	/**
	 * this method is use for get Query Entities
	 */
	@Test
	public void getQueryForEntitiesTest() throws Exception {
		try {

			QueryResult result = mock(QueryResult.class);
			when(queryService.getData(any(), any(), any(), any(), any())).thenReturn(result);

			ResultActions resultAction = mockMvc
					.perform(get("/ngsi-ld/v1/entities?type=Test1").accept(AppConstants.NGB_APPLICATION_JSON))
					.andExpect(status().isOk());

			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			assertEquals(200, status);
			verify(queryService, times(1)).getData(any(), any(), any(), any(), any());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	/**
	 * this method is use for validate the Id Not Found
	 */
	@Test
	public void getQueryForEntitiesNotFoundTest() throws Exception {
		try {

			QueryResult result = mock(QueryResult.class);
			when(queryService.getData(any(), any(), any(), any(), any()))
					.thenThrow(new ResponseException(ErrorType.TenantNotFound, "Tenant not found."));

			ResultActions resultAction = mockMvc
					.perform(get("/ngsi-ld/v1/entities?type=Test1").accept(AppConstants.NGB_APPLICATION_JSON))
					.andExpect(status().isNotFound());

			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			assertEquals(404, status);
			verify(queryService, times(1)).getData(any(), any(), any(), any(), any());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	/**
	 * this method is use for validate the Query Entities BadRequestData
	 */
	@Test
	public void getQueryForEntitiesBadRequestTest() throws Exception {
		try {

			QueryResult result = mock(QueryResult.class);
			when(queryService.getData(any(), any(), any(), any(), any()))
					.thenThrow(new ResponseException(ErrorType.BadRequestData, "invalid query"));

			ResultActions resultAction = mockMvc
					.perform(get("/ngsi-ld/v1/entities").accept(AppConstants.NGB_APPLICATION_JSON))
					.andExpect(status().isBadRequest());

			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			assertEquals(400, status);
			verify(queryService, times(0)).getData(any(), any(), any(), any(), any());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	/**
	 * this method is use for get all Types properties
	 */
	@Test
	public void getAllTypesTest() throws Exception {
		try {
			QueryResult result = new QueryResult(entities, null, ErrorType.None, -1, true);
			result.setActualDataString(entities);
			when(queryService.getData(any(), any(), any(), any(), any())).thenReturn(result);
			ResultActions resultAction = mockMvc
					.perform(get("/ngsi-ld/v1/types").accept(AppConstants.NGB_APPLICATION_JSON))
					.andExpect(status().isOk()).andExpect(jsonPath("$.id").value("urn:ngsi-ld:Vehicle:A101"));
			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			verify(queryService, times(1)).getData(any(), any(), any(), any(), any());
			assertEquals(200, status);
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	/**
	 * this method is use for validate getAllTypes Not Found
	 */
	@Test
	public void getAllTypesNotFoundTest() throws Exception {
		try {

			QueryResult result = new QueryResult(entities, null, ErrorType.None, -1, true);
			result.setActualDataString(entities);
			when(queryService.getData(any(), any(), any(), any(), any()))
					.thenThrow(new ResponseException(ErrorType.NotFound, "Resource not found.")).thenReturn(result);

			ResultActions resultAction = mockMvc
					.perform(get("/ngsi-ld/v1/types").accept(AppConstants.NGB_APPLICATION_JSON))
					.andExpect(status().isNotFound());

			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			assertEquals(404, status);
			verify(queryService, times(1)).getData(any(), any(), any(), any(), any());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	/**
	 * this method is use for validate getAllTypes Not Found in getAllTypes
	 */
	@Test
	public void getAllTypesNotFoundOutTest() throws Exception {
		try {
			QueryResult result = mock(QueryResult.class);
			when(queryService.getData(any(), any(), any(), any(), any())).thenReturn(result);
			ResultActions resultAction = mockMvc
					.perform(get("/ngsi-ld/v1/types").accept(AppConstants.NGB_APPLICATION_JSON))
					.andExpect(status().isNotFound());
			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			assertEquals(404, status);
			verify(queryService, times(1)).getData(any(), any(), any(), any(), any());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	/**
	 * this method is use for get Types by entityType
	 */
	@Test
	public void getTypeByEntityTypeTest() throws Exception {
		try {
			QueryResult result = new QueryResult(entities, null, ErrorType.None, -1, true);
			result.setActualDataString(entities);
			when(queryService.getData(any(), any(), any(), any(), any())).thenReturn(result);
			ResultActions resultAction = mockMvc
					.perform(get("/ngsi-ld/v1/types/{entityType}", "Vehicle").accept(AppConstants.NGB_APPLICATION_JSON))
					.andExpect(status().isOk()).andExpect(jsonPath("$.id").value("urn:ngsi-ld:Vehicle:A101"));
			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			verify(queryService, times(1)).getData(any(), any(), any(), any(), any());
			assertEquals(200, status);
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}

	}

	/**
	 * this method is use for Validate getTypes by entityType Not Found
	 */
	@Test
	public void getTypeByIdNotFoundTest() throws Exception {
		try {

			QueryResult result = new QueryResult(entities, null, ErrorType.None, -1, true);
			result.setActualDataString(entities);
			when(queryService.getData(any(), any(), any(), any(), any()))
					.thenThrow(new ResponseException(ErrorType.NotFound, "Resource not found.")).thenReturn(result);

			ResultActions resultAction = mockMvc
					.perform(get("/ngsi-ld/v1/types/{entityType}", "Vehicle").accept(AppConstants.NGB_APPLICATION_JSON))
					.andExpect(status().isNotFound());

			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			assertEquals(404, status);
			verify(queryService, times(1)).getData(any(), any(), any(), any(), any());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	/**
	 * this method is use for get Types by entityType Not Found
	 */
	@Test
	public void getTypeByIdNotFoundOutTest() throws Exception {
		try {

			QueryResult result = mock(QueryResult.class);
			when(queryService.getData(any(), any(), any(), any(), any())).thenReturn(result);
			ResultActions resultAction = mockMvc
					.perform(get("/ngsi-ld/v1/types/{entityType}", "Vehicle").accept(AppConstants.NGB_APPLICATION_JSON))
					.andExpect(status().isNotFound());
			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			assertEquals(404, status);
			verify(queryService, times(1)).getData(any(), any(), any(), any(), any());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	/**
	 * this method is use for get All Attributes
	 */
	@Test
	public void getAllAttributesTest() throws Exception {
		try {
			QueryResult result = new QueryResult(entities, null, ErrorType.None, -1, true);
			result.setActualDataString(entities);
			when(queryService.getData(any(), any(), any(), any(), any())).thenReturn(result);
			ResultActions resultAction = mockMvc
					.perform(get("/ngsi-ld/v1/attributes").accept(AppConstants.NGB_APPLICATION_JSON))
					.andExpect(status().isOk()).andExpect(jsonPath("$.id").value("urn:ngsi-ld:Vehicle:A101"));
			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			verify(queryService, times(1)).getData(any(), any(), any(), any(), any());
			assertEquals(200, status);
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}

	}

	/**
	 * this method is use for validate get All Attributes Not Found
	 */
	@Test
	public void getAllAttributesNotFoundOutTest() throws Exception {
		try {

			QueryResult result = mock(QueryResult.class);
			when(queryService.getData(any(), any(), any(), any(), any())).thenReturn(result);
			ResultActions resultAction = mockMvc
					.perform(get("/ngsi-ld/v1/attributes").accept(AppConstants.NGB_APPLICATION_JSON))
					.andExpect(status().isNotFound());
			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			assertEquals(404, status);
			verify(queryService, times(1)).getData(any(), any(), any(), any(), any());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	/**
	 * this method is use for validate get All Attributes Not Found in
	 * getAllAttribute method
	 */
	@Test
	public void getAllAttributesNotFoundTest() throws Exception {
		try {

			QueryResult result = new QueryResult(entities, null, ErrorType.None, -1, true);
			result.setActualDataString(entities);
			when(queryService.getData(any(), any(), any(), any(), any()))
					.thenThrow(new ResponseException(ErrorType.NotFound, "Resource not found.")).thenReturn(result);

			ResultActions resultAction = mockMvc
					.perform(get("/ngsi-ld/v1/attributes").accept(AppConstants.NGB_APPLICATION_JSON))
					.andExpect(status().isNotFound());

			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			assertEquals(404, status);
			verify(queryService, times(1)).getData(any(), any(), any(), any(), any());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	/**
	 * this method is use for get Attributes by Attr
	 */
	@Test
	public void getAttributeByAttrTest() throws Exception {
		try {
			QueryResult result = new QueryResult(entities, null, ErrorType.None, -1, true);
			result.setActualDataString(entities);
			when(queryService.getData(any(), any(), any(), any(), any())).thenReturn(result);
			ResultActions resultAction = mockMvc.perform(
					get("/ngsi-ld/v1/attributes/{attribute}", "brandName").accept(AppConstants.NGB_APPLICATION_JSON))
					.andExpect(status().isOk());

			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			verify(queryService, times(1)).getData(any(), any(), any(), any(), any());
			assertEquals(200, status);
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}

	}

	/**
	 * this method is use for validate get Attributes by Attr Not Found
	 */
	@Test
	public void getAttributeByAttrNotFoundOutTest() throws Exception {
		try {

			QueryResult result = mock(QueryResult.class);
			when(queryService.getData(any(), any(), any(), any(), any())).thenReturn(result);
			ResultActions resultAction = mockMvc.perform(
					get("/ngsi-ld/v1/attributes/{attribute}", "brandName").accept(AppConstants.NGB_APPLICATION_JSON))
					.andExpect(status().isNotFound());
			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			assertEquals(404, status);
			verify(queryService, times(1)).getData(any(), any(), any(), any(), any());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	/**
	 * this method is use for validate get Attributes by Attr in getAttr method
	 */
	@Test
	public void getAttributeByAttrNotFoundTest() throws Exception {
		try {

			QueryResult result = new QueryResult(entities, null, ErrorType.None, -1, true);
			result.setActualDataString(entities);
			when(queryService.getData(any(), any(), any(), any(), any()))
					.thenThrow(new ResponseException(ErrorType.NotFound, "Resource not found.")).thenReturn(result);

			ResultActions resultAction = mockMvc.perform(
					get("/ngsi-ld/v1/attributes/{attribute}", "brandName").accept(AppConstants.NGB_APPLICATION_JSON))
					.andExpect(status().isNotFound());

			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			assertEquals(404, status);
			verify(queryService, times(1)).getData(any(), any(), any(), any(), any());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}
}
