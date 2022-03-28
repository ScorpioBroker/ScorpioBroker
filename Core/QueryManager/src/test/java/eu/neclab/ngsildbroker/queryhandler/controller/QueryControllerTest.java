
package eu.neclab.ngsildbroker.queryhandler.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.redirectedUrl;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunnerDelegate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.ResultActions;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.controllers.QueryControllerFunctions;
import eu.neclab.ngsildbroker.commons.datatypes.results.QueryResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.EntryQueryService;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
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

//  @Autowired 
//  ParamsResolver paramsResolver; 
// 
	@InjectMocks
	@Spy
	QueryController qc;

	private QueryControllerFunctions qcf;

	private EntryQueryService eqs;

	MockHttpServletRequest mockRequest = new MockHttpServletRequest();

	private String entity;
	private String response = "";
	private String linkHeader = "<<http://localhost:9090/ngsi-ld/contextes/urn:ngsi-ld:Vehicle:A100>; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json\">";
	private List<String> entities;

	@Before
	public void setup() {
		// PowerMockito.mockStatic(HttpUtils.class);
		mockRequest.addHeader("Accept", "application/json");
		mockRequest.addHeader("Content-Type", "application/json");
		mockRequest.addHeader("Link",
				"<https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/feature/add-json-ld-context-for-ngsi-ld-test-suite/ngsi-ld-test-suite/ngsi-ld-test-suite-context.jsonld>; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json\"");
		// MockitoAnnotations.initMocks(this);
		MockitoAnnotations.openMocks(this);
		// this.mockMvc = MockMvcBuilders.standaloneSetup(qc).build();
	// @formatter:off
  entity = "[{\r\n" + "	\"http://example.org/vehicle/brandName\": [{\r\n" +
  "		\"@type\": [\"https://uri.etsi.org/ngsi-ld/Property\"],\r\n" +
  "		\"https://uri.etsi.org/ngsi-ld/hasValue\": [{\r\n" +
  "			\"@value\": \"Mercedes\"\r\n" + "		}]\r\n" + "	}],\r\n" +
  "	\"https://uri.etsi.org/ngsi-ld/createdAt\": [{\r\n" +
  "		\"@type\": \"https://uri.etsi.org/ngsi-ld/DateTime\",\r\n" +
  "		\"@value\": \"2017-07-29T12:00:04Z\"\r\n" + "	}],\r\n" +
  "	\"@id\": \"urn:ngsi-ld:Vehicle:A100\",\r\n" +
  "	\"http://example.org/common/isParked\": [{\r\n" +
  "		\"https://uri.etsi.org/ngsi-ld/hasObject\": [{\r\n" +
  "			\"@id\": \"urn:ngsi-ld:OffStreetParking:Downtown1\"\r\n" +
  "		}],\r\n" +
  "		\"https://uri.etsi.org/ngsi-ld/observedAt\": [{\r\n" +
  "			\"@type\": \"https://uri.etsi.org/ngsi-ld/DateTime\",\r\n" +
  "			\"@value\": \"2017-07-29T12:00:04Z\"\r\n" + "		}],\r\n" +
  "		\"http://example.org/common/providedBy\": [{\r\n" +
  "			\"https://uri.etsi.org/ngsi-ld/hasObject\": [{\r\n" +
  "				\"@id\": \"urn:ngsi-ld:Person:Bob\"\r\n" +
  "			}],\r\n" +
  "			\"@type\": [\"https://uri.etsi.org/ngsi-ld/Relationship\"]\r\n"
  + "		}],\r\n" +
  "		\"@type\": [\"https://uri.etsi.org/ngsi-ld/Relationship\"]\r\n" +
  "	}],\r\n" + "	\"https://uri.etsi.org/ngsi-ld/location\": [{\r\n" +
  "		\"@type\": [\"https://uri.etsi.org/ngsi-ld/GeoProperty\"],\r\n" +
  "		\"https://uri.etsi.org/ngsi-ld/hasValue\": [{\r\n" +
  "			\"@value\": \"{ \\\"type\\\":\\\"Point\\\", \\\"coordinates\\\":[ -8.5, 41.2 ] }\"\r\n"
  + "		}]\r\n" + "	}],\r\n" +
  "	\"http://example.org/vehicle/speed\": [{\r\n" +
  "		\"@type\": [\"https://uri.etsi.org/ngsi-ld/Property\"],\r\n" +
  "		\"https://uri.etsi.org/ngsi-ld/hasValue\": [{\r\n" +
  "			\"@value\": 80\r\n" + "		}]\r\n" + "	}],\r\n" +
  "	\"@type\": [\"http://example.org/vehicle/Vehicle\"]\r\n" + "}]";
  
  response = "{\r\n" + "	\"id\": \"urn:ngsi-ld:Vehicle:A100\",\r\n" +
  "	\"type\": \"Vehicle\",\r\n" + "	\"brandName\": {\r\n" +
  "		\"type\": \"Property\",\r\n" + "		\"value\": \"Mercedes\"\r\n"
  + "	},\r\n" + "	\"isParked\": {\r\n" +
  "		\"type\": \"Relationship\",\r\n" +
  "		\"object\": \"urn:ngsi-ld:OffStreetParking:Downtown1\",\r\n" +
  "		\"observedAt\": \"2017-07-29T12:00:04Z\",\r\n" +
  "		\"providedBy\": {\r\n" + "			\"type\": \"Relationship\",\r\n"
  + "			\"object\": \"urn:ngsi-ld:Person:Bob\"\r\n" + "		}\r\n" +
  "	},\r\n" + "	\"speed\": {\r\n" + "		\"type\": \"Property\",\r\n" +
  "		\"value\": 80\r\n" + "	},\r\n" +
  "	\"createdAt\": \"2017-07-29T12:00:04Z\",\r\n" + "	\"location\": {\r\n"
  + "		\"type\": \"GeoProperty\",\r\n" +
  "		\"value\": { \"type\":\"Point\", \"coordinates\":[ -8.5, 41.2 ] }\r\n"
  + "	}\r\n" + "}";
  
  entities = new ArrayList<String>(Arrays.asList("{\r\n" +
  "  \"http://example.org/vehicle/brandName\" : [ {\r\n" +
  "    \"@value\" : \"Volvo\"\r\n" + "  } ],\r\n" +
  "  \"@id\" : \"urn:ngsi-ld:Vehicle:A100\",\r\n" +
  "  \"http://example.org/vehicle/speed\" : [ {\r\n" +
  "    \"https://uri.etsi.org/ngsi-ld/instanceId\" : [ {\r\n" +
  "      \"@value\" : \"be664aaf-a7af-4a99-bebc-e89528238abf\"\r\n" +
  "    } ],\r\n" + "    \"https://uri.etsi.org/ngsi-ld/observedAt\" : [ {\r\n"
  + "      \"@value\" : \"2018-06-01T12:03:00Z\",\r\n" +
  "      \"@type\" : \"https://uri.etsi.org/ngsi-ld/DateTime\"\r\n" +
  "    } ],\r\n" +
  "    \"@type\" : [ \"https://uri.etsi.org/ngsi-ld/Property\" ],\r\n" +
  "    \"https://uri.etsi.org/ngsi-ld/hasValue\" : [ {\r\n" +
  "      \"@value\" : \"120\"\r\n" + "    } ]\r\n" + "  }, {\r\n" +
  "    \"https://uri.etsi.org/ngsi-ld/instanceId\" : [ {\r\n" +
  "      \"@value\" : \"d3ac28df-977f-4151-a432-dc088f7400d7\"\r\n" +
  "    } ],\r\n" + "    \"https://uri.etsi.org/ngsi-ld/observedAt\" : [ {\r\n"
  + "      \"@value\" : \"2018-08-01T12:05:00Z\",\r\n" +
  "      \"@type\" : \"https://uri.etsi.org/ngsi-ld/DateTime\"\r\n" +
  "    } ],\r\n" +
  "    \"@type\" : [ \"https://uri.etsi.org/ngsi-ld/Property\" ],\r\n" +
  "    \"https://uri.etsi.org/ngsi-ld/hasValue\" : [ {\r\n" +
  "      \"@value\" : \"80\"\r\n" + "    } ]\r\n" + "  } ],\r\n" +
  "  \"@type\" : [ \"http://example.org/vehicle/Vehicle\" ]\r\n" + "}"));
  // @formatter:on
	}

	@After
	public void tearDown() {
		entity = null;
		response = null;
	}

	/*
	 * @Test public void getEntityTest() throws Exception { try {
	 * ResponseEntity<String> responseEntity =
	 * ResponseEntity.status(HttpStatus.OK).header("location",
	 * "<<http://localhost:9090/ngsi-ld/contextes/urn:ngsi-ld:Vehicle:A100>; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json\">"
	 * ) .body(response);
	 * 
	 * // ResponseEntity<String> actualresponse =
	 * EntryControllerFunctions.createEntry(entityService, mockRequest,
	 * entityPayload, AppConstants.ENTITY_CREATE_PAYLOAD, AppConstants.ENTITES_URL,
	 * logger); //ResponseEntity<String> actualresponse=
	 * QueryControllerFunctions.getEntity(queryService, mockRequest, entities,
	 * entities,"urn:ngsi-ld:Vehicle:A100" , false, 0, 0);
	 * 
	 * Mockito.doReturn(responseEntity).when(qc).getEntity(any(), any(), any(),
	 * any()); QueryResult result = new QueryResult(entities, null, ErrorType.None,
	 * -1,true); Mockito.doReturn(result).when(queryService).getData(any(),
	 * any(),any(), any(), any());
	 * mockMvc.perform(get("/ngsi-ld/v1/entities/{entityId}",
	 * "urn:ngsi-ld:Vehicle:A100").accept(AppConstants.NGB_APPLICATION_JSON))
	 * .andExpect(status().isOk()).andExpect(jsonPath("$.id").value(
	 * "urn:ngsi-ld:Vehicle:A100")).andExpect(redirectedUrl(linkHeader)).andDo(print
	 * ()); verify(queryService, times(1)).getData(any(), any(), any(), any(),
	 * any()); verify(qc, times(1)).getEntity(any(), any(),any(),any());
	 * 
	 * } catch (Exception e) { Assert.fail(e.getMessage()); } }
	 */
	/*
	 * @org.junit.jupiter.api.Test public void getEntityTest() throws Exception {
	 * try {
	 * 
	 * ResponseEntity<String> responseEntity=
	 * ResponseEntity.status(HttpStatus.OK).header("location",
	 * "<<http://localhost:9090/ngsi-ld/contextes/urn:ngsi-ld:Vehicle:A100>; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json\">"
	 * ) .body(response);
	 * 
	 * QueryResult resutl = Mockito.mock(QueryResult.class);
	 * resutl.setActualDataString(entities);
	 * 
	 * qcf = Mockito.mock(QueryControllerFunctions.class); QueryControllerFunctions
	 * qcf1 = PowerMockito.spy(qcf); //
	 * PowerMockito.doReturn(responseEntity).when(qcf1,"getQueryData",any(),any(),
	 * any(),any(),any(),any(),any(),any(),any(),any()); //
	 * Mockito.doReturn(responseEntity).when(qcf1,"getQueryData",queryService,
	 * mockRequest,"urn:ngsi-ld:Vehicle:A100",); when(queryService.getData(any(),
	 * any(), any(), any(),any())).thenReturn(resutl);
	 * 
	 * mockMvc.perform(get("/ngsi-ld/v1/entities/{entityId}",
	 * "urn:ngsi-ld:Vehicle:A100").accept(AppConstants.NGB_APPLICATION_JSON))
	 * .andExpect(status().isOk()).andExpect(jsonPath("$.id").value(
	 * "urn:ngsi-ld:Vehicle:A100"))
	 * .andExpect(redirectedUrl(linkHeader)).andDo(print());
	 * 
	 * } catch (Exception e) { Assert.fail(e.getMessage()); }
	 * 
	 * }
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

	/*
	 * @Test() public void getAttrsTest() { try { //QueryController
	 * qc=Mockito.mock(QueryController.class); Set<Object> linkHeaders = new
	 * HashSet<Object>(); // Map<String, Object> entityContext = new HashMap<String,
	 * Object>(); // String jsonLdResolved = "{\r\n" +
	 * " \"http://example.org/vehicle/brandName\": // [{\r\n" +" \"@value\": 0\r\n"
	 * + " }]\r\n" + "}"; // ResponseEntity<String > responseEntity=
	 * ResponseEntity.status(HttpStatus.OK).header(
	 * "location","<<http://localhost:9090/ngsi-ld/contextes/urn:ngsi-ld:Vehicle:A100>; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json\">"
	 * ) .body(response); linkHeaders.add(
	 * "http://localhost:9090/ngsi-ld/contextes/urn:ngsi-ld:Vehicle:A100");
	 * 
	 * // entityContext.put("urn:ngsi-ld:Vehicle:A100",
	 * "{\"brandName\":\"http://example.org/vehicle/brandName\"}"); // // // // //
	 * when(paramsResolver.expandAttribute(any(),
	 * any())).thenReturn(jsonLdResolved); // // when(HttpUtils.generateReply(any(),
	 * any(), any(), any(), any())).thenReturn(responseEntity); // // QueryResult
	 * result = new QueryResult(entities, null, ErrorType.None, -1, true); //
	 * when(queryService.getData(any(),any(),any(),any(),any())).thenReturn(result);
	 * // // mockMvc.perform(get("/ngsi-ld/v1/entities/{entityId}?attrs=brandName",
	 * "urn:ngsi-ld:Vehicle:A100") //
	 * .accept(AppConstants.NGB_APPLICATION_JSON)).andExpect(status().isOk()) //
	 * .andExpect(jsonPath("$.id").value("urn:ngsi-ld:Vehicle:A100")).andDo(print())
	 * ; // Mockito.verify(queryService,times(1)).getData(any(), any(), any(),
	 * any(),any()); // // Mockito.verify(HttpUtils, times(1)).generateReply(any(),
	 * any(), any(),any(), any(), any());
	 * 
	 * // MultiValueMap<String,String> map = new
	 * LinkedMultiValueMap<String,String>(); //
	 * map.add("urn:ngsi-ld:Vehicle:A100",entity); // // HttpUtils ut = new
	 * HttpUtils(); // HttpUtils mock = mock(HttpUtils.class); //
	 * when(mock.getQueryParamMap(any())).thenReturn(map);
	 * //when(hu.getQueryParamMap(any())).thenReturn(map);
	 * 
	 * QueryResult result = mock(QueryResult.class);
	 * when(queryService.getData(any(), any(), any(), any(),
	 * any())).thenReturn(result); // when(HttpUtils.generateReply(any(), any(),
	 * any(),any(),any(),any())).thenReturn(responseEntity); ResultActions
	 * resultAction = mockMvc.perform(
	 * get("/ngsi-ld/v1//attributes?attrs=brandName/")
	 * .accept(AppConstants.NGB_APPLICATION_JSON)) .andExpect(status().isOk());
	 * 
	 * MvcResult mvcResult = resultAction.andReturn(); MockHttpServletResponse
	 * response = mvcResult.getResponse(); int status = response.getStatus();
	 * assertEquals(200, status); verify(queryService, times(1)).getData(any(),
	 * any(), any(), any(), any());
	 * 
	 * } catch (Exception e) { Assert.fail(e.getMessage()); } }
	 * 
	 */

	/*
	 * @Test() public void getEntityNotFoundTest() { try {
	 * 
	 * Mockito.doReturn("null").when(queryService).retrieveEntity(any(String.class),
	 * any(List.class), any(boolean.class), any(boolean.class)); //QueryResult
	 * result = new QueryResult(entities, null, ErrorType.None, -1, true);
	 * Mockito.doThrow(new
	 * ResponseException(ErrorType.NotFound)).when(queryService).getData(any(),
	 * any(), any(), any(), any(), any(), any(), any(), any(),any(),any());
	 * 
	 * mockMvc.perform(get("/ngsi-ld/v1/entities/{entityId}",
	 * "urn:ngsi-ld:Vehicle:A100").accept(AppConstants.NGB_APPLICATION_JSON))
	 * .andExpect(status().isNotFound()).andExpect(jsonPath("$.title").
	 * value("Resource not found.")) .andDo(print()); verify(queryService,
	 * times(1)).getData(any(), any(), any(), any(), any(), any(), any(),
	 * any(),any(),any(),any()); } catch (Exception e) {
	 * Assert.fail(e.getMessage()); } }
	 */
	/*
	 * @Test() public void getAttrsTest() { try { // QueryController
	 * qc=Mockito.mock(QueryController.class); Set<Object> linkHeaders = new
	 * HashSet<Object>(); Map<String, Object> entityContext = new HashMap<String,
	 * Object>(); // String jsonLdResolved = "{\r\n" +
	 * " \"http://example.org/vehicle/brandName\": // [{\r\n" // +
	 * " \"@value\": 0\r\n" + " }]\r\n" + "}";
	 * 
	 * ResponseEntity.status(HttpStatus.OK).header("location",
	 * "<<http://localhost:9090/ngsi-ld/contextes/urn:ngsi-ld:Vehicle:A100>; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json\">"
	 * ) .body(response); linkHeaders.add(
	 * "http://localhost:9090/ngsi-ld/contextes/urn:ngsi-ld:Vehicle:A100");
	 * entityContext.put("urn:ngsi-ld:Vehicle:A100",
	 * "{\"brandName\":\"http://example.org/vehicle/brandName\"}");
	 * 
	 * //
	 * when(queryService.retrieveEntity(any(),any(),any(),any())).thenReturn(entity)
	 * ; // when(HttpUtils.parseLinkHeader(any(HttpServletRequest.class), //
	 * NGSIConstants.HEADER_REL_LDCONTEXT)).thenReturn(linkHeaders);
	 * 
	 * // when(contextResolver.getContext(any())).thenReturn(entityContext); //
	 * when(contextResolver.expandPayload(any())).thenReturn(jsonLdResolved); //
	 * when(HttpUtils.generateReply(any(), any(), any(), any(), any(), //
	 * any())).thenReturn(responseEntity);
	 * 
	 * Mockito.doReturn(entity).when(queryService).retrieveEntity(any(String.class),
	 * any(List.class), any(boolean.class), any(boolean.class)); //
	 * Mockito.doReturn(entityContext).when(contextResolver).getContext(any()); //
	 * Mockito.doReturn(responseEntity).when(qc).generateReply(any(), any(), any());
	 * QueryResult result = new QueryResult(entities, null, ErrorType.None, -1,
	 * true); Mockito.doReturn(result).when(queryService).getData(any(), any(),
	 * any(), any(), any(), any(), any(), any(), any(), any(), any());
	 * mockMvc.perform(get("/ngsi-ld/v1/entities/{entityId}?attrs=brandName",
	 * "urn:ngsi-ld:Vehicle:A100")
	 * .accept(AppConstants.NGB_APPLICATION_JSON)).andExpect(status().isOk())
	 * .andExpect(jsonPath("$.id").value("urn:ngsi-ld:Vehicle:A100")).andDo(print())
	 * ; // verify(HttpUtils.parseLinkHeader(any(HttpServletRequest.class), //
	 * NGSIConstants.HEADER_REL_LDCONTEXT)); Mockito.verify(queryService,
	 * times(1)).getData(any(), any(), any(), any(), any(), any(), any(), any(),
	 * any(), any(), any()); // Mockito.verify(qc, times(1)).generateReply(any(),
	 * any(), any());
	 * 
	 * } catch (Exception e) { Assert.fail(e.getMessage()); } }
	 * 
	 */
	/*
	 * @Test() public void getAttrsFailureTest() { try { //
	 * when(queryService.retrieveEntity(any(),any(),any(),any())).thenReturn(entity)
	 * ;
	 * //Mockito.doReturn("null").when(queryService).retrieveEntity(any(String.class
	 * ), any(List.class), // any(boolean.class), any(boolean.class)); //QueryResult
	 * result = new QueryResult(entities, null, ErrorType.None, -1, true);
	 * 
	 * Mockito.doThrow(new
	 * ResponseException(ErrorType.NotFound)).when(queryService).getData(any(),
	 * any(), any(), any(), any(), any(), any(), any(), any(), any(), any());
	 * mockMvc.perform(get("/ngsi-ld/v1/entities/{entityId}?attrs=brandName",
	 * "urn:ngsi-ld:Vehicle:A100")
	 * .accept(AppConstants.NGB_APPLICATION_JSON)).andExpect(status().isNotFound())
	 * .andExpect(jsonPath("$.title").value("Resource not found.")).andDo(print());
	 * // verify(HttpUtils.parseLinkHeader(any(HttpServletRequest.class), //
	 * NGSIConstants.HEADER_REL_LDCONTEXT)); //
	 * Mockito.verify(queryService,times(1)).retrieveEntity(any(String.class), //
	 * any(List.class), any(boolean.class), any(boolean.class));
	 * 
	 * verify(queryService, times(1)).getData(any(), any(), any(), any(), any(),
	 * any(), any(), any(), any(), any(), any()); } catch (Exception e) {
	 * Assert.fail(e.getMessage()); } }
	 * 
	 * @Test() public void serviceThrowsAlreadyExistsTest() { ResponseException
	 * responseException = new ResponseException(ErrorType.BadRequestData);
	 * 
	 * try {
	 * Mockito.doThrow(responseException).when(queryService).retrieveEntity(any(
	 * String.class), any(List.class), any(boolean.class), any(boolean.class));
	 * 
	 * Mockito.doThrow(new
	 * ResponseException(ErrorType.BadRequestData)).when(queryService).getData(any()
	 * , any(), any(), any(), any(), any(), any(), any(), any(), any(), any());
	 * mockMvc.perform(get("/ngsi-ld/v1/entities/{entityId}",
	 * "urn:ngsi-ld:Vehicle:A100").accept(AppConstants.NGB_APPLICATION_JSON))
	 * .andExpect(status().isBadRequest()).andExpect(jsonPath("$.title").
	 * value("Bad Request Data.")) .andDo(print()); verify(queryService,
	 * times(1)).getData(any(), any(), any(), any(), any(), any(), any(), any(),
	 * any(), any(), any()); } catch (Exception e) { Assert.fail(e.getMessage()); }
	 * 
	 * }
	 * 
	 * // @Test TODO : Failing because of request URI check in controller class.
	 * public void getAttribOfEntityTest() { try { Set<Object> linkHeaders = new
	 * HashSet<Object>(); Map<String, Object> entityContext = new HashMap<String,
	 * Object>(); linkHeaders.add(
	 * "http://localhost:9090/ngsi-ld/contextes/urn:ngsi-ld:Vehicle:A100");
	 * entityContext.put("urn:ngsi-ld:Vehicle:A100",
	 * "{\"brandName\":\"http://example.org/vehicle/brandName\"}"); String
	 * resolveQueryLdContext = "http://example.org/vehicle/brandName"; String
	 * response = "{\r\n" + "	\"id\": \"urn:ngsi-ld:Vehicle:A100\",\r\n" +
	 * "	\"type\": \"Vehicle\",\r\n" + "	\"brandName\": {\r\n" +
	 * "		\"type\": \"Property\",\r\n" + "		\"value\": \"Mercedes\"\r\n"
	 * + "	}\r\n" + "}\r\n" + "\r\n" + "";
	 * ResponseEntity.status(HttpStatus.OK).header("location",
	 * "<<http://localhost:9090/ngsi-ld/contextes/urn:ngsi-ld:Vehicle:A100>; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json\">"
	 * ) .body(response);
	 * 
	 * HttpServletRequest request = Mockito.mock(HttpServletRequest.class); String
	 * res = "/"; // when(HttpUtils.parseLinkHeader(any(HttpServletRequest.class),
	 * // NGSIConstants.HEADER_REL_LDCONTEXT)).thenReturn(linkHeaders); //
	 * when(contextResolver.getContext(any())).thenReturn(entityContext); //
	 * when(paramsResolver.resolveQueryLdContext(any(), //
	 * any())).thenReturn(resolveQueryLdContext); //
	 * when(queryService.retrieveEntity(any(),any(),any(),any())).thenReturn(entity)
	 * ; // when(HttpUtils.generateReply(any(), any(), any(), any(), any(), //
	 * any())).thenReturn(responseEntity);
	 * 
	 * // Mockito.doReturn(entityContext).when(contextResolver).getContext(any());
	 * Mockito.doReturn(resolveQueryLdContext).when(paramsResolver).expandAttribute(
	 * any(), any());
	 * Mockito.doReturn(entity).when(queryService).retrieveEntity(any(String.class),
	 * any(List.class), any(boolean.class), any(boolean.class)); //
	 * Mockito.doReturn(res).when(request).getRequestURI();
	 * Mockito.when(request.getRequestURI()).thenReturn(res); //
	 * Mockito.doReturn(responseEntity).when(qc).generateReply(any(), any());
	 * 
	 * mockMvc.perform( get("/ngsi-ld/v1/entities/{entityId}/attrs/{attrsId}",
	 * "urn:ngsi-ld:Vehicle:A100", "brandName")
	 * .accept(AppConstants.NGB_APPLICATION_JSON))
	 * .andExpect(status().isOk()).andExpect(jsonPath("$.id").value(
	 * "urn:ngsi-ld:Vehicle:A100")) .andDo(print());
	 * 
	 * // verify(HttpUtils.parseLinkHeader(any(HttpServletRequest.class), //
	 * NGSIConstants.HEADER_REL_LDCONTEXT)); // verify(contextResolver,
	 * times(1)).getContext(any()); verify(paramsResolver,
	 * times(1)).expandAttribute(any(), any()); verify(queryService,
	 * times(1)).retrieveEntity(any(), any(), any(), any()); } catch (Exception e) {
	 * Assert.fail(e.getMessage()); } }
	 * 
	 * // @Test public void badRequestForAttribOfEntityTest() { try { Set<Object>
	 * linkHeaders = new HashSet<Object>(); Map<String, Object> entityContext = new
	 * HashMap<String, Object>(); linkHeaders.add(
	 * "http://localhost:9090/ngsi-ld/contextes/urn:ngsi-ld:Vehicle:A100");
	 * entityContext.put("urn:ngsi-ld:Vehicle:A100",
	 * "{\"brandName\":\"http://example.org/vehicle/brandName\"}"); //String
	 * resolveQueryLdContext = "http://example.org/vehicle/brandName";
	 * 
	 * // when(HttpUtils.parseLinkHeader(any(HttpServletRequest.class), //
	 * NGSIConstants.HEADER_REL_LDCONTEXT)).thenReturn(linkHeaders); //
	 * when(contextResolver.getContext(any())).thenReturn(entityContext); //
	 * when(paramsResolver.resolveQueryLdContext(any(), //
	 * any())).thenReturn(resolveQueryLdContext); //
	 * when(queryService.retrieveEntity(any(),any(),any(),any())).thenThrow(new //
	 * ResponseException(ErrorType.BadRequestData));
	 * 
	 * // Mockito.doReturn(entityContext).when(contextResolver).getContext(any());
	 * // Mockito.doReturn(resolveQueryLdContext).when(paramsResolver).
	 * resolveQueryLdContext(any(), any()); Mockito.doThrow(new
	 * ResponseException(ErrorType.BadRequestData)).when(queryService)
	 * .retrieveEntity(any(String.class), any(List.class), any(boolean.class),
	 * any(boolean.class));
	 * 
	 * mockMvc.perform(get("/ngsi-ld/v1/entities/{entityId}/attrs/{attrsId}",
	 * "urn%3Angsi-ld%3AVehicle%3AA100",
	 * "brandName").accept(AppConstants.NGB_APPLICATION_JSON)).andExpect(status().
	 * isBadRequest())
	 * .andExpect(jsonPath("$.title").value("Bad Request Data.")).andDo(print());
	 * 
	 * // verify(HttpUtils.parseLinkHeader(any(HttpServletRequest.class), //
	 * NGSIConstants.HEADER_REL_LDCONTEXT)); //
	 * verify(contextResolver,times(1)).getContext(any()); //
	 * verify(paramsResolver,times(1)).resolveQueryLdContext(any(), any()); //
	 * verify(queryService,times(1)).retrieveEntity(any(String.class), //
	 * any(List.class), any(boolean.class), any(boolean.class)); } catch (Exception
	 * e) { Assert.fail(e.getMessage()); } }
	 * 
	 * // @Test TODO : Failing because of request URI check in controller class.
	 * public void exception500ForAttribOfEntityTest() { try { //
	 * when(queryService.retrieveEntity(any(),any(),any(),any())).thenThrow(new //
	 * ResponseException(ErrorType.InternalError));
	 * 
	 * Mockito.doThrow(new
	 * ResponseException(ErrorType.InternalError)).when(queryService)
	 * .retrieveEntity(any(String.class), any(List.class), any(boolean.class),
	 * any(boolean.class));
	 * 
	 * mockMvc.perform( get("/ngsi-ld/v1/entities/{entityId}/attrs/{attrsId}/",
	 * "urn:ngsi-ld:Vehicle:A100", "brandName")
	 * .accept(AppConstants.NGB_APPLICATION_JSON))
	 * .andExpect(status().isInternalServerError()).andExpect(jsonPath("$.title").
	 * value("Internal error.")) .andDo(print());
	 * 
	 * // verify(HttpUtils.parseLinkHeader(any(HttpServletRequest.class), //
	 * NGSIConstants.HEADER_REL_LDCONTEXT)); //
	 * verify(contextResolver,times(1)).getContext(any()); //
	 * verify(paramsResolver,times(1)).resolveQueryLdContext(any(), any()); //
	 * verify(queryService,times(1)).retrieveEntity(any(),any(), any(),any()); }
	 * catch (Exception e) { Assert.fail(e.getMessage()); } }
	 * 
	 * @Test public void getAllEntityBadRequestTest() { try {
	 * mockMvc.perform(get("/ngsi-ld/v1/entities/").accept(AppConstants.
	 * NGB_APPLICATION_JSON))
	 * .andExpect(status().isBadRequest()).andExpect(jsonPath("$.title").
	 * value("Bad Request Data.")) .andDo(print()); } catch (Exception e) {
	 * Assert.fail(e.getMessage()); } }
	 * 
	 */
	/*
	 * @Test public void getAllEntitySuccessTest() { try { //String
	 * resolveQueryLdContext = "http://example.org/vehicle/brandName"; Set<Object>
	 * linkHeaders = new HashSet<Object>(); linkHeaders.add(
	 * "http://localhost:9090/ngsi-ld/contextes/urn:ngsi-ld:Vehicle:A100");
	 * ResponseEntity<String> responseEntity=
	 * ResponseEntity.status(HttpStatus.OK).header("location",
	 * "<<http://localhost:9090/ngsi-ld/contextes/urn:ngsi-ld:Vehicle:A100>; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json\">"
	 * ) .body(response);
	 * 
	 * QueryResult result = new QueryResult(entities, null, ErrorType.None, -1,
	 * true); //
	 * when(HttpUtils.parseLinkHeader(any(HttpServletRequest.class),NGSIConstants.
	 * HEADER_REL_LDCONTEXT)).thenReturn(linkHeaders); //
	 * when(paramsResolver.resolveQueryLdContext(any(),any())).thenReturn(
	 * resolveQueryLdContext); //
	 * when(paramsResolver.getQueryParamsFromUriQuery(any(),any())).thenReturn(new
	 * QueryParams().withAttrs("brandName")); //
	 * when(queryService.getData(any(),any(), any(), any(), null, null, //
	 * null)).thenReturn(result);
	 * 
	 * // Mockito.doReturn(resolveQueryLdContext).when(paramsResolver).
	 * esolveQueryLdContext(any(), any()); // Mockito.doReturn(new
	 * QueryParams().withAttrs("brandName")).when(paramsResolver).
	 * getQueryParamsFromUriQuery(any(), any());
	 * 
	 * // qc = new QueryController(); // QueryController mock =
	 * Mockito.mock(QueryController.class);
	 * Mockito.doReturn(result).when(queryService).getData(any(), any(), any(),
	 * any(), any()); //
	 * Mockito.doReturn(responseEntity).when(qc).generateReply(any(), any()); //
	 * Mockito.doReturn(responseEntity).when(mock).getAllAttributes(any(), any());
	 * // when(mock.getAllAttributes(any(), any())).thenReturn(responseEntity);
	 * mockMvc.perform(get("/ngsi-ld/v1/entities/?attrs=brandName").accept(
	 * AppConstants.NGB_APPLICATION_JSON))
	 * .andExpect(status().isOk()).andDo(print()); //
	 * verify(HttpUtils.parseLinkHeader(any(HttpServletRequest.class),NGSIConstants.
	 * HEADER_REL_LDCONTEXT)); //
	 * verify(paramsResolver,times(1)).resolveQueryLdContext(any(), any()); // //
	 * verify(paramsResolver, times(1)).getQueryParamsFromUriQuery(any(), any()); //
	 * // verify(paramsResolver, times(1)).getQueryParamsFromUriQuery(any(), any());
	 * 
	 * verify(queryService, times(1)).getData(any(), any(), any(), any(), any()); }
	 * catch (Exception e) { Assert.fail(e.getMessage()); } }
	 */
}
