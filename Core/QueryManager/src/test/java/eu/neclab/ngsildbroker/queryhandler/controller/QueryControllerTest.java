package eu.neclab.ngsildbroker.queryhandler.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.modules.junit4.PowerMockRunnerDelegate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.Entity;
import eu.neclab.ngsildbroker.commons.datatypes.QueryResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.ldcontext.ContextResolverBasic;
import eu.neclab.ngsildbroker.commons.ngsiqueries.ParamsResolver;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;
import eu.neclab.ngsildbroker.queryhandler.services.QueryService;

@SpringBootTest(properties = { "spring.main.allow-bean-definition-overriding=true" })
@RunWith(PowerMockRunner.class)
//@WebMvcTest(secure = false) 
@AutoConfigureMockMvc(secure = false)
@PowerMockRunnerDelegate(SpringRunner.class)
@PowerMockIgnore({ "javax.management.*", "com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*", "org.w3c.*",
		"com.sun.org.apache.xalan.*", "javax.activation.*", "javax.net.*", "javax.security.*" })
public class QueryControllerTest {

	@Autowired
	private MockMvc mockMvc;
	@MockBean
	private QueryService queryService;
	@Autowired
	ContextResolverBasic contextResolver;
	@Autowired
	ParamsResolver paramsResolver;
//	@InjectMocks
//	@Spy
//	QueryController qc;

	private String entity;
	private String response = "";
	private String linkHeader = "<<http://localhost:9090/ngsi-ld/contextes/urn:ngsi-ld:Vehicle:A100>; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json\">";
	private List<String> entities;

	@Before
	public void setup() {
		// PowerMockito.mockStatic(HttpUtils.class);
		MockitoAnnotations.initMocks(this);
//		 this.mockMvc =  MockMvcBuilders.standaloneSetup(qc).build();
		//@formatter:off
    	entity="[{\r\n" + 
    			"	\"http://example.org/vehicle/brandName\": [{\r\n" + 
    			"		\"@type\": [\"https://uri.etsi.org/ngsi-ld/Property\"],\r\n" + 
    			"		\"https://uri.etsi.org/ngsi-ld/hasValue\": [{\r\n" + 
    			"			\"@value\": \"Mercedes\"\r\n" + 
    			"		}]\r\n" + 
    			"	}],\r\n" + 
    			"	\"https://uri.etsi.org/ngsi-ld/createdAt\": [{\r\n" + 
    			"		\"@type\": \"https://uri.etsi.org/ngsi-ld/DateTime\",\r\n" + 
    			"		\"@value\": \"2017-07-29T12:00:04Z\"\r\n" + 
    			"	}],\r\n" + 
    			"	\"@id\": \"urn:ngsi-ld:Vehicle:A100\",\r\n" + 
    			"	\"http://example.org/common/isParked\": [{\r\n" + 
    			"		\"https://uri.etsi.org/ngsi-ld/hasObject\": [{\r\n" + 
    			"			\"@id\": \"urn:ngsi-ld:OffStreetParking:Downtown1\"\r\n" + 
    			"		}],\r\n" + 
    			"		\"https://uri.etsi.org/ngsi-ld/observedAt\": [{\r\n" + 
    			"			\"@type\": \"https://uri.etsi.org/ngsi-ld/DateTime\",\r\n" + 
    			"			\"@value\": \"2017-07-29T12:00:04Z\"\r\n" + 
    			"		}],\r\n" + 
    			"		\"http://example.org/common/providedBy\": [{\r\n" + 
    			"			\"https://uri.etsi.org/ngsi-ld/hasObject\": [{\r\n" + 
    			"				\"@id\": \"urn:ngsi-ld:Person:Bob\"\r\n" + 
    			"			}],\r\n" + 
    			"			\"@type\": [\"https://uri.etsi.org/ngsi-ld/Relationship\"]\r\n" + 
    			"		}],\r\n" + 
    			"		\"@type\": [\"https://uri.etsi.org/ngsi-ld/Relationship\"]\r\n" + 
    			"	}],\r\n" + 
    			"	\"https://uri.etsi.org/ngsi-ld/location\": [{\r\n" + 
    			"		\"@type\": [\"https://uri.etsi.org/ngsi-ld/GeoProperty\"],\r\n" + 
    			"		\"https://uri.etsi.org/ngsi-ld/hasValue\": [{\r\n" + 
    			"			\"@value\": \"{ \\\"type\\\":\\\"Point\\\", \\\"coordinates\\\":[ -8.5, 41.2 ] }\"\r\n" + 
    			"		}]\r\n" + 
    			"	}],\r\n" + 
    			"	\"http://example.org/vehicle/speed\": [{\r\n" + 
    			"		\"@type\": [\"https://uri.etsi.org/ngsi-ld/Property\"],\r\n" + 
    			"		\"https://uri.etsi.org/ngsi-ld/hasValue\": [{\r\n" + 
    			"			\"@value\": 80\r\n" + 
    			"		}]\r\n" + 
    			"	}],\r\n" + 
    			"	\"@type\": [\"http://example.org/vehicle/Vehicle\"]\r\n" + 
    			"}]";
    	
    	response="{\r\n" + 
    			"	\"id\": \"urn:ngsi-ld:Vehicle:A100\",\r\n" + 
    			"	\"type\": \"Vehicle\",\r\n" + 
    			"	\"brandName\": {\r\n" + 
    			"		\"type\": \"Property\",\r\n" + 
    			"		\"value\": \"Mercedes\"\r\n" + 
    			"	},\r\n" + 
    			"	\"isParked\": {\r\n" + 
    			"		\"type\": \"Relationship\",\r\n" + 
    			"		\"object\": \"urn:ngsi-ld:OffStreetParking:Downtown1\",\r\n" + 
    			"		\"observedAt\": \"2017-07-29T12:00:04Z\",\r\n" + 
    			"		\"providedBy\": {\r\n" + 
    			"			\"type\": \"Relationship\",\r\n" + 
    			"			\"object\": \"urn:ngsi-ld:Person:Bob\"\r\n" + 
    			"		}\r\n" + 
    			"	},\r\n" + 
    			"	\"speed\": {\r\n" + 
    			"		\"type\": \"Property\",\r\n" + 
    			"		\"value\": 80\r\n" + 
    			"	},\r\n" + 
    			"	\"createdAt\": \"2017-07-29T12:00:04Z\",\r\n" + 
    			"	\"location\": {\r\n" + 
    			"		\"type\": \"GeoProperty\",\r\n" + 
    			"		\"value\": { \"type\":\"Point\", \"coordinates\":[ -8.5, 41.2 ] }\r\n" + 
    			"	}\r\n" + 
    			"}";
    	
    	entities=new ArrayList<String>( 
                Arrays.asList("{\r\n" + 
    			"  \"http://example.org/vehicle/brandName\" : [ {\r\n" + 
    			"    \"@value\" : \"Volvo\"\r\n" + 
    			"  } ],\r\n" + 
    			"  \"@id\" : \"urn:ngsi-ld:Vehicle:A100\",\r\n" + 
    			"  \"http://example.org/vehicle/speed\" : [ {\r\n" + 
    			"    \"https://uri.etsi.org/ngsi-ld/instanceId\" : [ {\r\n" + 
    			"      \"@value\" : \"be664aaf-a7af-4a99-bebc-e89528238abf\"\r\n" + 
    			"    } ],\r\n" + 
    			"    \"https://uri.etsi.org/ngsi-ld/observedAt\" : [ {\r\n" + 
    			"      \"@value\" : \"2018-06-01T12:03:00Z\",\r\n" + 
    			"      \"@type\" : \"https://uri.etsi.org/ngsi-ld/DateTime\"\r\n" + 
    			"    } ],\r\n" + 
    			"    \"@type\" : [ \"https://uri.etsi.org/ngsi-ld/Property\" ],\r\n" + 
    			"    \"https://uri.etsi.org/ngsi-ld/hasValue\" : [ {\r\n" + 
    			"      \"@value\" : \"120\"\r\n" + 
    			"    } ]\r\n" + 
    			"  }, {\r\n" + 
    			"    \"https://uri.etsi.org/ngsi-ld/instanceId\" : [ {\r\n" + 
    			"      \"@value\" : \"d3ac28df-977f-4151-a432-dc088f7400d7\"\r\n" + 
    			"    } ],\r\n" + 
    			"    \"https://uri.etsi.org/ngsi-ld/observedAt\" : [ {\r\n" + 
    			"      \"@value\" : \"2018-08-01T12:05:00Z\",\r\n" + 
    			"      \"@type\" : \"https://uri.etsi.org/ngsi-ld/DateTime\"\r\n" + 
    			"    } ],\r\n" + 
    			"    \"@type\" : [ \"https://uri.etsi.org/ngsi-ld/Property\" ],\r\n" + 
    			"    \"https://uri.etsi.org/ngsi-ld/hasValue\" : [ {\r\n" + 
    			"      \"@value\" : \"80\"\r\n" + 
    			"    } ]\r\n" + 
    			"  } ],\r\n" + 
    			"  \"@type\" : [ \"http://example.org/vehicle/Vehicle\" ]\r\n" + 
    			"}"));
    	//@formatter:on
	}

	@After
	public void tearDown() {
		entity = null;
		response = null;
	}

	@Test
	public void getEntityTest() throws Exception {
		try {

			ResponseEntity<Object> responseEntity = ResponseEntity.status(HttpStatus.OK).header("location",
					"<<http://localhost:9090/ngsi-ld/contextes/urn:ngsi-ld:Vehicle:A100>; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json\">")
					.body(response);

			Mockito.doReturn(entity).when(queryService).retrieveEntity(any(String.class), any(List.class),
					any(boolean.class), any(boolean.class));
//			Mockito.doReturn(responseEntity).when(qc).generateReply(any(), any(), any());
			QueryResult result = new QueryResult(entities, null, ErrorType.None, -1, true);
			Mockito.doReturn(result).when(queryService).getData(any(), any(), any(), any(), any(), any(), any());
			mockMvc.perform(get("/ngsi-ld/v1/entities/{entityId}", "urn:ngsi-ld:Vehicle:A100").accept(AppConstants.NGB_APPLICATION_JSON))
					.andExpect(status().isOk()).andExpect(jsonPath("$.id").value("urn:ngsi-ld:Vehicle:A100"));
//					.andExpect(redirectedUrl(linkHeader)).andDo(print());
			verify(queryService, times(1)).getData(any(), any(), any(), any(), any(), any(), any());
//			verify(qc, times(1)).generateReply(any(), any(), any());

		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}

	}

	@Test()
	public void getEntityNotFoundTest() {
		try {

			Mockito.doReturn("null").when(queryService).retrieveEntity(any(String.class), any(List.class),
					any(boolean.class), any(boolean.class));
			//QueryResult result = new QueryResult(entities, null, ErrorType.None, -1, true);
			Mockito.doThrow(new ResponseException(ErrorType.NotFound)).when(queryService).getData(any(), any(), any(), any(), any(), any(), any());

			mockMvc.perform(get("/ngsi-ld/v1/entities/{entityId}", "urn:ngsi-ld:Vehicle:A100").accept(AppConstants.NGB_APPLICATION_JSON))
					.andExpect(status().isNotFound()).andExpect(jsonPath("$.title").value("Resource not found."))
					.andDo(print());
			verify(queryService, times(1)).getData(any(), any(), any(), any(), any(), any(), any());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test()
	public void getAttrsTest() {
		try {
			//QueryController qc=Mockito.mock(QueryController.class);
			Set<Object> linkHeaders = new HashSet<Object>();
			Map<String, Object> entityContext = new HashMap<String, Object>();
			String jsonLdResolved = "{\r\n" + "	\"http://example.org/vehicle/brandName\": [{\r\n"
					+ "		\"@value\": 0\r\n" + "	}]\r\n" + "}";

			ResponseEntity<Object> responseEntity = ResponseEntity.status(HttpStatus.OK).header("location",
					"<<http://localhost:9090/ngsi-ld/contextes/urn:ngsi-ld:Vehicle:A100>; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json\">")
					.body(response);
			linkHeaders.add("http://localhost:9090/ngsi-ld/contextes/urn:ngsi-ld:Vehicle:A100");
			entityContext.put("urn:ngsi-ld:Vehicle:A100", "{\"brandName\":\"http://example.org/vehicle/brandName\"}");

			// when(queryService.retrieveEntity(any(),any(),any(),any())).thenReturn(entity);
			// when(HttpUtils.parseLinkHeader(any(HttpServletRequest.class),
			// NGSIConstants.HEADER_REL_LDCONTEXT)).thenReturn(linkHeaders);

			// when(contextResolver.getContext(any())).thenReturn(entityContext);
			// when(contextResolver.expandPayload(any())).thenReturn(jsonLdResolved);
			// when(HttpUtils.generateReply(any(), any(), any(), any(), any(),
			// any())).thenReturn(responseEntity);

			Mockito.doReturn(entity).when(queryService).retrieveEntity(any(String.class), any(List.class),
					any(boolean.class), any(boolean.class));
//			Mockito.doReturn(entityContext).when(contextResolver).getContext(any());
//			Mockito.doReturn(responseEntity).when(qc).generateReply(any(), any(), any());
			QueryResult result = new QueryResult(entities, null, ErrorType.None, -1, true);
			Mockito.doReturn(result).when(queryService).getData(any(), any(), any(), any(), any(), any(), any());
			mockMvc.perform(get("/ngsi-ld/v1/entities/{entityId}?attrs=brandName", "urn:ngsi-ld:Vehicle:A100")
					.accept(AppConstants.NGB_APPLICATION_JSON)).andExpect(status().isOk())
					.andExpect(jsonPath("$.id").value("urn:ngsi-ld:Vehicle:A100")).andDo(print());
			// verify(HttpUtils.parseLinkHeader(any(HttpServletRequest.class),
			// NGSIConstants.HEADER_REL_LDCONTEXT));

			Mockito.verify(queryService, times(1)).getData(any(), any(), any(), any(), any(), any(), any());
//			Mockito.verify(qc, times(1)).generateReply(any(), any(), any());

		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test()
	public void getAttrsFailureTest() {
		try {
			// when(queryService.retrieveEntity(any(),any(),any(),any())).thenReturn(entity);
			//Mockito.doReturn("null").when(queryService).retrieveEntity(any(String.class), any(List.class),
				//	any(boolean.class), any(boolean.class));
			//QueryResult result = new QueryResult(entities, null, ErrorType.None, -1, true);
			Mockito.doThrow(new ResponseException(ErrorType.NotFound)).when(queryService).getData(any(), any(), any(), any(), any(), any(), any());
			mockMvc.perform(get("/ngsi-ld/v1/entities/{entityId}?attrs=brandName", "urn:ngsi-ld:Vehicle:A100")
					.accept(AppConstants.NGB_APPLICATION_JSON)).andExpect(status().isNotFound())
					.andExpect(jsonPath("$.title").value("Resource not found.")).andDo(print());
			// verify(HttpUtils.parseLinkHeader(any(HttpServletRequest.class),
			// NGSIConstants.HEADER_REL_LDCONTEXT));
			// Mockito.verify(queryService,times(1)).retrieveEntity(any(String.class),
			// any(List.class), any(boolean.class), any(boolean.class));
			verify(queryService, times(1)).getData(any(), any(), any(), any(), any(), any(), any());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test()
	public void serviceThrowsAlreadyExistsTest() {
		ResponseException responseException = new ResponseException(ErrorType.BadRequestData);

		try {
			Mockito.doThrow(responseException).when(queryService).retrieveEntity(any(String.class), any(List.class),
					any(boolean.class), any(boolean.class));
			Mockito.doThrow(new ResponseException(ErrorType.BadRequestData)).when(queryService).getData(any(), any(), any(), any(), any(), any(), any());
			mockMvc.perform(get("/ngsi-ld/v1/entities/{entityId}", "urn:ngsi-ld:Vehicle:A100").accept(AppConstants.NGB_APPLICATION_JSON))
					.andExpect(status().isBadRequest()).andExpect(jsonPath("$.title").value("Bad Request Data."))
					.andDo(print());
			verify(queryService, times(1)).getData(any(), any(), any(), any(), any(), any(), any());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}

	}

	//@Test  TODO : Failing because of request URI check in controller class.
	public void getAttribOfEntityTest() {
		try {
			Set<Object> linkHeaders = new HashSet<Object>();
			Map<String, Object> entityContext = new HashMap<String, Object>();
			linkHeaders.add("http://localhost:9090/ngsi-ld/contextes/urn:ngsi-ld:Vehicle:A100");
			entityContext.put("urn:ngsi-ld:Vehicle:A100", "{\"brandName\":\"http://example.org/vehicle/brandName\"}");
			String resolveQueryLdContext = "http://example.org/vehicle/brandName";
			String response = "{\r\n" + "	\"id\": \"urn:ngsi-ld:Vehicle:A100\",\r\n" + "	\"type\": \"Vehicle\",\r\n"
					+ "	\"brandName\": {\r\n" + "		\"type\": \"Property\",\r\n"
					+ "		\"value\": \"Mercedes\"\r\n" + "	}\r\n" + "}\r\n" + "\r\n" + "";
			ResponseEntity<Object> responseEntity = ResponseEntity.status(HttpStatus.OK).header("location",
					"<<http://localhost:9090/ngsi-ld/contextes/urn:ngsi-ld:Vehicle:A100>; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json\">")
					.body(response);

			HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
			String res = "/";
			// when(HttpUtils.parseLinkHeader(any(HttpServletRequest.class),
			// NGSIConstants.HEADER_REL_LDCONTEXT)).thenReturn(linkHeaders);
			// when(contextResolver.getContext(any())).thenReturn(entityContext);
			// when(paramsResolver.resolveQueryLdContext(any(),
			// any())).thenReturn(resolveQueryLdContext);
			// when(queryService.retrieveEntity(any(),any(),any(),any())).thenReturn(entity);
			// when(HttpUtils.generateReply(any(), any(), any(), any(), any(),
			// any())).thenReturn(responseEntity);

//			Mockito.doReturn(entityContext).when(contextResolver).getContext(any());
			Mockito.doReturn(resolveQueryLdContext).when(paramsResolver).expandAttribute(any(), any());
			Mockito.doReturn(entity).when(queryService).retrieveEntity(any(String.class), any(List.class),
					any(boolean.class), any(boolean.class));
			// Mockito.doReturn(res).when(request).getRequestURI();
			Mockito.when(request.getRequestURI()).thenReturn(res);
//			Mockito.doReturn(responseEntity).when(qc).generateReply(any(), any());

			mockMvc.perform(get("/ngsi-ld/v1/entities/{entityId}/attrs/{attrsId}", "urn:ngsi-ld:Vehicle:A100", "brandName")
					.accept(AppConstants.NGB_APPLICATION_JSON)).andExpect(status().isOk())
					.andExpect(jsonPath("$.id").value("urn:ngsi-ld:Vehicle:A100")).andDo(print());

			// verify(HttpUtils.parseLinkHeader(any(HttpServletRequest.class),
			// NGSIConstants.HEADER_REL_LDCONTEXT));
//			verify(contextResolver, times(1)).getContext(any());
			verify(paramsResolver, times(1)).expandAttribute(any(), any());
			verify(queryService, times(1)).retrieveEntity(any(), any(), any(), any());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	//@Test
	public void badRequestForAttribOfEntityTest() {
		try {
			Set<Object> linkHeaders = new HashSet<Object>();
			Map<String, Object> entityContext = new HashMap<String, Object>();
			linkHeaders.add("http://localhost:9090/ngsi-ld/contextes/urn:ngsi-ld:Vehicle:A100");
			entityContext.put("urn:ngsi-ld:Vehicle:A100", "{\"brandName\":\"http://example.org/vehicle/brandName\"}");
			String resolveQueryLdContext = "http://example.org/vehicle/brandName";

			// when(HttpUtils.parseLinkHeader(any(HttpServletRequest.class),
			// NGSIConstants.HEADER_REL_LDCONTEXT)).thenReturn(linkHeaders);
			// when(contextResolver.getContext(any())).thenReturn(entityContext);
			// when(paramsResolver.resolveQueryLdContext(any(),
			// any())).thenReturn(resolveQueryLdContext);
			// when(queryService.retrieveEntity(any(),any(),any(),any())).thenThrow(new
			// ResponseException(ErrorType.BadRequestData));

//			Mockito.doReturn(entityContext).when(contextResolver).getContext(any());
//			Mockito.doReturn(resolveQueryLdContext).when(paramsResolver).resolveQueryLdContext(any(), any());
			Mockito.doThrow(new ResponseException(ErrorType.BadRequestData)).when(queryService)
					.retrieveEntity(any(String.class), any(List.class), any(boolean.class), any(boolean.class));

			mockMvc.perform(get("/ngsi-ld/v1/entities/{entityId}/attrs/{attrsId}", "urn%3Angsi-ld%3AVehicle%3AA100", "brandName")
					.accept(AppConstants.NGB_APPLICATION_JSON)).andExpect(status().isBadRequest())
					.andExpect(jsonPath("$.title").value("Bad Request Data.")).andDo(print());

			// verify(HttpUtils.parseLinkHeader(any(HttpServletRequest.class),
			// NGSIConstants.HEADER_REL_LDCONTEXT));
			// verify(contextResolver,times(1)).getContext(any());
			// verify(paramsResolver,times(1)).resolveQueryLdContext(any(), any());
			// verify(queryService,times(1)).retrieveEntity(any(String.class),
			// any(List.class), any(boolean.class), any(boolean.class));
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	// @Test TODO : Failing because of request URI check in controller class.
	public void exception500ForAttribOfEntityTest() {
		try {
			// when(queryService.retrieveEntity(any(),any(),any(),any())).thenThrow(new
			// ResponseException(ErrorType.InternalError));

			Mockito.doThrow(new ResponseException(ErrorType.InternalError)).when(queryService)
					.retrieveEntity(any(String.class), any(List.class), any(boolean.class), any(boolean.class));

			mockMvc.perform(get("/ngsi-ld/v1/entities/{entityId}/attrs/{attrsId}/", "urn:ngsi-ld:Vehicle:A100", "brandName")
					.accept(AppConstants.NGB_APPLICATION_JSON)).andExpect(status().isInternalServerError())
					.andExpect(jsonPath("$.title").value("Internal error.")).andDo(print());

			// verify(HttpUtils.parseLinkHeader(any(HttpServletRequest.class),
			// NGSIConstants.HEADER_REL_LDCONTEXT));
			// verify(contextResolver,times(1)).getContext(any());
			// verify(paramsResolver,times(1)).resolveQueryLdContext(any(), any());
			// verify(queryService,times(1)).retrieveEntity(any(),any(), any(),any());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void getAllEntityBadRequestTest() {
		try {
			mockMvc.perform(get("/ngsi-ld/v1/entities/").accept(AppConstants.NGB_APPLICATION_JSON)).andExpect(status().isBadRequest())
					.andExpect(jsonPath("$.title").value("Bad Request Data.")).andDo(print());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void getAllEntitySuccessTest() {
		try {
			String resolveQueryLdContext = "http://example.org/vehicle/brandName";
			Set<Object> linkHeaders = new HashSet<Object>();
			linkHeaders.add("http://localhost:9090/ngsi-ld/contextes/urn:ngsi-ld:Vehicle:A100");
			ResponseEntity<Object> responseEntity = ResponseEntity.status(HttpStatus.OK).header("location",
					"<<http://localhost:9090/ngsi-ld/contextes/urn:ngsi-ld:Vehicle:A100>; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json\">")
					.body(response);

			QueryResult result = new QueryResult(entities, null, ErrorType.None, -1, true);
			// when(HttpUtils.parseLinkHeader(any(HttpServletRequest.class),
			// NGSIConstants.HEADER_REL_LDCONTEXT)).thenReturn(linkHeaders);
			// when(paramsResolver.resolveQueryLdContext(any(),
			// any())).thenReturn(resolveQueryLdContext);
			// when(paramsResolver.getQueryParamsFromUriQuery(any(),any())).thenReturn(new
			// QueryParams().withAttrs("brandName"));
			// when(queryService.getData(any(), any(), any(), null, null,
			// null)).thenReturn(result);

//			Mockito.doReturn(resolveQueryLdContext).when(paramsResolver).resolveQueryLdContext(any(), any());
//			Mockito.doReturn(new QueryParams().withAttrs("brandName")).when(paramsResolver)
//					.getQueryParamsFromUriQuery(any(), any());
			Mockito.doReturn(result).when(queryService).getData(any(), any(), any(), any(), any(), any(), any());
//			Mockito.doReturn(responseEntity).when(qc).generateReply(any(), any());

			mockMvc.perform(get("/ngsi-ld/v1/entities/?attrs=brandName").accept(AppConstants.NGB_APPLICATION_JSON))
					.andExpect(status().isOk()).andDo(print());
			// verify(HttpUtils.parseLinkHeader(any(HttpServletRequest.class),
			// NGSIConstants.HEADER_REL_LDCONTEXT));
			// verify(paramsResolver,times(1)).resolveQueryLdContext(any(), any());
//			verify(paramsResolver, times(1)).getQueryParamsFromUriQuery(any(), any());
//			verify(paramsResolver, times(1)).getQueryParamsFromUriQuery(any(), any());
			verify(queryService, times(1)).getData(any(), any(), any(), any(), any(), any(), any());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

}