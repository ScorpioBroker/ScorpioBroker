package eu.neclab.ngsildbroker.historymanager.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.web.client.RestTemplate;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.datatypes.QueryResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.ldcontext.ContextResolverBasic;
import eu.neclab.ngsildbroker.commons.ngsiqueries.ParamsResolver;
import eu.neclab.ngsildbroker.commons.stream.service.KafkaOps;
import eu.neclab.ngsildbroker.historymanager.repository.HistoryDAO;
import eu.neclab.ngsildbroker.historymanager.service.HistoryService;
import eu.neclab.ngsildbroker.historymanager.utils.Validator;

//@SpringBootTest(properties= {"spring.main.allow-bean-definition-overriding=true"})
//@RunWith(SpringRunner.class)
///@WebMvcTest(HistoryController.class)
//@AutoConfigureMockMvc//(secure = false)
@RunWith(SpringRunner.class)
@SpringBootTest(properties= {"spring.main.allow-bean-definition-overriding=true"})
@AutoConfigureMockMvc
public class HistoryControllerTest {
	@Autowired
	private MockMvc mockMvc;

	@Mock
	private ParamsResolver paramResolver;

	@MockBean
	private HistoryService historyService;

	@MockBean
	private Validator validate;

	@Mock
	HistoryDAO historyDAO;

	@Autowired
	ContextResolverBasic contextResolver;
	
	@Value("${atcontext.url}")
	String atContextServerUrl;

	private List<String> entities;
	private MockRestServiceServer mockServer;
	private String response = "";
	private String temporalPayload;
	private String attrPayload;
	private URI uri;

	@Before
	public void setup() throws Exception {

		uri = new URI(AppConstants.HISTORY_URL + "urn:ngsi-ld:testunit:151");

		MockitoAnnotations.initMocks(this);

		// @formatter:on

		temporalPayload = "{\r\n    " + "\"id\": \"urn:ngsi-ld:testunit:151\","
				+ "\r\n    \"type\": \"AirQualityObserved\"," + "\r\n    \"airQualityLevel\": " + "[\r\n        {"
				+ "\r\n              " + "\r\n            "
				+ "\"type\": \"Property\",\r\n            \"value\": \"good\","
				+ "\r\n            \"observedAt\": \"2018-08-07T12:00:00Z\"" + "\r\n        }," + "\r\n        {"
				+ "\r\n               " + "\r\n            \"type\": \"Property\","
				+ "\r\n            \"value\": \"moderate\","
				+ "\r\n            \"observedAt\": \"2018-08-14T12:00:00Z\"" + "\r\n        }," + "\r\n        "
				+ "{\r\n       " + "\r\n            \"type\": \"Property\","
				+ "\r\n            \"value\": \"unhealthy\","
				+ "\r\n            \"observedAt\": \"2018-09-14T12:00:00Z\"" + "\r\n        }\r\n    ],"
				+ "\r\n    \"@context\": [" + "\r\n    \"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld\""
				+ "\r\n    ]\r\n}\r\n\r\n";

	}

	@After
	public void tearDown() {
		temporalPayload = "";
	}

	/**
	 * this method is use for create the temporalEntity
	 */
	@Test
	public void createTemporalEntityTest() {
		try {
			when(historyService.createTemporalEntityFromBinding(any())).thenReturn(uri);
			mockMvc.perform(post("/ngsi-ld/v1/temporal/entities/").contentType(AppConstants.NGB_APPLICATION_JSONLD)
					.accept(AppConstants.NGB_APPLICATION_JSONLD).content(temporalPayload))
					.andExpect(status().isCreated());
			verify(historyService, times(1)).createTemporalEntityFromBinding(any());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void createTemporalEntityBadRequestTest() {
		try {
			when(historyService.createTemporalEntityFromBinding(any()))
					.thenThrow(new ResponseException(ErrorType.BadRequestData));
			mockMvc.perform(post("/ngsi-ld/v1/temporal/entities/").contentType(AppConstants.NGB_APPLICATION_JSONLD)
					.accept(AppConstants.NGB_APPLICATION_JSONLD).content(temporalPayload))
					.andExpect(status().isBadRequest()).andExpect(jsonPath("$.title").value("Bad Request Data."));

			verify(historyService, times(1)).createTemporalEntityFromBinding(any());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}
	
	@Test
	public void createTemporalEntityInternalServerErrorTest() {
		try {
			when(historyService.createTemporalEntityFromBinding(any()))
					.thenThrow(new Exception());
			mockMvc.perform(post("/ngsi-ld/v1/temporal/entities/").contentType(AppConstants.NGB_APPLICATION_JSONLD)
					.accept(AppConstants.NGB_APPLICATION_JSONLD).content(temporalPayload))
					.andExpect(status().isInternalServerError());

			verify(historyService, times(1)).createTemporalEntityFromBinding(any());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}
	
	/**
	 * this method is use for update the temporalEntity
	 */

	@Test
	public void updateAttrById() {
		try {
			mockMvc.perform(post("/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:testunit:151/attrs")
					.contentType(AppConstants.NGB_APPLICATION_JSONLD).accept(AppConstants.NGB_APPLICATION_JSONLD)
					.content(temporalPayload)).andExpect(status().isNoContent());

		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}
	
	@Test
	public void updateAttrByIdInternalServerError() {
		try {
			Mockito.doThrow(new Exception()).when(historyService).addAttrib2TemporalEntity(any(), any());
			mockMvc.perform(post("/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:testunit:151/attrs")
					.contentType(AppConstants.NGB_APPLICATION_JSONLD).accept(AppConstants.NGB_APPLICATION_JSONLD)
					.content(temporalPayload)).andExpect(status().isInternalServerError());

		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}	
	}
	
	@Test
	public void updateAttrByIdBadRequest() {
		try {
			Mockito.doThrow(new ResponseException(ErrorType.BadRequestData)).when(historyService).addAttrib2TemporalEntity(any(), any());
			mockMvc.perform(post("/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:testunit:151/attrs")
					.contentType(AppConstants.NGB_APPLICATION_JSONLD).accept(AppConstants.NGB_APPLICATION_JSONLD)
					.content(temporalPayload)).andExpect(status().isBadRequest());

		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}	
	}

	/**
	 * this method is use for modify the temporalEntity
	 */
	@Test
	public void modifyAttribInstanceTemporalEntityTest() {
		try {
			mockMvc.perform(patch("/ngsi-ld/v1/temporal/entities/{entityId}/attrs/{attrId}/{instanceId}",
					"urn:ngsi-ld:testunit:151", "airQualityLevel", "urn:ngsi-ld:d43aa0fe-a986-4479-9fac-35b7eba232041")
							.contentType(AppConstants.NGB_APPLICATION_JSON).accept(AppConstants.NGB_APPLICATION_JSONLD)
							.content(temporalPayload))
					.andExpect(status().isNoContent());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}
	
	@Test
	public void modifyAttribInstanceTemporalEntityInternalServerError() {
		
		try {
			Mockito.doThrow(new Exception()).when(historyService).modifyAttribInstanceTemporalEntity(any(),any(), any(), any(), any());
			mockMvc.perform(patch("/ngsi-ld/v1/temporal/entities/{entityId}/attrs/{attrId}/{instanceId}",
					"urn:ngsi-ld:testunit:151", "airQualityLevel", "urn:ngsi-ld:d43aa0fe-a986-4479-9fac-35b7eba232041")
							.contentType(AppConstants.NGB_APPLICATION_JSON).accept(AppConstants.NGB_APPLICATION_JSONLD)
							.content(temporalPayload))
					.andExpect(status().isInternalServerError());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}
	
	@Test
	public void modifyAttribInstanceTemporalEntityBadRequest() {
		
		try {
			Mockito.doThrow(new ResponseException(ErrorType.BadRequestData)).when(historyService).modifyAttribInstanceTemporalEntity(any(),any(), any(), any(), any());
			mockMvc.perform(patch("/ngsi-ld/v1/temporal/entities/{entityId}/attrs/{attrId}/{instanceId}",
					"urn:ngsi-ld:testunit:151", "airQualityLevel", "urn:ngsi-ld:d43aa0fe-a986-4479-9fac-35b7eba232041")
							.contentType(AppConstants.NGB_APPLICATION_JSON).accept(AppConstants.NGB_APPLICATION_JSONLD)
							.content(temporalPayload))
					.andExpect(status().isBadRequest());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	/**
	 * this method is use for delete the temporalEntity
	 */
	
	@Test
	public void deleteTemporalEntityByAttr() {
		List<Object> linkHeaders = new ArrayList<>();
		try {
			mockMvc.perform(
					MockMvcRequestBuilders
							.delete("/ngsi-ld/v1/temporal/entities/{entityId}/attrs/{attrId}",
									"urn:ngsi-ld:testunit:151", "airQualityLevel")
							.contentType(AppConstants.NGB_APPLICATION_JSONLD))
					.andExpect(status().isNoContent());
			verify(historyService, times(1)).delete("urn:ngsi-ld:testunit:151", "airQualityLevel", null, linkHeaders);
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void deleteTemporalEntity() {
		List<Object> linkHeaders = new ArrayList<>();
		try {
			mockMvc.perform(MockMvcRequestBuilders
					.delete("/ngsi-ld/v1/temporal/entities/{entityId}", "urn:ngsi-ld:testunit:151")
					.contentType(AppConstants.NGB_APPLICATION_JSONLD)).andExpect(status().isNoContent());
			verify(historyService, times(1)).delete("urn:ngsi-ld:testunit:151", null, null, linkHeaders);
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void deleteTemporalEntityInternalServerError() {
		List<Object> linkHeaders = null;
		try {
			Mockito.doThrow(new Exception()).when(historyService).delete(any(), any(), any(), any());
			mockMvc.perform(MockMvcRequestBuilders
					.delete("/ngsi-ld/v1/temporal/entities/{entities}", "urn:ngsi-ld:testunit:151")
					.contentType(AppConstants.NGB_APPLICATION_JSONLD)).andExpect(status().isInternalServerError());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}
	
	@Test
	public void deleteTemporalEntityBadRequest() {
		List<Object> linkHeaders = null;
		try {
			Mockito.doThrow(new ResponseException(ErrorType.BadRequestData)).when(historyService).delete(any(), any(), any(), any());
			mockMvc.perform(MockMvcRequestBuilders
					.delete("/ngsi-ld/v1/temporal/entities/{entities}", "urn:ngsi-ld:testunit:151")
					.contentType(AppConstants.NGB_APPLICATION_JSONLD)).andExpect(status().isBadRequest());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void deleteTemporalEntityByAttrInternalServerError() {
		List<Object> linkHeaders = null;
		try {
			Mockito.doThrow(new Exception()).when(historyService).delete(any(), any(), any(), any());
			mockMvc.perform(
					MockMvcRequestBuilders
							.delete("/ngsi-ld/v1/temporal/entities/{entityId}/attrs/{attrId}",
									"urn:ngsi-ld:testunit:151", "airQualityLevel")
							.contentType(AppConstants.NGB_APPLICATION_JSONLD))
					.andExpect(status().isInternalServerError());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}
	
	@Test
	public void deleteTemporalEntityByAttrBadRequest() {
		List<Object> linkHeaders = null;
		try {
			Mockito.doThrow(new ResponseException(ErrorType.BadRequestData)).when(historyService).delete(any(), any(), any(), any());
			mockMvc.perform(
					MockMvcRequestBuilders
							.delete("/ngsi-ld/v1/temporal/entities/{entityId}/attrs/{attrId}",
									"urn:ngsi-ld:testunit:151", "airQualityLevel")
							.contentType(AppConstants.NGB_APPLICATION_JSONLD))
					.andExpect(status().isBadRequest());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void deleteTemporalEntityByAttrResourceNotFound() {
		List<Object> linkHeaders = null;
		try {
			Mockito.doThrow(new ResponseException(ErrorType.NotFound)).when(historyService).delete(any(), any(), any(), any());
			mockMvc.perform(
					MockMvcRequestBuilders
							.delete("/ngsi-ld/v1/temporal/entities/{entityId}/attrs/{attrId}",
									"urn:ngsi-ld:testunit:151", "airQualityLevel")
							.contentType(AppConstants.NGB_APPLICATION_JSONLD))
					.andExpect(status().isNotFound());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}
	
	/**
	 * this method is use for get the temporalEntity
	 */
	
	@Test
	public void getTemporalEntityById() {
		try {

			mockMvc.perform(get("/ngsi-ld/v1/temporal/entities/{entityId}", "urn:ngsi-ld:testunit:151")
					.accept(AppConstants.NGB_APPLICATION_JSON)).andExpect(status().isOk())
					.andExpect(jsonPath("$.id").value("urn:ngsi-ld:testunit:151")).andDo(print());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	@Test
	public void getTemporalEntityByTimefilter() {
		try {
			mockMvc.perform(
					get("/ngsi-ld/v1/temporal/entities/2018-08-07T12:00:00Z").accept(AppConstants.NGB_APPLICATION_JSON))
					.andExpect(status().isOk()).andDo(print());

		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}
}
