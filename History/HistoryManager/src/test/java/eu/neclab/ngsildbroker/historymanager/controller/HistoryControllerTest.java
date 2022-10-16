
package eu.neclab.ngsildbroker.historymanager.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import java.net.URI;
import java.util.ArrayList;
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
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.results.CreateResult;
import eu.neclab.ngsildbroker.commons.datatypes.results.QueryResult;
import eu.neclab.ngsildbroker.commons.datatypes.results.UpdateResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.ngsiqueries.ParamsResolver;
import eu.neclab.ngsildbroker.historymanager.repository.HistoryDAO;
import eu.neclab.ngsildbroker.historymanager.service.HistoryService;
import eu.neclab.ngsildbroker.historymanager.utils.Validator;

@RunWith(SpringRunner.class)

@SpringBootTest(properties = { "spring.main.allow-bean-definition-overriding=true" })
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
	@Value("${atcontext.url}")
	String atContextServerUrl;

	private String temporalPayload;
	private URI uri;
	private String payload;

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

		payload = "";

	}

	@After
	public void tearDown() {
		temporalPayload = null;
		payload = null;
	}

	/**
	 * this method is use for create the temporalEntity
	 */
	@Test
	public void createTemporalEntityTest() {
		try {
			when(historyService.createEntry(any(), any()))
					.thenReturn(new CreateResult("urn:ngsi-ld:testunit:151", true));

			ResultActions resultAction = mockMvc
					.perform(post("/ngsi-ld/v1/temporal/entities/").contentType(AppConstants.NGB_APPLICATION_JSONLD)
							.accept(AppConstants.NGB_APPLICATION_JSONLD).content(temporalPayload))
					.andExpect(status().isCreated());
			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			assertEquals(201, status);

			verify(historyService, times(1)).createEntry(any(), any());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	/**
	 * this method is try to create the temporalEntity having "BAD REQUEST"
	 */
	@Test
	public void createTemporalEntityBadRequestTest() {
		try {
			when(historyService.createEntry(any(), any()))
					.thenThrow(new ResponseException(ErrorType.BadRequestData, "Bad Request Data."));
			ResultActions resultAction = mockMvc
					.perform(post("/ngsi-ld/v1/temporal/entities/").contentType(AppConstants.NGB_APPLICATION_JSONLD)
							.accept(AppConstants.NGB_APPLICATION_JSONLD).content(temporalPayload))
					.andExpect(status().isBadRequest()).andExpect(jsonPath("$.title").value("Bad Request Data."));
			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			assertEquals(400, status);
			verify(historyService, times(1)).createEntry(any(), any());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	/**
	 * this method is try to create the temporalEntity having "INTERNAL SERVER
	 * ERROR"
	 */

	@Test
	public void createTemporalEntityInternalServerErrorTest() {
		try {
			when(historyService.createEntry(any(), any()))
					.thenThrow(new ResponseException(ErrorType.InternalError, "Internal error"));
			ResultActions resultAction = mockMvc
					.perform(post("/ngsi-ld/v1/temporal/entities/").contentType(AppConstants.NGB_APPLICATION_JSONLD)
							.accept(AppConstants.NGB_APPLICATION_JSONLD).content(temporalPayload))
					.andExpect(status().isInternalServerError());
			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			assertEquals(500, status);
			verify(historyService, times(1)).createEntry(any(), any());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	/**
	 * this method is try to get temporalEntity
	 * 
	 */

	@Test
	public void getTemporalEntityTest() throws Exception {
		try {

			QueryResult result = mock(QueryResult.class);
			when(historyService.getData(any(), any(), any(), any(), any())).thenReturn(result);

			ResultActions resultAction = mockMvc
					.perform(get("/ngsi-ld/v1/temporal/entities?type=Test1").accept(AppConstants.NGB_APPLICATION_JSON))
					.andExpect(status().isOk());

			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			assertEquals(200, status);
			verify(historyService, times(1)).getData(any(), any(), any(), any(), any());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	/**
	 * this method is used to get temporal entity having "Tenant not found."
	 * 
	 */

	@Test
	public void getTemporalEntitiesNotFoundTest() throws Exception {
		try {

			QueryResult result = mock(QueryResult.class);
			when(historyService.getData(any(), any(), any(), any(), any()))
					.thenThrow(new ResponseException(ErrorType.TenantNotFound, "Tenant not found."));

			ResultActions resultAction = mockMvc
					.perform(get("/ngsi-ld/v1/temporal/entities?type=Test1").accept(AppConstants.NGB_APPLICATION_JSON))
					.andExpect(status().isNotFound());

			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			assertEquals(404, status);
			verify(historyService, times(1)).getData(any(), any(), any(), any(), any());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	/**
	 * this method is try to get the temporalEntity having "BAD REQUEST"
	 */

	@Test
	public void getQueryForEntitiesBadRequestTest() throws Exception {
		try {

			QueryResult result = mock(QueryResult.class);
			when(historyService.getData(any(), any(), any(), any(), any()))
					.thenThrow(new ResponseException(ErrorType.BadRequestData, "invalid query"));

			ResultActions resultAction = mockMvc
					.perform(get("/ngsi-ld/v1/temporal/entities").accept(AppConstants.NGB_APPLICATION_JSON))
					.andExpect(status().isBadRequest());

			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			assertEquals(400, status);
			verify(historyService, times(0)).getData(any(), any(), any(), any(), any());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	/**
	 * this method is use for update the temporalEntity
	 */

	@Test
	public void addAttrib2TemopralEntityTest() throws Exception {
		try {
			UpdateResult result = mock(UpdateResult.class);
			when(historyService.appendToEntry(any(), any(), any(), any())).thenReturn(result);
			ResultActions resultAction = mockMvc
					.perform(post("/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:testunit:151/attrs/")
							.contentType(AppConstants.NGB_APPLICATION_JSONLD)
							.accept(AppConstants.NGB_APPLICATION_JSONLD).content(temporalPayload))
					.andExpect(status().isNoContent());

			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			assertEquals(204, status);
			verify(historyService, times(1)).appendToEntry(any(), any(), any(), any());

		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	/**
	 * this method is try to update the temporalEntity with "INTERNAL SERVER ERROR"
	 */

	@Test
	public void addAttrib2TemopralEntityInternalServerErrorTest() {
		try {
			UpdateResult result = mock(UpdateResult.class);
			when(historyService.appendToEntry(any(), any(), any(), any()))
					.thenThrow(new ResponseException(ErrorType.InternalError, "Internal error"));

			ResultActions resultAction = mockMvc
					.perform(post("/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:testunit:151/attrs")
							.contentType(AppConstants.NGB_APPLICATION_JSONLD)
							.accept(AppConstants.NGB_APPLICATION_JSONLD).content(temporalPayload))
					.andExpect(status().isInternalServerError());
			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			assertEquals(500, status);
			verify(historyService, times(1)).appendToEntry(any(), any(), any(), any());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	/**
	 * this method is try to update the temporalEntity having "BAD REQUEST"
	 */

	@Test
	public void addAttrib2TemopralEntityBadRequestTest() {
		try {
			UpdateResult result = mock(UpdateResult.class);
			when(historyService.appendToEntry(any(), any(), any(), any()))
					.thenThrow(new ResponseException(ErrorType.BadRequestData, "Bad Request Data."));

			ResultActions resultAction = mockMvc
					.perform(post("/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:testunit:151/attrs")
							.contentType(AppConstants.NGB_APPLICATION_JSONLD)
							.accept(AppConstants.NGB_APPLICATION_JSONLD).content(temporalPayload))
					.andExpect(status().isBadRequest());
			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			assertEquals(400, status);
			verify(historyService, times(1)).appendToEntry(any(), any(), any(), any());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	/**
	 * this method is try to modify the attribute of temporalEntity having "BAD
	 * REQUEST"
	 */

	@Test
	public void modifyAttribInstanceTemporalEntityBadRequest() {

		try {
			Mockito.doThrow(new ResponseException(ErrorType.BadRequestData, "Bad Request Data.")).when(historyService)
					.modifyAttribInstanceTemporalEntity(any(), any(), any(), any(), any(), any());
			mockMvc.perform(patch("/ngsi-ld/v1/temporal/entities/{entityId}/attrs/{attrId}/{instanceId}",
					"urn:ngsi-ld:testunit:151", "airQualityLevel", "urn:ngsi-ld:d43aa0fe-a986-4479-9fac-35b7eba232041")
					.contentType(AppConstants.NGB_APPLICATION_JSON).accept(AppConstants.NGB_APPLICATION_JSONLD)
					.content(temporalPayload)).andExpect(status().isBadRequest());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	/**
	 * this method is use for delete the temporalEntity by attribute
	 */

	@Test
	public void deleteTemporalEntityByAttrTest() {
		try {
			when(historyService.delete(any(), any(), any(), any(), any(), any())).thenReturn(true);
			ResultActions resultAction = mockMvc
					.perform(MockMvcRequestBuilders
							.delete("/ngsi-ld/v1/temporal/entities/{entityId}/attrs/{attrId}",
									"urn:ngsi-ld:testunit:151", "airQualityLevel")
							.contentType(AppConstants.NGB_APPLICATION_JSONLD))
					.andExpect(status().isNoContent());

			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			assertEquals(204, status);
			verify(historyService, times(1)).delete(any(), any(), any(), any(), any(), any());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	/**
	 * this method is try to delete the attribute of temporalEntity having "INTERNAL
	 * SERVER ERROR"
	 */

	@Test
	public void deleteTemporalEntityByAttrInternalServerErrorTest() throws Exception {
		try {
			when(historyService.delete(any(), any(), any(), any(), any(), any()))
					.thenThrow(new ResponseException(ErrorType.InternalError, "Internal error"));

			ResultActions resultAction = mockMvc
					.perform(MockMvcRequestBuilders
							.delete("/ngsi-ld/v1/temporal/entities/{entityId}/attrs/{attrId}",
									"urn:ngsi-ld:testunit:151", "airQualityLevel")
							.contentType(AppConstants.NGB_APPLICATION_JSONLD))
					.andExpect(status().isInternalServerError());

			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			assertEquals(500, status);
			verify(historyService, times(1)).delete(any(), any(), any(), any(), any(), any());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	/**
	 * this method is try to delete the attribute of temporalEntity having "BAD
	 * REQUEST"
	 */
	@Test
	public void deleteTemporalEntityByAttrBadRequestTest() {
		try {
			when(historyService.delete(any(), any(), any(), any(), any(), any()))
					.thenThrow(new ResponseException(ErrorType.BadRequestData, "Bad Request Data."));

			ResultActions resultAction = mockMvc
					.perform(MockMvcRequestBuilders
							.delete("/ngsi-ld/v1/temporal/entities/{entityId}/attrs/{attrId}",
									"urn:ngsi-ld:testunit:151", "airQualityLevel")
							.contentType(AppConstants.NGB_APPLICATION_JSONLD))
					.andExpect(status().isBadRequest());

			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			assertEquals(400, status);
			verify(historyService, times(1)).delete(any(), any(), any(), any(), any(), any());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	/**
	 * this method is used to delete the temporal entity having "Resource not
	 * found".
	 */
	@Test
	public void deleteTemporalEntityByAttrResourceNotFoundTest() {
		try {
			when(historyService.delete(any(), any(), any(), any(), any(), any()))
					.thenThrow(new ResponseException(ErrorType.NotFound, "Resource not found."));

			ResultActions resultAction = mockMvc
					.perform(MockMvcRequestBuilders
							.delete("/ngsi-ld/v1/temporal/entities/{entityId}/attrs/{attrId}",
									"urn:ngsi-ld:testunit:151", "airQualityLevel")
							.contentType(AppConstants.NGB_APPLICATION_JSONLD))
					.andExpect(status().isNotFound());

			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			assertEquals(404, status);
			verify(historyService, times(1)).delete(any(), any(), any(), any(), any(), any());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	/**
	 * this method is use to delete the temporalEntity
	 */

	@Test
	public void deleteTemporalEntityByIdTest() throws Exception {
		List<Object> linkHeaders = new ArrayList<>();
		when(historyService.deleteEntry(any(), any())).thenReturn(true);
		try {
			ResultActions resultAction = mockMvc.perform(MockMvcRequestBuilders
					.delete("/ngsi-ld/v1/temporal/entities/{entityId}", "urn:ngsi-ld:testunit:151")
					.contentType(AppConstants.NGB_APPLICATION_JSONLD)).andExpect(status().isNoContent());

			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse res = mvcResult.getResponse();
			int status = res.getStatus();
			assertEquals(204, status);
			verify(historyService, times(1)).deleteEntry(any(), any());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	/**
	 * this method is used to Delete temporal entity having "Resource not found."
	 * 
	 */

	@Test
	public void deleteTemporalEntityByIdNotFoundTest() throws Exception {
		List<Object> linkHeaders = new ArrayList<>();
		when(historyService.deleteEntry(any(), any()))
				.thenThrow(new ResponseException(ErrorType.NotFound, "Resource not found."));
		try {
			ResultActions resultAction = mockMvc.perform(MockMvcRequestBuilders
					.delete("/ngsi-ld/v1/temporal/entities/{entityId}", "urn:ngsi-ld:testunit:151")
					.contentType(AppConstants.NGB_APPLICATION_JSONLD)).andExpect(status().isNotFound());

			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse res = mvcResult.getResponse();
			int status = res.getStatus();
			assertEquals(404, status);
			verify(historyService, times(1)).deleteEntry(any(), any());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	/**
	 * this method is try to delete the temporalEntity having "INTERNAL SERVER
	 * ERROR"
	 */

	@Test
	public void deleteTemporalEntityInternalServerError() {
		try {
			// Mockito.doThrow(new Exception()).when(historyService).delete(any(), any(),
			// any(), any(), any());
			when(historyService.deleteEntry(any(), any()))
					.thenThrow(new ResponseException(ErrorType.InternalError, "Internal error"));

			ResultActions resultAction = mockMvc
					.perform(MockMvcRequestBuilders
							.delete("/ngsi-ld/v1/temporal/entities/{entities}", "urn:ngsi-ld:testunit:151")
							.contentType(AppConstants.NGB_APPLICATION_JSONLD))
					.andExpect(status().isInternalServerError());
			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			assertEquals(500, status);
			verify(historyService, times(1)).deleteEntry(any(), any());

		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

	/**
	 * this method is try to delete the temporalEntity having "BAD REQUEST"
	 */

	@Test
	public void deleteTemporalEntityBadRequest() {
		try {
			when(historyService.deleteEntry(any(), any()))
					.thenThrow(new ResponseException(ErrorType.BadRequestData, "Bad Request Data."));

			ResultActions resultAction = mockMvc.perform(MockMvcRequestBuilders
					.delete("/ngsi-ld/v1/temporal/entities/{entities}", "urn:ngsi-ld:testunit:151")
					.contentType(AppConstants.NGB_APPLICATION_JSONLD)).andExpect(status().isBadRequest());
			MvcResult mvcResult = resultAction.andReturn();
			MockHttpServletResponse response = mvcResult.getResponse();
			int status = response.getStatus();
			assertEquals(400, status);
			verify(historyService, times(1)).deleteEntry(any(), any());
		} catch (Exception e) {
			Assert.fail(e.getMessage());
		}
	}

}
