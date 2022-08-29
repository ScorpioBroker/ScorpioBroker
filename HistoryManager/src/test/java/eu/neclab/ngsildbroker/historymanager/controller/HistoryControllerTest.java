
  package eu.neclab.ngsildbroker.historymanager.controller;
 
  import static io.restassured.RestAssured.given;
  import static org.junit.jupiter.api.Assertions.assertEquals;
  import static org.mockito.ArgumentMatchers.any;
  import static org.mockito.Mockito.mock;
  import static org.mockito.Mockito.when;
  import javax.ws.rs.core.HttpHeaders;
  import javax.ws.rs.core.Response.Status;
  import org.junit.jupiter.api.AfterEach;
  import org.junit.jupiter.api.BeforeEach;
  import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
  import org.junit.jupiter.api.Order;
  import org.junit.jupiter.api.Test;
  import org.junit.jupiter.api.TestMethodOrder;
  import org.mockito.InjectMocks;
  import org.mockito.Mockito;
  import java.net.URI; 
  import java.util.ArrayList;
  import java.util.List;
  import eu.neclab.ngsildbroker.commons.constants.AppConstants;
  import eu.neclab.ngsildbroker.commons.datatypes.results.CreateResult;
  import eu.neclab.ngsildbroker.commons.enums.ErrorType; 
  import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
  import eu.neclab.ngsildbroker.commons.ngsiqueries.ParamsResolver; 
  import eu.neclab.ngsildbroker.historymanager.repository.HistoryDAO; 
  import eu.neclab.ngsildbroker.historymanager.service.HistoryService; 
  import eu.neclab.ngsildbroker.historymanager.utils.Validator;
  import io.quarkus.test.junit.QuarkusTest;
  import io.quarkus.test.junit.TestProfile;
  import io.restassured.response.ExtractableResponse;
  import io.restassured.response.Response;
  import io.smallrye.mutiny.Uni;
  
	@QuarkusTest
	@TestMethodOrder(OrderAnnotation.class)
	@TestProfile(CustomProfile.class)
	public class HistoryControllerTest {

		@InjectMocks
		private HistoryService historyService;
		private String temporalPayload;
		private String temporalInvalidPayload;

		@BeforeEach
		public void setup() {
			historyService = mock(HistoryService.class);
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
					+ "\r\n    \"@context\": ["
					+ "\r\n    \"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.3.jsonld\""
					+ "\r\n    ]\r\n}\r\n\r\n";
			
			temporalInvalidPayload = "{\r\n    " + "\"id\": \"urn151\","
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
					+ "\r\n    \"@context\": ["
					+ "\r\n    \"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.3.jsonld\""
					+ "\r\n    ]\r\n}\r\n\r\n";
		}

		@AfterEach
		public void teardown() {
			temporalPayload = null;
			temporalInvalidPayload = null;
		}

		/*
		 * this method is used to create temporal entities
		 */
		@Test
		@Order(1)
		public void createTemporalEntityTest() {

			CreateResult cr = Mockito.mock(CreateResult.class);
			System.out.println("----"+temporalPayload);
			cr.setEntityId("urn:ngsi-ld:testunit:151");
			when(historyService.createEntry(any(), any())).thenReturn(Uni.createFrom().item(cr));
			ExtractableResponse<Response> response = given().body(temporalPayload)
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSONLD)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSON).when()
					.post("/ngsi-ld/v1/temporal/entities/").then().statusCode(Status.CREATED.getStatusCode())
					.statusCode(201).extract();
			int statusCode = response.statusCode();
			assertEquals(201, statusCode);
		}
		
	     /*
		 * this method is create the temporalEntity having "BAD REQUEST"
		 */
		@Test
		@Order(2)
	    public void createTemporalEntityBadRequestTest() {
			try {
				ExtractableResponse<Response> response =	given()
			      .body(temporalInvalidPayload)
			    .header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSONLD)
			     .header(HttpHeaders.ACCEPT,AppConstants.NGB_APPLICATION_JSON)
			    .when()
			    .post("/ngsi-ld/v1/temporal/entities/")
			    .then()
			       .statusCode(Status.BAD_REQUEST.getStatusCode())
			       .statusCode(400).extract();
				int statusCode = response.statusCode();
				assertEquals(400, statusCode);
			}catch(Exception e) {
				e.printStackTrace();
		   }
		}
		
		/*
		 * this method is use to delete the temporalEntity
		 */
		@Test
		@Order(3)
		public void deleteTemporalEntity() {
			try {
				ExtractableResponse<Response> response = given()
						.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
						.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
						.delete("/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:testunit:151").then()
						.statusCode(Status.NO_CONTENT.getStatusCode()).statusCode(204).extract();
				int statusCode = response.statusCode();
				assertEquals(204, statusCode);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}
	
/**
	 * this method is try to create the temporalEntity having "INTERNAL SERVER
	 * ERROR"
	 */
/*
 * 
 * @Test public void createTemporalEntityInternalServerErrorTest() { try {
 * when(historyService.createTemporalEntityFromBinding(any(),
 * any())).thenThrow(new Exception());
 * mockMvc.perform(post("/ngsi-ld/v1/temporal/entities/").contentType(
 * AppConstants.NGB_APPLICATION_JSONLD)
 * .accept(AppConstants.NGB_APPLICATION_JSONLD).content(temporalPayload))
 * .andExpect(status().isInternalServerError());
 * 
 * verify(historyService, times(1)).createTemporalEntityFromBinding(any(),
 * any()); } catch (Exception e) { Assert.fail(e.getMessage()); } }
 * 
 *//**
	 * this method is use for update the temporalEntity
	 */
/*
 * 
 * @Test public void updateAttrById() { try { mockMvc.perform(post(
 * "/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:testunit:151/attrs")
 * .contentType(AppConstants.NGB_APPLICATION_JSONLD).accept(AppConstants.
 * NGB_APPLICATION_JSONLD)
 * .content(temporalPayload)).andExpect(status().isNoContent());
 * 
 * } catch (Exception e) { Assert.fail(e.getMessage()); } }
 * 
 *//**
	 * this method is try to update the temporalEntity with "INTERNAL SERVER ERROR"
	 */
/*
 * 
 * @Test public void updateAttrByIdInternalServerError() { try {
 * Mockito.doThrow(new
 * Exception()).when(historyService).addAttrib2TemporalEntity(any(), any(),
 * any()); mockMvc.perform(post(
 * "/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:testunit:151/attrs")
 * .contentType(AppConstants.NGB_APPLICATION_JSONLD).accept(AppConstants.
 * NGB_APPLICATION_JSONLD)
 * .content(temporalPayload)).andExpect(status().isInternalServerError());
 * 
 * } catch (Exception e) { Assert.fail(e.getMessage()); } }
 * 
 *//**
	 * this method is try to update the temporalEntity having "BAD REQUEST"
	 */
/*
 * 
 * @Test public void updateAttrByIdBadRequest() { try { Mockito.doThrow(new
 * ResponseException(ErrorType.BadRequestData)).when(historyService)
 * .addAttrib2TemporalEntity(any(), any(), any()); mockMvc.perform(post(
 * "/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:testunit:151/attrs")
 * .contentType(AppConstants.NGB_APPLICATION_JSONLD).accept(AppConstants.
 * NGB_APPLICATION_JSONLD)
 * .content(temporalPayload)).andExpect(status().isBadRequest());
 * 
 * } catch (Exception e) { Assert.fail(e.getMessage()); } }
 * 
 *//**
	 * this method is use for modify the attribute of temporalEntity
	 */
/*
 * 
 * @Test public void modifyAttribInstanceTemporalEntityTest() { try {
 * mockMvc.perform(patch(
 * "/ngsi-ld/v1/temporal/entities/{entityId}/attrs/{attrId}/{instanceId}",
 * "urn:ngsi-ld:testunit:151", "airQualityLevel",
 * "urn:ngsi-ld:d43aa0fe-a986-4479-9fac-35b7eba232041")
 * .contentType(AppConstants.NGB_APPLICATION_JSON).accept(AppConstants.
 * NGB_APPLICATION_JSONLD) .content(temporalPayload))
 * .andExpect(status().isNoContent()); } catch (Exception e) {
 * Assert.fail(e.getMessage()); } }
 * 
 *//**
	 * this method is try to modify the attribute of temporalEntity having "INTERNAL
	 * SERVER ERROR"
	 */
/*
 * 
 * @Test public void modifyAttribInstanceTemporalEntityInternalServerError() {
 * 
 * try { Mockito.doThrow(new
 * Exception()).when(historyService).modifyAttribInstanceTemporalEntity(any(),
 * any(), any(), any(), any(), any()); mockMvc.perform(patch(
 * "/ngsi-ld/v1/temporal/entities/{entityId}/attrs/{attrId}/{instanceId}",
 * "urn:ngsi-ld:testunit:151", "airQualityLevel",
 * "urn:ngsi-ld:d43aa0fe-a986-4479-9fac-35b7eba232041")
 * .contentType(AppConstants.NGB_APPLICATION_JSON).accept(AppConstants.
 * NGB_APPLICATION_JSONLD) .content(temporalPayload))
 * .andExpect(status().isInternalServerError()); } catch (Exception e) {
 * Assert.fail(e.getMessage()); } }
 * 
 *//**
	 * this method is try to modify the attribute of temporalEntity having "BAD
	 * REQUEST"
	 */
/*
 * 
 * @Test public void modifyAttribInstanceTemporalEntityBadRequest() {
 * 
 * try { Mockito.doThrow(new
 * ResponseException(ErrorType.BadRequestData)).when(historyService)
 * .modifyAttribInstanceTemporalEntity(any(), any(), any(), any(), any(),
 * any()); mockMvc.perform(patch(
 * "/ngsi-ld/v1/temporal/entities/{entityId}/attrs/{attrId}/{instanceId}",
 * "urn:ngsi-ld:testunit:151", "airQualityLevel",
 * "urn:ngsi-ld:d43aa0fe-a986-4479-9fac-35b7eba232041")
 * .contentType(AppConstants.NGB_APPLICATION_JSON).accept(AppConstants.
 * NGB_APPLICATION_JSONLD) .content(temporalPayload))
 * .andExpect(status().isBadRequest()); } catch (Exception e) {
 * Assert.fail(e.getMessage()); } }
 * 
 *//**
	 * this method is use for delete the temporalEntity by attribute
	 */
/*
 * 
 * @Test public void deleteTemporalEntityByAttr() { List<Object> linkHeaders =
 * new ArrayList<>(); try { mockMvc.perform( MockMvcRequestBuilders
 * .delete("/ngsi-ld/v1/temporal/entities/{entityId}/attrs/{attrId}",
 * "urn:ngsi-ld:testunit:151", "airQualityLevel")
 * .contentType(AppConstants.NGB_APPLICATION_JSONLD))
 * .andExpect(status().isNoContent()); verify(historyService,
 * times(1)).delete(any(), "urn:ngsi-ld:testunit:151", "airQualityLevel", null,
 * linkHeaders); } catch (Exception e) { Assert.fail(e.getMessage()); } }
 * 
 *//**
	 * this method is use to delete the temporalEntity
	 */
/*
 * 
 * @Test public void deleteTemporalEntity() { List<Object> linkHeaders = new
 * ArrayList<>(); try { mockMvc.perform(MockMvcRequestBuilders
 * .delete("/ngsi-ld/v1/temporal/entities/{entityId}",
 * "urn:ngsi-ld:testunit:151")
 * .contentType(AppConstants.NGB_APPLICATION_JSONLD)).andExpect(status().
 * isNoContent()); verify(historyService, times(1)).delete(any(),
 * "urn:ngsi-ld:testunit:151", null, null, linkHeaders); } catch (Exception e) {
 * Assert.fail(e.getMessage()); } }
 * 
 *//**
	 * this method is try to delete the temporalEntity having "INTERNAL SERVER
	 * ERROR"
	 */
/*
 * 
 * @Test public void deleteTemporalEntityInternalServerError() { try {
 * Mockito.doThrow(new Exception()).when(historyService).delete(any(), any(),
 * any(), any(), any()); mockMvc.perform(MockMvcRequestBuilders
 * .delete("/ngsi-ld/v1/temporal/entities/{entities}",
 * "urn:ngsi-ld:testunit:151")
 * .contentType(AppConstants.NGB_APPLICATION_JSONLD)).andExpect(status().
 * isInternalServerError()); } catch (Exception e) {
 * Assert.fail(e.getMessage()); } }
 * 
 *//**
	 * this method is try to delete the temporalEntity having "BAD REQUEST"
	 */
/*
 * 
 * @Test public void deleteTemporalEntityBadRequest() { try {
 * Mockito.doThrow(new
 * ResponseException(ErrorType.BadRequestData)).when(historyService).delete(any(
 * ), any(), any(), any(), any()); mockMvc.perform(MockMvcRequestBuilders
 * .delete("/ngsi-ld/v1/temporal/entities/{entities}",
 * "urn:ngsi-ld:testunit:151")
 * .contentType(AppConstants.NGB_APPLICATION_JSONLD)).andExpect(status().
 * isBadRequest()); } catch (Exception e) { Assert.fail(e.getMessage()); } }
 * 
 *//**
	 * this method is try to delete the attribute of temporalEntity having "INTERNAL
	 * SERVER ERROR"
	 */
/*
 * 
 * @Test public void deleteTemporalEntityByAttrInternalServerError() { try {
 * Mockito.doThrow(new Exception()).when(historyService).delete(any(), any(),
 * any(), any(), any()); mockMvc.perform( MockMvcRequestBuilders
 * .delete("/ngsi-ld/v1/temporal/entities/{entityId}/attrs/{attrId}",
 * "urn:ngsi-ld:testunit:151", "airQualityLevel")
 * .contentType(AppConstants.NGB_APPLICATION_JSONLD))
 * .andExpect(status().isInternalServerError()); } catch (Exception e) {
 * Assert.fail(e.getMessage()); } }
 * 
 *//**
	 * this method is try to delete the attribute of temporalEntity having "BAD
	 * REQUEST"
	 */
/*
 * 
 * @Test public void deleteTemporalEntityByAttrBadRequest() { try {
 * Mockito.doThrow(new
 * ResponseException(ErrorType.BadRequestData)).when(historyService).delete(any(
 * ), any(), any(), any(), any()); mockMvc.perform( MockMvcRequestBuilders
 * .delete("/ngsi-ld/v1/temporal/entities/{entityId}/attrs/{attrId}",
 * "urn:ngsi-ld:testunit:151", "airQualityLevel")
 * .contentType(AppConstants.NGB_APPLICATION_JSONLD))
 * .andExpect(status().isBadRequest()); } catch (Exception e) {
 * Assert.fail(e.getMessage()); } }
 * 
 *//**
	 * this method is used to delete the temporal entity having "Resource not
	 * found".
	 */
/*
 * 
 * @Test public void deleteTemporalEntityByAttrResourceNotFound() { try {
 * Mockito.doThrow(new
 * ResponseException(ErrorType.NotFound)).when(historyService).delete(any(),
 * any(), any(), any(), any()); mockMvc.perform( MockMvcRequestBuilders
 * .delete("/ngsi-ld/v1/temporal/entities/{entityId}/attrs/{attrId}",
 * "urn:ngsi-ld:testunit:151", "airQualityLevel")
 * .contentType(AppConstants.NGB_APPLICATION_JSONLD))
 * .andExpect(status().isNotFound()); } catch (Exception e) {
 * Assert.fail(e.getMessage()); } }
 * 
 *//**
	 * this method is used get the temporalEntity by ID.
	 */
/*
 * 
 * @Test public void getTemporalEntityById() { try {
 * 
 * mockMvc.perform(get("/ngsi-ld/v1/temporal/entities/{entityId}",
 * "urn:ngsi-ld:testunit:151")
 * .accept(AppConstants.NGB_APPLICATION_JSON)).andExpect(status().isOk()); }
 * catch (Exception e) { Assert.fail(e.getMessage()); } }
 * 
 *//**
	 * this method is used get the temporalEntity by time filter.
	 *//*
		 * 
		 * @Test public void getTemporalEntityByTimefilter() { try { mockMvc.perform(
		 * get("/ngsi-ld/v1/temporal/entities/2018-08-07T12:00:00Z").accept(AppConstants
		 * .NGB_APPLICATION_JSON)) .andExpect(status().isOk()).andDo(print());
		 * 
		 * } catch (Exception e) { Assert.fail(e.getMessage()); } } }
		 */