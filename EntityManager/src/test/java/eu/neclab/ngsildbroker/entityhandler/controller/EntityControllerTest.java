package eu.neclab.ngsildbroker.entityhandler.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response.Status;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.entityhandler.services.EntityService;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.RestAssured;
import io.restassured.response.ExtractableResponse;
import io.restassured.response.Response;

@QuarkusTest
@TestProfile(CustomProfile.class)
@TestMethodOrder(OrderAnnotation.class)
public class EntityControllerTest {

	// @InjectMock
	EntityService entityService;

	private String appendPayload;
	private String invalidAppendPayload;
//	private String updatePayload;
	private String entityPayload;
	private String partialUpdatePayload;
	private String partialUpdateDefaultCasePayload;
//	private String payload;
	private String incorrectPayload;
	private String incorrectAppendPayload;
	private String invalidPartialUpdateDefaultCasePayload;
	private String InvalidPartialUpdatePayload;

	@BeforeEach
	public void setup() throws Exception { //@formatter:off
		//entityService = Mockito.mock(EntityService.class);
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
		 
	
		 incorrectPayload = "{\r\n    \"ids\": \"urn:ngsi-ld:testunit:154\",\r\n    \"type\": \"Vehicle\",\r\n    \"brandName\": [\r\n        {\r\n              \r\n            \"type\": \"Property\",\r\n            \"value\": \"good\",\r\n            \"observedAt\": \"2018-08-07T12:00:00Z\"\r\n        }\r\n    ]\r\n}";
         appendPayload= "{\r\n" + "	\"brandName1\": {\r\n" +
				  "		\"type\": \"Property\",\r\n" + "		\"value\": \"BMW\"\r\n" +
				  "	}\r\n" + "}";
         invalidAppendPayload= "{\r\n" + "	\"brandName1\": {\r\n" +
		  "		\"type\": \"property\",\r\n" + "		\"value\": \"BMW\"\r\n" +
		  "	}\r\n" + "}";
         incorrectAppendPayload= "{\r\n" + "	\"brandName123\": {\r\n" +
		  "		\"type\": \"Property\",\r\n" + "		\"value\": \"BMW\"\r\n" +
		  "	}\r\n" + "}";
//				  
//  updatePayload="{\r\n" + "	\"brandName1\": {\r\n" +
//				  "		\"type\": \"Property\",\r\n" + "		\"value\": \"Audi\"\r\n" +
//				  "	}\r\n" + "}"; 
// 
         partialUpdatePayload= "{\r\n" + "		\"value\": 20,\r\n"
				  + "		\"datasetId\": \"urn:ngsi-ld:Property:speedometerA4567-speed\"\r\n"
				  + "}";
         InvalidPartialUpdatePayload= "{\r\n" + "		\"value\": 20,\r\n"
		  + "		\"datasetId\": \"urn:ngsi-ld:Property:InvalidspeedometerA4567-speed\"\r\n"
		  + "}";
			  
         partialUpdateDefaultCasePayload= "{\r\n" + "		\"value\": 11\r\n" + "}";
         invalidPartialUpdateDefaultCasePayload= "{\r\n" + "		\"valueInvalid\": 11\r\n" + "}";

	}


	/**
	 * this method is use for create the entity
	 */

	@Test
	@Order(1)
	public void createEntityTest() {
		
//		Mockito.when(entityService.createEntry(any(), any()))
//				.thenReturn((Uni.createFrom().item(new CreateResult("urn:ngsi-ld:Vehicle:A101", true))));
		try
		{
			ExtractableResponse<Response> response = RestAssured.given()
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD)
					.body(entityPayload).when()
					.post("/ngsi-ld/v1/entities").then()
					.statusCode(201).statusCode(201).extract();
			int statusCode = response.statusCode();
			assertEquals(201, statusCode);
		} catch (Exception e) {
			e.printStackTrace();		
		}

	}


	/**
	 * this method is use for the entity if entity already exist
	 */

	@Test
	@Order(2)
	public void createEntityAlreadyExistTest() {
		try {
			ExtractableResponse<Response> response = RestAssured.given()
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD)
					.body(entityPayload).when()
					.post("/ngsi-ld/v1/entities").then()
					.statusCode(409).statusCode(409).extract();
			int statusCode = response.statusCode();
			assertEquals(409, statusCode);

		} catch (Exception e) {
			e.printStackTrace();		
		}
	}

//
//	/**
//	 * this method is validate for the bad request if create the entity
//	 */
//
	@Test
	@Order(3)
	public void createEntityBadRequestTest() {
		try {
			ExtractableResponse<Response> response = RestAssured.given()
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD)
					.body(incorrectPayload).when()
					.post("/ngsi-ld/v1/entities").then()
					.statusCode(400).statusCode(400).extract();
			int statusCode = response.statusCode();
			assertEquals(400, statusCode);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	/**
	 * this method is use for append the attribute in entity
	 */

	@Test
	@Order(4)
	public void appendEntityNoContentTest() {
		try {
			ExtractableResponse<Response> response = RestAssured.given()
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD)
					.body(appendPayload).when()
					.post("/ngsi-ld/v1/entities/{entityId}/attrs\", \"urn:ngsi-ld:Vehicle:A101").then()
					.statusCode(204).statusCode(204).extract();
			int statusCode = response.statusCode();
			assertEquals(204, statusCode);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	/**
	 * this method is use for append the attribute in entity for multi status
	 */

//	@Test
//	@Order(5)
//	public void appendEntityMultiStatusTest() throws Exception {
//		try {
//			ExtractableResponse<Response> response =    RestAssured.given()
//                    .header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
//                    .header(HttpHeaders.ACCEPT,AppConstants.NGB_APPLICATION_JSONLD)
//                    .body(appendPayload)
//                   .when()
//                   .post("/ngsi-ld/v1/entities/{entityId}/attrs", "urn:ngsi-ld:Vehicle:A101")
//                   .then()
//                      .statusCode(207)
//                      .statusCode(207).extract();
//                   int statusCode = response.statusCode();
//                   assertEquals(207, statusCode);
////			UpdateResult updateResult = Mockito.mock(UpdateResult.class);
////			when(entityService.appendToEntry(any(), any(), any(), any()))
////					.thenThrow(new ResponseException(ErrorType.MultiStatus, "Multi status result"));
////
////			ResultActions resultAction = mockMvc.perform(post("/ngsi-ld/v1/entities/{entityId}/attrs", "urn:ngsi-ld:Vehicle:A101")
////							.contentType(AppConstants.NGB_APPLICATION_JSON).accept(AppConstants.NGB_APPLICATION_JSONLD)
////							.content(appendPayload))
////					.andExpect(status().isMultiStatus());
////
////			MvcResult mvcResult = resultAction.andReturn();
////			MockHttpServletResponse res = mvcResult.getResponse();
////			int status = res.getStatus();
////			assertEquals(207, status);
////			verify(entityService, times(1)).appendToEntry(any(), any(), any(), any());
////
//		} catch (Exception e) {
//			//Assert.fail();
//			e.printStackTrace();
//		}
//	}
//
//	/**
//	 * this method is validate the bad request if append the attribute in entity
//	 */
//
	@Test
	@Order(6)
	public void appendEntityBadRequestTest() {
		try {
			ExtractableResponse<Response> response = RestAssured.given()
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).body(invalidAppendPayload).when()
					.post("/ngsi-ld/v1/entities/{entityId}/attrs", "urn:ngsi-ld:Vehicle:A101").then()
					.statusCode(Status.BAD_REQUEST.getStatusCode()).statusCode(400).extract();
			int statusCode = response.statusCode();
			assertEquals(400, statusCode);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	/**
	 * this method is validate the data not found if append the attribute in entity
	 */

	@Test
	@Order(7)
	public void appendEntityNotFoundTest() {
		try {
			ExtractableResponse<Response> response = RestAssured.given()
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).body(appendPayload).when()
					.post("/ngsi-ld/v1/entities/{entityId}/attrs", "urn:ngsi-ld:VehicleInvalid:A101").then()
					.statusCode(Status.NOT_FOUND.getStatusCode()).statusCode(404).extract();
			int statusCode = response.statusCode();
			assertEquals(404, statusCode);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	/**
	 * this method is use for update the entity
	 */

//	@Test
//	@Order(8)
//	public void updateEntityNoContentTest() {
//		try {
//			ExtractableResponse<Response> response = RestAssured.given()
//					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
//					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).body(appendPayload).when()
//					.patch("/ngsi-ld/v1/entities/{entityId}/attrs", "urn:ngsi-ld:Vehicle:A101").then()
//					.statusCode(Status.NO_CONTENT.getStatusCode()).statusCode(204).extract();
//			int statusCode = response.statusCode();
//			assertEquals(204, statusCode);
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//	}

//
//	/**
//	 * this method is use for update the entity for multi status
//	 */
////
//	@Test
//	@Order(9)
//	public void updateEntityMultiStatusTest() {
//		try {
//			ExtractableResponse<Response> response =    RestAssured.given()
//                    .header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
//                    .header(HttpHeaders.ACCEPT,AppConstants.NGB_APPLICATION_JSONLD)
//                    .body(appendPayload)
//                   .when()
//                   .patch("/ngsi-ld/v1/entities/{entityId}/attrs", "urn:ngsi-ld:Vehicle:A101")
//                   .then()
//                      .statusCode(207)
//                      .statusCode(207).extract();
//                   int statusCode = response.statusCode();
//                   assertEquals(207, statusCode);
////
////			when(entityService.updateEntry(any(), any(), any()))
////					.thenThrow(new ResponseException(ErrorType.MultiStatus, "Multi status result"));
////			;
////
////			ResultActions resultAction = mockMvc.perform(patch("/ngsi-ld/v1/entities/{entityId}/attrs", "urn:ngsi-ld:Vehicle:A101")
////							.contentType(AppConstants.NGB_APPLICATION_JSON).accept(AppConstants.NGB_APPLICATION_JSONLD)
////							.content(updatePayload))
////					.andExpect(status().isMultiStatus());
////
////			MvcResult mvcResult = resultAction.andReturn();
////			MockHttpServletResponse res = mvcResult.getResponse();
////			int status = res.getStatus();
////			assertEquals(207, status);
////
//		} catch (Exception e) {
//			//Assert.fail();
//			e.printStackTrace();
//		}
//	}
//
//	/**
//	 * this method is validate the bad request if entity update
//	 */
//
	@Test
	@Order(10)
	public void updateEntityBadRequestTest() {
		try {
			ExtractableResponse<Response> response = RestAssured.given()
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).body(invalidAppendPayload).when()
					.patch("/ngsi-ld/v1/entities/{entityId}/attrs", "urn:ngsi-ld:Vehicle:A101").then()
					.statusCode(Status.BAD_REQUEST.getStatusCode()).statusCode(400).extract();
			int statusCode = response.statusCode();
			assertEquals(400, statusCode);
		} catch (Exception e) {
		}
	}


	/**
	 * this method is validate the data not found if entity update
	 */

	@Test
	@Order(11)
	public void updateEntityNotFoundTest() {
		try {
			ExtractableResponse<Response> response = RestAssured.given()
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).body(incorrectAppendPayload).when()
					.patch("/ngsi-ld/v1/entities/{entityId}/attrs", "urn:ngsi-ld:VehicleInvalid:A101").then()
					.statusCode(Status.NOT_FOUND.getStatusCode()).statusCode(404).extract();
			int statusCode = response.statusCode();
			assertEquals(404, statusCode);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	/**
	 * this method is use for partial update the attribute
	 */

	@Test
	@Order(12)
	public void partialUpdateAttributeIfDatasetIdExistNoContentTest() {
		try {
			ExtractableResponse<Response> response = RestAssured.given()
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).body(partialUpdatePayload).when()
					.patch("/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}", "urn:ngsi-ld:Vehicle:A101", "speed").then()
					.statusCode(Status.NO_CONTENT.getStatusCode()).statusCode(204).extract();
			int statusCode = response.statusCode();
			assertEquals(204, statusCode);
		} catch (Exception e) {
			
			e.printStackTrace();
		}
	}


	/**
	 * this method is use for partial update the attribute if datasetId is not exist
	 */

	@Test
	@Order(13)
	public void partialUpdateAttributeIfDatasetIdIsNotExistTest() {
		try {
			ExtractableResponse<Response> response = RestAssured.given()
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).body(partialUpdatePayload).when()
					.patch("/ngsi-ld/v1/entitiess/{entityId}/attrs/{attrId}", "urn:ngsi-ld:Vehicle:A101", "speed")
					.then().statusCode(Status.NOT_FOUND.getStatusCode()).statusCode(404).extract();
			int statusCode = response.statusCode();
			assertEquals(404, statusCode);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	/**
	 * this method is validate the bad request in partial update attribute
	 */

	@Test
	@Order(14)
	public void partialUpdateAttributeBadRequestTest() {
		try {
			ExtractableResponse<Response> response = RestAssured.given()
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).body(appendPayload).when()
					.patch("/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}", "urn:ngsi-ld:Vehicle:A101", "speed").then()
					.statusCode(Status.BAD_REQUEST.getStatusCode()).statusCode(400).extract();
			int statusCode = response.statusCode();
			assertEquals(400, statusCode);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	/**
	 * this method is use for partial update attribute default datasetId
	 */

	@Test
	@Order(15)
	public void partialUpdateAttributeDefaultDatasetIdCaseTest() {
		try {
			ExtractableResponse<Response> response = RestAssured.given()
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD)
					.body(partialUpdateDefaultCasePayload).when()
					.patch("/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}", "urn:ngsi-ld:Vehicle:A101", "speed").then()
					.statusCode(Status.NO_CONTENT.getStatusCode()).statusCode(204).extract();
			int statusCode = response.statusCode();
			assertEquals(204, statusCode);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	/**
	 * this method is validate for the not found in partial update attribute default
	 * datasetId
	 */

	@Test
	@Order(16)
	public void partialUpdateAttributeDefaultDatasetIdCaseNotFoundTest() {
		try {
			ExtractableResponse<Response> response = RestAssured.given()
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).body(InvalidPartialUpdatePayload)
					.when().patch("/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}", "urn:ngsi-ld:Vehicle:A101", "speed")
					.then().statusCode(Status.NOT_FOUND.getStatusCode()).statusCode(404).extract();
			int statusCode = response.statusCode();
			assertEquals(404, statusCode);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	/**
	 * this method is use for delete the entity
	 */

	@Test
	@Order(17)
	public void deleteEntityTest() {
		try {

			RestAssured.given().accept(AppConstants.NGB_APPLICATION_JSONLD).request()
					.contentType(AppConstants.NGB_APPLICATION_JSON).when()
					.delete("/ngsi-ld/v1/entities/{entityId}", "urn:ngsi-ld:Vehicle:A101").then().statusCode(204);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	/**
	 * this method is validate the not found if delete the entity
	 */

	@Test
	@Order(18)
	public void deleteEntityNotFoundTest() {
		try {
			ExtractableResponse<Response> response = RestAssured.given()
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).body(appendPayload).when()
					.delete("/ngsi-ld/v1/entities/{entityId}", "urn:ngsi-ld:Vehicle:A101").then()
					.statusCode(Status.NOT_FOUND.getStatusCode()).statusCode(404).extract();
			int statusCode = response.statusCode();
			assertEquals(404, statusCode);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

//
//	/**
//	 * this method is validate the bad request if delete the entity
//	 */
//
//	@Test
//	@Order(19)
//	public void deleteEntityBadRequestTest() {
//		try {
//			ExtractableResponse<Response> response =    RestAssured.given()
//                    .header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
//                    .header(HttpHeaders.ACCEPT,AppConstants.NGB_APPLICATION_JSONLD)
//                    
//                   .when()
//                   .delete("/ngsi-ld/v1/entities/{entityId}", "test:urn:ngsi-ld:Vehicle:A101")
//                   .then()
//                      .statusCode(Status.BAD_REQUEST.getStatusCode())
//                      .statusCode(400).extract();
//                   int statusCode = response.statusCode();
//                   assertEquals(400, statusCode);
////			when(entityService.deleteEntry(any(), any()))
////					.thenThrow(new ResponseException(ErrorType.BadRequestData, "Bad Request Data."));
////			ResultActions resultAction = mockMvc.perform(delete("/ngsi-ld/v1/entities/{entityId}", "urn:ngsi-ld:Vehicle:A101")
////							.contentType(AppConstants.NGB_APPLICATION_JSONLD))
////					.andExpect(status().isBadRequest()).andExpect(jsonPath("$.title").value("Bad Request Data."));
////			MvcResult mvcResult = resultAction.andReturn();
////			MockHttpServletResponse res = mvcResult.getResponse();
////			int status = res.getStatus();
////			assertEquals(400, status);
////
////			verify(entityService, times(1)).deleteEntry(any(), any());
//		} catch (Exception e) {
//			//Assert.fail();
//		}
//	}
//
//	/**
//	 * this method is validate for the datasetId is exist in case of delete
//	 * attribute instance
//	 */
//
//	@Test
//	@Order(20)
//	public void deleteAttributeInstanceIfDatasetIdExistTest() {
//		try {
//			ExtractableResponse<Response> response =    RestAssured.given()
//                    .header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
//                    .header(HttpHeaders.ACCEPT,AppConstants.NGB_APPLICATION_JSONLD)
//                    .body(appendPayload)
//                   .when()
//                   .delete("/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}?datasetId=urn:ngsi-ld:Property:speedometerA4567-speed",
//       					"urn:ngsi-ld:Vehicle:A101", "speed")
//                   .then()
//                      .statusCode(Status.NO_CONTENT.getStatusCode())
//                      .statusCode(204).extract();
//                   int statusCode = response.statusCode();
//                   assertEquals(204, statusCode);
////			when(entityService.deleteAttribute(any(), any(), any(), any(), any())).thenReturn(true);
////			ResultActions resultAction = mockMvc.perform(delete(
////					"/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}?datasetId=urn:ngsi-ld:Property:speedometerA4567-speed",
////					"urn:ngsi-ld:Vehicle:A101", "speed").with(request -> {
////						request.setServletPath(
////								"/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}?datasetId=urn:ngsi-ld:Property:speedometerA4567-speed\",\r\n"
////										+ "					\"urn:ngsi-ld:Vehicle:A101\", \"speed\"");
////						return request;
////					}).contentType(AppConstants.NGB_APPLICATION_JSONLD)).andExpect(status().isNoContent());
////			MvcResult mvcResult = resultAction.andReturn();
////			MockHttpServletResponse res = mvcResult.getResponse();
////			int status = res.getStatus();
////			assertEquals(204, status);
////			verify(entityService, times(1)).deleteAttribute(any(), any(), any(), any(), any());
//		} catch (Exception e) {
//		//	Assert.fail();
//			e.printStackTrace();
//		}
//	}
//
//	/**
//	 * this method is validate for the datasetId is not exist in case of delete
//	 * attribute instance
//	 */
//
	@Test
	@Order(21)
	public void deleteAttributeInstanceIfDatasetIdNotExistTest() {
		try {
			ExtractableResponse<Response> response = RestAssured.given()
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).body(appendPayload).when()
					.delete("/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}?datasetId=urn:ngsi-ld:Property:speedometerA4567-speed",
							"urn:ngsi-ld:Vehicle:A101", "speed")
					.then().statusCode(Status.NOT_FOUND.getStatusCode()).statusCode(404).extract();
			int statusCode = response.statusCode();
			assertEquals(404, statusCode);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

//
//	/**
//	 * this method is validate for bad request in case of delete attribute
//	 */
//
//	@Test
//	@Order(22)
//	public void deleteAttributeInstanceBadRequestTest() {
//		try {
//			ExtractableResponse<Response> response =    RestAssured.given()
//                    .header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
//                    .header(HttpHeaders.ACCEPT,AppConstants.NGB_APPLICATION_JSONLD)
//                    .body(appendPayload)
//                   .when()
//                   .delete("/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}?datasetId=urn:ngsi-ld:Property:speedometerA4567-speed",
//       					"urn:ngsi-ld:Vehicle:A101", "speedd")
//                   .then()
//                      .statusCode(Status.BAD_REQUEST.getStatusCode())
//                      .statusCode(400).extract();
//                   int statusCode = response.statusCode();
//                   assertEquals(400, statusCode);
//			
////			when(entityService.deleteAttribute(any(), any(), any(), any(), any()))
////					.thenThrow(new ResponseException(ErrorType.BadRequestData, "Bad Request Data."));
////			ResultActions resultAction = mockMvc.perform(delete(
////					"/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}?datasetId=urn:ngsi-ld:Property:speedometerA4567-speed",
////					"urn:ngsi-ld:Vehicle:A101", "speed").with(request -> {
////						request.setServletPath(
////								"/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}?datasetId=urn:ngsi-ld:Property:speedometerA4567-speed\",\r\n"
////										+ "					\"urn:ngsi-ld:Vehicle:A101\", \"speed\"");
////						return request;
////					}).contentType(AppConstants.NGB_APPLICATION_JSONLD))
////					.andExpect(jsonPath("$.title").value("Bad Request Data."));
////			MvcResult mvcResult = resultAction.andReturn();
////			MockHttpServletResponse res = mvcResult.getResponse();
////			int status = res.getStatus();
////			assertEquals(400, status);
////			verify(entityService, times(1)).deleteAttribute(any(), any(), any(), any(), any());
//		} catch (Exception e) {
//		//	Assert.fail();
//		}
//	}
//
	/**
	 * this method is validate not found in case of delete attribute
	 */

	@Test
	@Order(23)
	public void deleteAttributeInstanceNotFoundTest() {
		try {
			ExtractableResponse<Response> response = RestAssured.given()
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).body(appendPayload).when()
					.delete("/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}", "urn:ngsi-ld:Vehicle:A101", "speed")
					.then().statusCode(Status.NOT_FOUND.getStatusCode()).statusCode(404).extract();
			int statusCode = response.statusCode();
			assertEquals(404, statusCode);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
//
//	/**
//	 * this method is use for all the delete attribute
//	 */
//
//	@Test
//	@Order(24)
//	public void deleteAllAttributeInstanceTest() {
//		try {
//			ExtractableResponse<Response> response =    RestAssured.given()
//                    .header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
//                    .header(HttpHeaders.ACCEPT,AppConstants.NGB_APPLICATION_JSONLD)
//                    .body(appendPayload)
//                   .when()
//                   .delete("/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}?deleteAll=true",
//							"urn:ngsi-ld:Vehicle:A101", "speed")
//                   .then()
//                      .statusCode(Status.NO_CONTENT.getStatusCode())
//                      .statusCode(204).extract();
//                   int statusCode = response.statusCode();
//                   assertEquals(204, statusCode);
////			when(entityService.deleteAttribute(any(), any(), any(), any(), any())).thenReturn(true);
////			ResultActions resultAction = mockMvc.perform(delete("/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}?deleteAll=true",
////							"urn:ngsi-ld:Vehicle:A101", "speed").with(request -> {
////								request.setServletPath(
////										"\"/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}?deleteAll=true\",\r\n"
////												+ "				 	\"urn:ngsi-ld:Vehicle:A101\", \"speed\"");
////								return request;
////							}).contentType(AppConstants.NGB_APPLICATION_JSONLD))
////					.andExpect(status().isNoContent());
////			MvcResult mvcResult = resultAction.andReturn();
////			MockHttpServletResponse res = mvcResult.getResponse();
////			int status = res.getStatus();
////			assertEquals(204, status);
////
////			verify(entityService, times(1)).deleteAttribute(any(), any(), any(), any(), any());
//		} catch (Exception e) {
//			//Assert.fail();
//			e.printStackTrace();
//		}
//	}
////
////	/**
////	 * this method is validate the bad request in case of delete all attribute
////	 */
////
//	@Test
//	@Order(25)
//	public void deleteAllAttributeInstanceBadRequestTest() {
//		try {
//			ExtractableResponse<Response> response =    RestAssured.given()
//                    .header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
//                    .header(HttpHeaders.ACCEPT,AppConstants.NGB_APPLICATION_JSONLD)
//                    .body(appendPayload)
//                   .when()
//                   .delete("/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}?deleteAll=true",
//							"urn:ngsi-ld:Vehicle:A101", "speed")
//                   .then()
//                      .statusCode(Status.BAD_REQUEST.getStatusCode())
//                      .statusCode(400).extract();
//                   int statusCode = response.statusCode();
//                   assertEquals(400, statusCode);
////			when(entityService.deleteAttribute(any(), any(), any(), any(), any()))
////					.thenThrow(new ResponseException(ErrorType.BadRequestData, "Bad Request Data."));
////			ResultActions resultAction = mockMvc.perform(delete("/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}?deleteAll=true",
////							"urn:ngsi-ld:Vehicle:A101", "speed").with(request -> {
////								request.setServletPath(
////										"\"/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}?deleteAll=true\",\r\n"
////												+ "					\"urn:ngsi-ld:Vehicle:A101\", \"speed\"");
////								return request;
////							}).contentType(AppConstants.NGB_APPLICATION_JSONLD))
////					.andExpect(status().isBadRequest()).andExpect(jsonPath("$.title").value("Bad Request Data."));
////			MvcResult mvcResult = resultAction.andReturn();
////			MockHttpServletResponse res = mvcResult.getResponse();
////			int status = res.getStatus();
////			assertEquals(400, status);
////			verify(entityService, times(1)).deleteAttribute(any(), any(), any(), any(), any());
//		} catch (Exception e) {
//			//Assert.fail();
//		}
//	}
//
}
