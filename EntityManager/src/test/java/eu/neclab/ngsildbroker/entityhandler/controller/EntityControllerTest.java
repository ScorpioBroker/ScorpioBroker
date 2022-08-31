package eu.neclab.ngsildbroker.entityhandler.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response.Status;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
 import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.RestAssured;
import io.restassured.response.ExtractableResponse;
import io.restassured.response.Response;

@QuarkusTest
@TestProfile(CustomProfile.class)
@TestMethodOrder(OrderAnnotation.class)
public class EntityControllerTest {

	private String appendPayload;
	private String invalidAppendPayload;
	private String entityPayload;
	private String partialUpdatePayload;
	private String partialUpdateDefaultCasePayload;
	private String incorrectPayload;
	private String incorrectAppendPayload;
	private String InvalidPartialUpdatePayload;

	@BeforeEach
	public void setup() throws Exception { //@formatter:off
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
 
         partialUpdatePayload= "{\r\n" + "		\"value\": 20,\r\n"
				  + "		\"datasetId\": \"urn:ngsi-ld:Property:speedometerA4567-speed\"\r\n"
				  + "}";
         InvalidPartialUpdatePayload= "{\r\n" + "		\"value\": 20,\r\n"
		  + "		\"datasetId\": \"urn:ngsi-ld:Property:InvalidspeedometerA4567-speed\"\r\n"
		  + "}";
			  
         partialUpdateDefaultCasePayload= "{\r\n" + "		\"value\": 11\r\n" + "}";
 
	}

	@AfterEach
	public void tearDown() {
		 appendPayload=null;
		  invalidAppendPayload=null;
	 	  entityPayload=null;
		  partialUpdatePayload=null;
		  partialUpdateDefaultCasePayload=null;
	 	  incorrectPayload=null;
		  incorrectAppendPayload=null;
	 	  InvalidPartialUpdatePayload=null;

	}
	/**
	 * this method is use for create the entity
	 */

	@Test
	@Order(1)
	public void createEntityTest() {
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


	/**
	 * this method is validate for the bad request if create the entity
	 */
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
					.post("/ngsi-ld/v1/entities/{entityId}/attrs", "urn:ngsi-ld:Vehicle:A101").then()
					.statusCode(204).statusCode(204).extract();
			int statusCode = response.statusCode();
			assertEquals(204, statusCode);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

 
	/**
	 * this method is validate the bad request if append the attribute in entity
	 */

	@Test
	@Order(5)
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
	@Order(6)
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
	 * this method is validate the bad request if entity update
	 */

	@Test
	@Order(7)
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
	@Order(8)
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
	@Order(9)
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
	@Order(10)
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
	@Order(11)
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
	@Order(12)
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
	@Order(13)
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
	@Order(14)
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
	@Order(15)
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

 
	/**
	 * this method is validate for the datasetId is not exist in case of delete
	 * attribute instance
	 */

	@Test
	@Order(16)
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

 
	/**
	 * this method is validate not found in case of delete attribute
	 */

	@Test
	@Order(17)
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
 
}
