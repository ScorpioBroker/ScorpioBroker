package eu.neclab.ngsildbroker.subscriptionmanager.controller;

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response.Status;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.response.ExtractableResponse;
import io.restassured.response.Response;

@QuarkusTest
@TestMethodOrder(OrderAnnotation.class)
@TestProfile(CustomProfile.class)
public class SubscriptionControllerTest {

	private String subscriptionEntityPayload;
	private String subscriptionIdPayloadBadRequest;
	private String subscriptionTypePayloadBadRequestt;
	private String subscripttionNotFoundPayload;

	@BeforeEach
	public void setup() throws Exception {

		// @formatter:off
		  
		  subscriptionEntityPayload = "{" +
		  "\r\n\"id\": \"urn:ngsi-ld:Subscription:c011\"," +
		  "\r\n\"type\": \"Subscription\"," + "\r\n\"entities\": [{" +
		  "\r\n		  \"id\": \"urn:ngsi-ld:Vehicle:a034\"," +
		  "\r\n		  \"type\": \"Vehicle\"" + "\r\n		}]," +
		  "\r\n\"watchedAttributes\": [\"brandName\"]," +
		  "\r\n		\"q\":\"brandName!=Mercedes\"," + "\r\n\"notification\": {" +
		  "\r\n  \"attributes\": [\"brandName\"]," + "\r\n  \"format\": \"keyValues\","
		  + "\r\n  \"endpoint\": {" +
		  "\r\n   \"uri\": \"mqtt://localhost:1883/notify\"," +
		  "\r\n   \"accept\": \"application/json\"," + "\r\n	\"notifierinfo\": {" +
		  "\r\n	  \"version\" : \"mqtt5.0\"," + "\r\n	  \"qos\" : 0" + "\r\n	}" +
		  "\r\n  }" + "\r\n}" + "\r\n}";
		  
		  
		  subscriptionIdPayloadBadRequest="{\r\n"
						  		+ "   \"id\":\"\",\r\n"
						  		+ "   \"type\":\"Subscription\",\r\n"
						  		+ "   \"entities\":[\r\n"
						  		+ "      {\r\n"
						  		+ "         \"type\":\"Building\"\r\n"
						  		+ "      }\r\n"
						  		+ "   ],\r\n"
						  		+ "   \"notification\":{\r\n"
						  		+ "      \"format\":\"keyValues\",\r\n"
						  		+ "      \"endpoint\":{\r\n"
						  		+ "         \"uri\":\"http://my.endpoint.org/notify\",\r\n"
						  		+ "         \"accept\":\"application/json\"\r\n"
						  		+ "      }\r\n"
						  		+ "   }\r\n"
						  		+ "}";
		  
		  subscriptionTypePayloadBadRequestt="{\r\n"
										  		+ "   \"id\":\"urn:ngsi-ld:Subscription:v01\",\r\n"
										  		+ "   \"type\":\"Subscription1\",\r\n"
										  		+ "   \"entities\":[\r\n"
										  		+ "      {\r\n"
										  		+ "         \"type\":\"Building\"\r\n"
										  		+ "      }\r\n"
										  		+ "   ],\r\n"
										  		+ "   \"notification\":{\r\n"
										  		+ "      \"format\":\"keyValues\",\r\n"
										  		+ "      \"endpoint\":{\r\n"
										  		+ "         \"uri\":\"http://my.endpoint.org/notify\",\r\n"
										  		+ "         \"accept\":\"application/json\"\r\n"
										  		+ "      }\r\n"
										  		+ "   }\r\n"
										  		+ "}";
														 
		  
		  subscripttionNotFoundPayload  ="\r\n"
								  		+ "{\r\n"
								  		+ "    \"entities\": [\r\n"
								  		+ "        {\r\n"
								  		+ "            \"id\": \"urn:ngsi-ld:Subscription:v01\",\r\n"
								  		+ "            \"type\": \"Building\"\r\n"
								  		+ "        }\r\n"
								  		+ "    ],\r\n"
								  		+ "    \"watchedAttributes\": [\r\n"
								  		+ "        \"brandName2\"\r\n"
								  		+ "    ],\r\n"
								  		+ "    \"notification\": {\r\n"
								  		+ "        \"attributes\": [\r\n"
								  		+ "            \"brandNam2\"\r\n"
								  		+ "        ],\r\n"
								  		+ "        \"format\": \"keyValues\",\r\n"
								  		+ "        \"endpoint\": {\r\n"
								  		+ "            \"uri\": \"tcp://localhost:1883/notify\",\r\n"
								  		+ "            \"accept\": \"application/json\",\r\n"
								  		+ "            \"notifierinfo\": {\r\n"
								  		+ "                \"version\": \"mqtt3.1.1\",\r\n"
								  		+ "                \"qos\": 0\r\n"
								  		+ "            }\r\n"
								  		+ "        }\r\n"
								  		+ "    }\r\n"
								  		+ "}";
								  
		  // @formatter:on
	}

	@AfterEach
	public void tearDown() {
		subscriptionEntityPayload = null;
		subscriptionIdPayloadBadRequest = null;
		subscriptionTypePayloadBadRequestt = null;
		subscripttionNotFoundPayload = null;

	}

	/**
	 * this method is use for create Registry subscription entity
	 */
	@Test
	@Order(1)
	public void createRegistrySubscriptionEntityTest() {

		try {
			ExtractableResponse<Response> response = given().body(subscriptionEntityPayload)
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
					.post("/ngsi-ld/v1/csourceSubscriptions").then().statusCode(Status.CREATED.getStatusCode())
					.statusCode(201).extract();
			assertEquals(201, response.statusCode());
		} catch (Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}

	}

	/**
	 * this method is use for Already exist Registry subscription entity
	 */
	@Test
	@Order(2)
	public void createSubscriptionEntityAlreadyExistTest() {
		try {
			ExtractableResponse<Response> response = given().body(subscriptionEntityPayload)
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
					.post("/ngsi-ld/v1/csourceSubscriptions").then().statusCode(Status.CONFLICT.getStatusCode())
					.statusCode(409).extract();

			assertEquals(409, response.statusCode());
		} catch (Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}

	}

	/**
	 * this method is test for create Registry subscription entity id , required
	 * BadRequest
	 */
	@Test
	@Order(3)
	public void createRegistrySubscriptionEntityIdNeedBadRequestTest() {
		try {
			ExtractableResponse<Response> response = given().body(subscriptionIdPayloadBadRequest)
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
					.post("/ngsi-ld/v1/csourceSubscriptions").then().statusCode(Status.BAD_REQUEST.getStatusCode())
					.statusCode(400).extract();
			assertEquals(400, response.statusCode());
		} catch (Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}
	}

	/**
	 * this method is test for create Registry subscription entity type required
	 * BadRequest
	 */
	@Test
	@Order(4)
	public void createRegistrySubscriptionEntityTypeBadRequestTest() {
		try {
			ExtractableResponse<Response> response = given().body(subscriptionTypePayloadBadRequestt)
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
					.post("/ngsi-ld/v1/csourceSubscriptions").then().statusCode(Status.BAD_REQUEST.getStatusCode())
					.statusCode(400).extract();
			assertEquals(400, response.statusCode());
		} catch (Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}
	}

	/**
	 * this method is use to get rgistry subscribe entity by id
	 */

	@Test
	@Order(5)
	public void getRegistrySubscriptionEntityByIdTest() {
		try {
			ExtractableResponse<Response> response = given()
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
					.get("/ngsi-ld/v1/csourceSubscriptions/urn:ngsi-ld:Subscription:c011").then()
					.statusCode(Status.OK.getStatusCode()).statusCode(200).extract();
			assertEquals(200, response.statusCode());
		} catch (Exception e) {
			Assertions.fail();
		}

	}

	/**
	 * this method is use to get rgistry subscribe entity by id with two parameter
	 */
	@Test
	@Order(6)
	public void getRegistrySubscriptionEntityByIdwithTest() throws Exception {
		ExtractableResponse<Response> response = given()
				.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
				.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
				.get("/ngsi-ld/v1/csourceSubscriptions/{id}", "urn:ngsi-ld:Subscription:c011").then()
				.statusCode(Status.OK.getStatusCode()).statusCode(200).extract();
		int statusCode = response.statusCode();
		assertEquals(200, statusCode);

	}

	/**
	 * this method is use for validate Not Found
	 */
	@Test
	@Order(7)
	public void getRegistrySubscriptionEntityByIdNotFoundTest() throws Exception {
		try {
			ExtractableResponse<Response> response = given()
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
					.get("/ngsi-ld/v1/csourceSubscriptions/{id}", "urn:ngsi-ld:Subscription:0a").then()
					.statusCode(Status.NOT_FOUND.getStatusCode()).statusCode(404).extract();
			assertEquals(404, response.statusCode());
		} catch (Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}
	}

	/**
	 * this method is use for validate BadRequest
	 */
	@Test
	@Order(8)
	public void getRegistrySubscriptionEntityByIdBadRequestTest() throws Exception {
		try {
			ExtractableResponse<Response> response = given()
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
					.get("/ngsi-ld/v1/csourceSubscriptions/{id}", ":ngsi-ld:Subscription:0a").then()
					.statusCode(Status.BAD_REQUEST.getStatusCode()).statusCode(400).extract();
			assertEquals(400, response.statusCode());
		} catch (Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}
	}

	/**
	 * this method is update the registry subscription entity
	 */
	@Test
	@Order(9)
	public void updateRegistrySubscriptionTest() {
		try {
			ExtractableResponse<Response> response = given().body(subscriptionEntityPayload)
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
					.patch("/ngsi-ld/v1/csourceSubscriptions/{id}", "urn:ngsi-ld:Subscription:211").then()
					.statusCode(Status.NO_CONTENT.getStatusCode()).statusCode(204).extract();
			assertEquals(204, response.statusCode());
			assertNotEquals(201, response.statusCode());
		} catch (Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}
	}

	/**
	 * this method is use for validate Update Registry subscription Not Found
	 */
	@Test
	@Order(10)
	public void updateRegistrySubscriptionNotFoundTest() {
		try {
			ExtractableResponse<Response> response = given().body(subscripttionNotFoundPayload)
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
					.patch("/ngsi-ld/v1/csourceSubscriptions/{id}", "urn:ngsi-ld:Subscription:0a").then()
					.statusCode(Status.NOT_FOUND.getStatusCode()).statusCode(404).extract();
			assertEquals(404, response.statusCode());
			assertNotEquals(400, response.statusCode());
		} catch (Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}
	}

	/**
	 * this method is use for validate Update Registry subscription BadRequest Data
	 */
	@Test
	@Order(11)
	public void updateRegistrySubscriptionBadRequestTest() {
		try {
			ExtractableResponse<Response> response = given().body(subscriptionEntityPayload)
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
					.patch("/ngsi-ld/v1/csourceSubscriptions/{id}", "urn:ngsi-ld:Subscription:a01a").then()
					.statusCode(Status.BAD_REQUEST.getStatusCode()).statusCode(400).extract();
			assertEquals(400, response.statusCode());
			assertNotEquals(201, response.statusCode());
		} catch (Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}
	}

	/*
	 * this method is use to get All registry subscription entity
	 */

	@Test
	@Order(12)
	public void getAllRegistrySubscriptionTest() {
		try {
			ExtractableResponse<Response> response = given()
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
					.get("/ngsi-ld/v1/csourceSubscriptions/").then().statusCode(Status.OK.getStatusCode())
					.statusCode(200).extract();
			assertEquals(200, response.statusCode());
			assertNotEquals(400, response.statusCode());
		} catch (Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}
	}

	/*
	 * this method is use for validate BadRequest
	 */
	@Test
	@Order(13)
	public void getAllRegistrySubscriptionBadRequestTest() {
		try {
			ExtractableResponse<Response> response = given()
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
					.get("/ngsi-ld/v1/csourceSubscriptions?limit=-1").then()
					.statusCode(Status.BAD_REQUEST.getStatusCode()).statusCode(400).extract();
			int statusCode = response.statusCode();
			assertEquals(400, statusCode);
			assertNotEquals(200, response.statusCode());
		} catch (Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}
	}

	/*
	 * this method is use for validate Forbidden
	 */
	@Test
	@Order(14)
	public void getAllRegistrySubscriptionForbiddenTest() {
		try {
			ExtractableResponse<Response> response = given()
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
					.get("/ngsi-ld/v1/csourceSubscriptions?limit=10000").then()
					.statusCode(Status.FORBIDDEN.getStatusCode()).statusCode(403).extract();
			assertEquals(403, response.statusCode());
			assertNotEquals(200, response.statusCode());
		} catch (Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}
	}

	/**
	 * this method is try to delete the registry subscription entity by ID
	 */
	@Test
	@Order(15)
	public void deleteSubscriptionByIdTest() {
		try {
			ExtractableResponse<Response> response = given().body(subscriptionEntityPayload)
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
					.delete("/ngsi-ld/v1/csourceSubscriptions/{id}", "urn:ngsi-ld:Subscription:211").then()
					.statusCode(Status.NO_CONTENT.getStatusCode()).statusCode(204).extract();
			assertEquals(204, response.statusCode());
			assertNotEquals(400, response.statusCode());
		} catch (Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}

	}

	/**
	 * this method is use for validate Delete Id Not Found
	 */
	@Test
	@Order(16)
	public void deleteSubscriptionByIdNotFoundTest() {
		try {
			ExtractableResponse<Response> response = given().body(subscriptionEntityPayload)
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
					.delete("/ngsi-ld/v1/csourceSubscriptions/{id}", "urn:ngsi-ld:Subscription:a0a").then()
					.statusCode(Status.NOT_FOUND.getStatusCode()).statusCode(404).extract();
			assertEquals(404, response.statusCode());
			assertNotEquals(400, response.statusCode());
		} catch (Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}

	}

	/**
	 * this method is use for validate Delete BadRequest Data
	 */
	@Test
	@Order(17)
	public void deleteSubscriptionByBadRequestTest() {
		try {
			ExtractableResponse<Response> response = given().body(subscriptionEntityPayload)
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
					.delete("/ngsi-ld/v1/csourceSubscriptions/{id}", ":ngsi-ld:Subscription:a0a").then()
					.statusCode(Status.BAD_REQUEST.getStatusCode()).statusCode(400).extract();
			assertEquals(400, response.statusCode());
			assertNotEquals(200, response.statusCode());
		} catch (Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}

	}

}
