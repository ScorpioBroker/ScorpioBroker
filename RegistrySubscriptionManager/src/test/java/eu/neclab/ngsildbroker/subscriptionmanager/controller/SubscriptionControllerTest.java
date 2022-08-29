package eu.neclab.ngsildbroker.subscriptionmanager.controller;

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response.Status;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.mockito.MockitoAnnotations;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.registry.subscriptionmanager.service.RegistrySubscriptionService;
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
		  "\r\n\"id\": \"urn:ngsi-ld:Subscription:211\"," +
		  "\r\n\"type\": \"Subscription\"," + "\r\n\"entities\": [{" +
		  "\r\n		  \"id\": \"urn:ngsi-ld:Vehicle:A143\"," +
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
										  		+ "   \"id\":\"urn:ngsi-ld:Subscription:102\",\r\n"
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
								  		+ "            \"id\": \"urn:ngsi-ld:Subscription:102\",\r\n"
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
			int statusCode = response.statusCode();
			assertEquals(201, statusCode);
		} catch (Exception e) {
			System.out.println("SubscriptionControllerTestMy Test()  :: " + e);
			e.printStackTrace();
		}

	}

	@Test
	@Order(2)
	public void createSubscriptionEntityAlreadyExistTest() {
		try {
			ExtractableResponse<Response> response = given().body(subscriptionEntityPayload)
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
					.post("/ngsi-ld/v1/csourceSubscriptions").then().statusCode(Status.CONFLICT.getStatusCode())
					.statusCode(409).extract();
			int statusCode = response.statusCode();
			assertEquals(409, statusCode);
		} catch (Exception e) {
			System.out.println("SubscriptionControllerTestMy Test()  :: " + e);
			e.printStackTrace();
		}

	}

	@Test
	@Order(3)
	public void createRegistrySubscriptionEntityIdNeedBadRequestTest() {
		try {
			ExtractableResponse<Response> response = given().body(subscriptionIdPayloadBadRequest)
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
					.post("/ngsi-ld/v1/csourceSubscriptions").then().statusCode(Status.BAD_REQUEST.getStatusCode())
					.statusCode(400).extract();
			int statusCode = response.statusCode();
			assertEquals(400, statusCode);
		} catch (Exception e) {
			System.out.println("SubscriptionControllerTestMy Test()  :: " + e);
			e.printStackTrace();
		}
	}

	@Test
	@Order(4)
	public void createRegistrySubscriptionEntityTypeBadRequestTest() {
		try {
			ExtractableResponse<Response> response = given().body(subscriptionTypePayloadBadRequestt)
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
					.post("/ngsi-ld/v1/csourceSubscriptions").then().statusCode(Status.BAD_REQUEST.getStatusCode())
					.statusCode(400).extract();
			int statusCode = response.statusCode();
			assertEquals(400, statusCode);
		} catch (Exception e) {
			System.out.println("SubscriptionControllerTestMy Test()  :: " + e);
			e.printStackTrace();
		}
	}

	/**
	 * this method is use for the rgistry subscribe entity if Id Not Found
	 */

	@Test
	@Order(5)
	public void getRegistrySubscriptionEntityByIdTest() throws Exception {

		ExtractableResponse<Response> response = given()
				.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
				.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
				.get("/ngsi-ld/v1/csourceSubscriptions/urn:ngsi-ld:Subscription:211").then()
				.statusCode(Status.OK.getStatusCode()).statusCode(200).extract();
		int statusCode = response.statusCode();

	}

	@Test
	@Order(6)
	public void getRegistrySubscriptionEntityByIdwithTest() throws Exception {
		ExtractableResponse<Response> response = given()
				.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
				.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
				.get("/ngsi-ld/v1/csourceSubscriptions/{id}", "urn:ngsi-ld:Subscription:211").then()
				.statusCode(Status.OK.getStatusCode()).statusCode(200).extract();
		int statusCode = response.statusCode();
		assertEquals(200, statusCode);

	}

	@Test
	@Order(7)
	public void getRegistrySubscriptionEntityByIdNotFoundTest() throws Exception {
		ExtractableResponse<Response> response = given()
				.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
				.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
				.get("/ngsi-ld/v1/csourceSubscriptions/{id}", "urn:ngsi-ld:Subscription:0a").then()
				.statusCode(Status.NOT_FOUND.getStatusCode()).statusCode(404).extract();
		int statusCode = response.statusCode();
		assertEquals(404, statusCode);
	}

	@Test
	@Order(8)
	public void getRegistrySubscriptionEntityByIdBadRequestTest() throws Exception {
		ExtractableResponse<Response> response = given()
				.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
				.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
				.get("/ngsi-ld/v1/csourceSubscriptions/{id}", ":ngsi-ld:Subscription:0a").then()
				.statusCode(Status.BAD_REQUEST.getStatusCode()).statusCode(400).extract();
		int statusCode = response.statusCode();

	}

	/**
	 * this method is update the registry subscription entity
	 */

	@Test
	@Order(9)
	public void updateRegistrySubscriptionTest() {

		ExtractableResponse<Response> response = given().body(subscriptionEntityPayload)
				.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
				.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
				.patch("/ngsi-ld/v1/csourceSubscriptions/{id}", "urn:ngsi-ld:Subscription:211").then()
				.statusCode(Status.NO_CONTENT.getStatusCode()).statusCode(204).extract();
		int statusCode = response.statusCode();
		assertEquals(204, statusCode);
		assertNotEquals(201, response.statusCode());
	}

	@Test
	@Order(10)
	public void updateRegistrySubscriptionNotFoundTest() {
		ExtractableResponse<Response> response = given().body(subscripttionNotFoundPayload)
				.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
				.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
				.patch("/ngsi-ld/v1/csourceSubscriptions/{id}", "urn:ngsi-ld:Subscription:0a").then()
				.statusCode(Status.NOT_FOUND.getStatusCode()).statusCode(404).extract();
		int statusCode = response.statusCode();
		assertEquals(404, statusCode);
		assertNotEquals(400, response.statusCode());
	}

	@Test
	@Order(11)
	public void updateRegistrySubscriptionBadRequestTest() {
		ExtractableResponse<Response> response = given().body(subscriptionEntityPayload)
				.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
				.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
				.patch("/ngsi-ld/v1/csourceSubscriptions/{id}", "urn:ngsi-ld:Subscription:a01a").then()
				.statusCode(Status.BAD_REQUEST.getStatusCode()).statusCode(400).extract();
		int statusCode = response.statusCode();
		assertEquals(400, statusCode);
		assertNotEquals(201, response.statusCode());
	}

	/*
	 * this method is use to get All registry subscription entity
	 */

	@Test
	@Order(12)
	public void getAllRegistrySubscriptionTest() {
		ExtractableResponse<Response> response = given()
				.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
				.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
				.get("/ngsi-ld/v1/csourceSubscriptions/").then().statusCode(Status.OK.getStatusCode()).statusCode(200)
				.extract();
		int statusCode = response.statusCode();

		assertNotEquals(400, response.statusCode());
	}

	@Test
	@Order(13)
	public void getAllRegistrySubscriptionBadRequestTest() {
		ExtractableResponse<Response> response = given()
				.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
				.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
				.get("/ngsi-ld/v1/csourceSubscriptions?limit=-1").then().statusCode(Status.BAD_REQUEST.getStatusCode())
				.statusCode(400).extract();
		int statusCode = response.statusCode();
		assertEquals(400, statusCode);
		assertNotEquals(200, response.statusCode());
	}

	@Test
	@Order(14)
	public void getAllRegistrySubscriptionForbiddenTest() {
		ExtractableResponse<Response> response = given()
				.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
				.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
				.get("/ngsi-ld/v1/csourceSubscriptions?limit=10000").then().statusCode(Status.FORBIDDEN.getStatusCode())
				.statusCode(403).extract();
		int statusCode = response.statusCode();
		assertEquals(403, statusCode);
		assertNotEquals(200, response.statusCode());
	}

	/**
	 * this method is try to delete the registry subscription entity by ID
	 */
	@Test
	@Order(15)
	public void deleteSubscriptionByIdTest() throws Exception {

		ExtractableResponse<Response> response = given().body(subscriptionEntityPayload)
				.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
				.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
				.delete("/ngsi-ld/v1/csourceSubscriptions/{id}", "urn:ngsi-ld:Subscription:211").then()
				.statusCode(Status.NO_CONTENT.getStatusCode()).statusCode(204).extract();
		int statusCode = response.statusCode();
		assertEquals(204, statusCode);
		assertNotEquals(400, response.statusCode());

	}

	@Test
	@Order(16)
	public void deleteSubscriptionByIdNotFoundTest() throws Exception {
		ExtractableResponse<Response> response = given().body(subscriptionEntityPayload)
				.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
				.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
				.delete("/ngsi-ld/v1/csourceSubscriptions/{id}", "urn:ngsi-ld:Subscription:a0a").then()
				.statusCode(Status.NOT_FOUND.getStatusCode()).statusCode(404).extract();
		int statusCode = response.statusCode();
		assertEquals(404, statusCode);
		assertNotEquals(400, response.statusCode());

	}

	@Test
	@Order(17)
	public void deleteSubscriptionByBadRequestTest() throws Exception {
		ExtractableResponse<Response> response = given().body(subscriptionEntityPayload)
				.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
				.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
				.delete("/ngsi-ld/v1/csourceSubscriptions/{id}", ":ngsi-ld:Subscription:a0a").then()
				.statusCode(Status.BAD_REQUEST.getStatusCode()).statusCode(400).extract();
		int statusCode = response.statusCode();
		assertEquals(400, statusCode);
		assertNotEquals(200, response.statusCode());

	}

}
