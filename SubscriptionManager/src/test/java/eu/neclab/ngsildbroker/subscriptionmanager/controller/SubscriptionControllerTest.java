package eu.neclab.ngsildbroker.subscriptionmanager.controller;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response.Status;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
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

import static org.junit.jupiter.api.Assertions.assertEquals;

@QuarkusTest
@TestMethodOrder(OrderAnnotation.class)
@TestProfile(CustomProfile.class)
public class SubscriptionControllerTest {

	private String subscriptionEntityPayload;
	private String subscriptionEntityBadRequestPayload;

	@BeforeEach
	public void setup() throws Exception {

		subscriptionEntityPayload = "{" + "\r\n\"id\": \"urn:ngsi-ld:Subscription:211\","
				+ "\r\n\"type\": \"Subscription\"," + "\r\n\"entities\": [{"
				+ "\r\n		  \"id\": \"urn:ngsi-ld:Vehicle:A143\"," + "\r\n		  \"type\": \"Vehicle\""
				+ "\r\n		}]," + "\r\n\"watchedAttributes\": [\"brandName\"],"
				+ "\r\n		\"q\":\"brandName!=Mercedes\"," + "\r\n\"notification\": {"
				+ "\r\n  \"attributes\": [\"brandName\"]," + "\r\n  \"format\": \"keyValues\","
				+ "\r\n  \"endpoint\": {" + "\r\n   \"uri\": \"mqtt://localhost:1883/notify\","
				+ "\r\n   \"accept\": \"application/json\"," + "\r\n	\"notifierinfo\": {"
				+ "\r\n	  \"version\" : \"mqtt5.0\"," + "\r\n	  \"qos\" : 0" + "\r\n	}" + "\r\n  }" + "\r\n}"
				+ "\r\n}";

		subscriptionEntityBadRequestPayload = "{" + "\r\n\"id\": \"urn:ngsi-ld:Subscription:211\","
				+ "\r\n\"type\": \"Subscription123\"," + "\r\n\"entities\": [{"
				+ "\r\n		  \"id\": \"urn:ngsi-ld:Vehicle:A143\"," + "\r\n		  \"type\": \"Vehicle\""
				+ "\r\n		}]," + "\r\n\"watchedAttributes\": [\"brandName\"],"
				+ "\r\n		\"q\":\"brandName!=Mercedes\"," + "\r\n\"notification\": {"
				+ "\r\n  \"attributes\": [\"brandName\"]," + "\r\n  \"format\": \"keyValues\","
				+ "\r\n  \"endpoint\": {" + "\r\n   \"uri\": \"mqtt://localhost:1883/notify\","
				+ "\r\n   \"accept\": \"application/json\"," + "\r\n	\"notifierinfo\": {"
				+ "\r\n	  \"version\" : \"mqtt5.0\"," + "\r\n	  \"qos\" : 0" + "\r\n	}" + "\r\n  }" + "\r\n}"
				+ "\r\n}";

	}

	@AfterEach
	public void tearDown() {
		subscriptionEntityPayload = null;
		subscriptionEntityBadRequestPayload = null;
	}

	/**
	 * this method is use for subscribe the entity
	 */
	@Test
	@Order(1)
	public void createSubscriptionEntityTest() {

		try {
			ExtractableResponse<Response> response = RestAssured.given().body(subscriptionEntityPayload)
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
					.post("/ngsi-ld/v1/subscriptions").then().statusCode(Status.CREATED.getStatusCode()).statusCode(201)
					.extract();
			assertEquals(201, response.statusCode());

		} catch (Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}

	}

	/**
	 * this method is use for the subscribe entity if subscribe entity already
	 * exists
	 */
	@Test
	@Order(2)
	public void createSubscriptionEntityAlreadyExistTest() {

		try {
			ExtractableResponse<Response> response = RestAssured.given().body(subscriptionEntityPayload)
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
					.post("/ngsi-ld/v1/subscriptions").then().statusCode(Status.CONFLICT.getStatusCode())
					.statusCode(409).extract();
			assertEquals(409, response.statusCode());

		} catch (Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}

	}

	/**
	 * this method is try to subscribe the entity having "BAD REQUEST"
	 */
	@Test
	@Order(3)
	public void createSubscriptionEntityBadRequestTest() {

		try {
			ExtractableResponse<Response> response = RestAssured.given().body(subscriptionEntityBadRequestPayload)
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
					.post("/ngsi-ld/v1/subscriptions").then().statusCode(Status.BAD_REQUEST.getStatusCode())
					.statusCode(400).extract();
			assertEquals(400, response.statusCode());

		} catch (Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}

	}

	/**
	 * this method is used get the subscribe entity by Id.
	 */
	@Test
	@Order(4)
	public void getSubscriptionEntityByIdTest() {

		try {
			ExtractableResponse<Response> response = RestAssured.given()
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
					.get("/ngsi-ld/v1/subscriptions/urn:ngsi-ld:Subscription:211").then()
					.statusCode(Status.OK.getStatusCode()).statusCode(200).extract();
			assertEquals(200, response.statusCode());

		} catch (Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}

	}

	/**
	 * this method is used get the subscribe entity if Id not found
	 */
	@Test
	@Order(5)
	public void getSubscriptionEntityByIdNotFoundTest() {

		try {
			ExtractableResponse<Response> response = RestAssured.given()
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
					.get("/ngsi-ld/v1/subscriptions/urn:ngsi-ld:Subscription:212").then()
					.statusCode(Status.NOT_FOUND.getStatusCode()).statusCode(404).extract();
			assertEquals(404, response.statusCode());

		} catch (Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}

	}

	/**
	 * this method is used get the subscribe entity.
	 */
	@Test
	@Order(6)
	public void getSubscriptionEntityTest() {

		try {
			ExtractableResponse<Response> response = RestAssured.given()
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
					.get("/ngsi-ld/v1/subscriptions/").then().statusCode(Status.OK.getStatusCode()).statusCode(200)
					.extract();
			assertEquals(200, response.statusCode());

		} catch (Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}

	}

	/**
	 * this method is update the subscription
	 */
	@Test
	@Order(7)
	public void updateSubscriptionTest() {

		try {
			ExtractableResponse<Response> response = RestAssured.given().body(subscriptionEntityPayload)
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
					.patch("/ngsi-ld/v1/subscriptions/urn:ngsi-ld:Subscription:211/").then()
					.statusCode(Status.NO_CONTENT.getStatusCode()).statusCode(204).extract();
			assertEquals(204, response.statusCode());

		} catch (Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}

	}

	/**
	 * this method is update the subscription if Id not found
	 */
	@Test
	@Order(8)
	public void updateSubscriptionBadRequestTest() {

		try {
			ExtractableResponse<Response> response = RestAssured.given().body(subscriptionEntityPayload)
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
					.patch("/ngsi-ld/v1/subscriptions/urn:ngsi-ld:Subscription:212/").then()
					.statusCode(Status.BAD_REQUEST.getStatusCode()).statusCode(400).extract();
			assertEquals(400, response.statusCode());

		} catch (Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}

	}

	/**
	 * this method is used get the subscribe entity if limit < 0
	 */
	@Test
	@Order(9)
	public void getAllSubscriptionBadRequestTest() {

		try {
			ExtractableResponse<Response> response = RestAssured.given()
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
					.get("/ngsi-ld/v1/subscriptions?limit=-1").then().statusCode(Status.BAD_REQUEST.getStatusCode())
					.statusCode(400).extract();
			assertEquals(400, response.statusCode());

		} catch (Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}

	}

	/**
	 * this method is used get the subscribe entity if limit = 10000
	 */
	@Test
	@Order(10)
	public void getAllSubscriptionForbiddenTest() {

		try {
			ExtractableResponse<Response> response = RestAssured.given()
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
					.get("/ngsi-ld/v1/subscriptions?limit=10000").then().statusCode(Status.FORBIDDEN.getStatusCode())
					.statusCode(403).extract();
			assertEquals(403, response.statusCode());

		} catch (Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}

	}

	/**
	 * this method is use for delete subscription
	 */
	@Test
	@Order(11)
	public void deleteSubscriptionTest() {

		try {
			ExtractableResponse<Response> response = RestAssured.given()
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
					.delete("/ngsi-ld/v1/subscriptions/urn:ngsi-ld:Subscription:211").then()
					.statusCode(Status.NO_CONTENT.getStatusCode()).statusCode(204).extract();
			assertEquals(204, response.statusCode());

		} catch (Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}

	}

	/**
	 * this method is use for delete subscription if Id not found
	 */
	@Test
	@Order(12)
	public void deleteSubscriptionIdNotFoundTest() {

		try {
			ExtractableResponse<Response> response = RestAssured.given()
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
					.delete("/ngsi-ld/v1/subscriptions/urn:ngsi-ld:Subscription:212").then()
					.statusCode(Status.NOT_FOUND.getStatusCode()).statusCode(404).extract();
			assertEquals(404, response.statusCode());

		} catch (Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}

	}

}