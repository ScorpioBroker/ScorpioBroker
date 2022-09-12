package eu.neclab.ngsildbroker.entityhandler.controller;

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
public class EntityBatchControllerTest {

	private String payload;
	private String badRequestPayload;
	private String badRequestPayload1;
	private String deletePayload;
	private String BadRequestDeletePayload;

	@BeforeEach
	public void setup() {

		payload = "[  \r\n" + "{  \r\n" + "   \"id\":\"urn:ngsi-ld:Vehicle:A101\",\r\n" + "   \"type\":\"Vehicle\",\r\n"
				+ "   \"brandName\":\r\n" + "      {  \r\n" + "         \"type\":\"Property\",\r\n"
				+ "         \"value\":\"Mercedes\"\r\n" + "      },\r\n" + "   \"speed\":{  \r\n"
				+ "         \"type\":\"Property\",\r\n" + "         \"value\":80\r\n" + "    }\r\n" + "},\r\n"
				+ "{  \r\n" + "   \"id\":\"urn:ngsi-ld:Vehicle:A102\",\r\n" + "   \"type\":\"Vehicle\",\r\n"
				+ "   \"brandName\":\r\n" + "      {  \r\n" + "         \"type\":\"Property\",\r\n"
				+ "         \"value\":\"Mercedes\"\r\n" + "      },\r\n" + "   \"speed\":{  \r\n"
				+ "         \"type\":\"Property\",\r\n" + "         \"value\":81\r\n" + "    }\r\n" + "},\r\n"
				+ "{  \r\n" + "   \"id\":\"urn:ngsi-ld:Vehicle:A103\",\r\n" + "   \"type\":\"Vehicle\",\r\n"
				+ "   \"brandName\":\r\n" + "      {  \r\n" + "         \"type\":\"Property\",\r\n"
				+ "         \"value\":\"Mercedes\"\r\n" + "      },\r\n" + "   \"speed\":{  \r\n"
				+ "         \"type\":\"Property\",\r\n" + "         \"value\":82\r\n" + "    }\r\n" + "}\r\n" + "]";

		badRequestPayload = "[  \r\n" + "{  \r\n" + "   \"id\":\"urn:ngsi-ld:Vehicle:A101\",\r\n"
				+ "   \"type123\":\"Vehicle\",\r\n" + "   \"brandName\":\r\n" + "      {  \r\n"
				+ "         \"type\":\"Property\",\r\n" + "         \"value\":\"Mercedes\"\r\n" + "      },\r\n"
				+ "   \"speed\":{  \r\n" + "         \"type\":\"Property\",\r\n" + "         \"value\":80\r\n"
				+ "    }\r\n" + "},\r\n" + "{  \r\n" + "   \"id\":\"urn:ngsi-ld:Vehicle:A102\",\r\n"
				+ "   \"type\":\"Vehicle\",\r\n" + "   \"brandName\":\r\n" + "      {  \r\n"
				+ "         \"type\":\"Property\",\r\n" + "         \"value\":\"Mercedes\"\r\n" + "      },\r\n"
				+ "   \"speed\":{  \r\n" + "         \"type\":\"Property\",\r\n" + "         \"value\":81\r\n"
				+ "    }\r\n" + "},\r\n" + "{  \r\n" + "   \"id\":\"urn:ngsi-ld:Vehicle:A103\",\r\n"
				+ "   \"type\":\"Vehicle\",\r\n" + "   \"brandName\":\r\n" + "      {  \r\n"
				+ "         \"type\":\"Property\",\r\n" + "         \"value\":\"Mercedes\"\r\n" + "      },\r\n"
				+ "   \"speed\":{  \r\n" + "         \"type\":\"Property\",\r\n" + "         \"value\":82\r\n"
				+ "    }\r\n" + "}\r\n" + "]";

		badRequestPayload1 = "[  \r\n" + "{  \r\n" + "   \"id\":\"urn:ngsi-ld:Vehicle:A101\",\r\n"
				+ "   \"type123\":\"Vehicle\",\r\n" + "   \"brandName\":\r\n" + "      {  \r\n"
				+ "         \"type\":\"Property\",\r\n" + "         \"value\":\"Mercedes\"\r\n" + "      },\r\n"
				+ "   \"speed\":{  \r\n" + "         \"type\":\"Property\",\r\n" + "         \"value\":80\r\n"
				+ "    }\r\n" + "},\r\n" + "{  \r\n" + "   \"id\":\"urn:ngsi-ld:Vehicle:A102\",\r\n"
				+ "   \"type123\":\"Vehicle\",\r\n" + "   \"brandName\":\r\n" + "      {  \r\n"
				+ "         \"type\":\"Property\",\r\n" + "         \"value\":\"Mercedes\"\r\n" + "      },\r\n"
				+ "   \"speed\":{  \r\n" + "         \"type\":\"Property\",\r\n" + "         \"value\":81\r\n"
				+ "    }\r\n" + "},\r\n" + "{  \r\n" + "   \"id\":\"urn:ngsi-ld:Vehicle:A103\",\r\n"
				+ "   \"type123\":\"Vehicle\",\r\n" + "   \"brandName\":\r\n" + "      {  \r\n"
				+ "         \"type\":\"Property\",\r\n" + "         \"value\":\"Mercedes\"\r\n" + "      },\r\n"
				+ "   \"speed\":{  \r\n" + "         \"type\":\"Property\",\r\n" + "         \"value\":82\r\n"
				+ "    }\r\n" + "}\r\n" + "]";

		deletePayload = "[  \r\n" + "\"urn:ngsi-ld:Vehicle:A101\",\r\n" + "\"urn:ngsi-ld:Vehicle:A102\",\r\n"
				+ "\"urn:ngsi-ld:Vehicle:A103\"\r\n" + "]";

		BadRequestDeletePayload = "[  \r\n" + "\"urn:ngsi-ld:Vehicle:A201\",\r\n" + "\"urn:ngsi-ld:Vehicle:A102\"\r\n"
				+ "]";

	}

	@AfterEach
	public void tearDown() {
		payload = null;
		badRequestPayload = null;
		badRequestPayload1 = null;
		deletePayload = null;
		BadRequestDeletePayload = null;

	}

	/**
	 * this method is use for create the multiple entity
	 */
	@Test
	@Order(1)
	public void createMultipleEntityTest() {

		try {
			ExtractableResponse<Response> response = RestAssured.given().body(payload)
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
					.post("/ngsi-ld/v1/entityOperations/create").then().statusCode(Status.CREATED.getStatusCode())
					.statusCode(201).extract();
			assertEquals(201, response.statusCode());

		} catch (Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}

	}

	/**
	 * this method is validate the bad request if create the multiple entity but
	 * some entity request is not valid
	 */
	@Test
	@Order(2)
	public void createMultipleEntityBadRequestTest() {

		try {
			ExtractableResponse<Response> response = RestAssured.given().body(badRequestPayload)
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
					.post("/ngsi-ld/v1/entityOperations/create").then().statusCode(Status.BAD_REQUEST.getStatusCode())
					.statusCode(400).extract();
			assertEquals(400, response.statusCode());

		} catch (Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}

	}

	/**
	 * this method is use for update the multiple entity.
	 */
	@Test
	@Order(3)
	public void updateMultipleEntityTest() {

		try {
			ExtractableResponse<Response> response = RestAssured.given().body(payload)
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
					.post("/ngsi-ld/v1/entityOperations/update").then().statusCode(Status.NO_CONTENT.getStatusCode())
					.statusCode(204).extract();
			assertEquals(204, response.statusCode());

		} catch (Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}

	}

	/**
	 * this method is validate the bad request if update the multiple entity but
	 * some entity request is not valid
	 */
	@Test
	@Order(4)
	public void updateMultipleEntityBadRequestTest() {

		try {
			ExtractableResponse<Response> response = RestAssured.given().body(badRequestPayload1)
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
					.post("/ngsi-ld/v1/entityOperations/update").then().statusCode(Status.BAD_REQUEST.getStatusCode())
					.statusCode(400).extract();
			assertEquals(400, response.statusCode());

		} catch (Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}

	}

	/**
	 * this method is use for upsert the multiple entity if all entities already
	 * exist.
	 */
	@Test
	@Order(5)
	public void upsertMultipleEntityTest() {

		try {
			ExtractableResponse<Response> response = RestAssured.given().body(payload)
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
					.post("/ngsi-ld/v1/entityOperations/upsert").then().statusCode(Status.NO_CONTENT.getStatusCode())
					.statusCode(204).extract();
			assertEquals(204, response.statusCode());

		} catch (Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}

	}

	/**
	 * this method is validate the bad request if upsert the multiple entity but
	 * some entity request is not valid
	 */
	@Test
	@Order(6)
	public void upsertMultipleEntityBadRequestTest() {

		try {
			ExtractableResponse<Response> response = RestAssured.given().body(badRequestPayload1)
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
					.post("/ngsi-ld/v1/entityOperations/upsert").then().statusCode(Status.BAD_REQUEST.getStatusCode())
					.statusCode(400).extract();
			assertEquals(400, response.statusCode());

		} catch (Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}

	}

	/**
	 * this method is use for delete the multiple entity
	 */
	@Test
	@Order(7)
	public void deleteMultipleEntityTest() {

		try {
			ExtractableResponse<Response> response = RestAssured.given().body(deletePayload)
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
					.post("/ngsi-ld/v1/entityOperations/delete").then().statusCode(Status.NO_CONTENT.getStatusCode())
					.statusCode(204).extract();
			assertEquals(204, response.statusCode());

		} catch (Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}

	}

	/**
	 * this method is use for delete the multiple entity but someone entity not
	 * exist.
	 */
	@Test
	@Order(8)
	public void deleteMultipleEntityBadRequestTest() {

		try {
			ExtractableResponse<Response> response = RestAssured.given().body(BadRequestDeletePayload)
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
					.post("/ngsi-ld/v1/entityOperations/delete").then().statusCode(Status.BAD_REQUEST.getStatusCode())
					.statusCode(400).extract();
			assertEquals(400, response.statusCode());

		} catch (Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}

	}

}