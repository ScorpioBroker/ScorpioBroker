package eu.neclab.ngsildbroker.registryhandler.controller;

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
import io.restassured.response.ExtractableResponse;
import io.restassured.response.Response;

@QuarkusTest
@TestMethodOrder(OrderAnnotation.class)
@TestProfile(CustomProfile.class)
public class RegistryControllerTest {

	private String payload;
	private String payloadBadRequest;
	private String updatePayload;
	private String updatePayloadBadRequest;

	@BeforeEach
	public void setup() {

		payload = "{\r\n" + "    \"id\": \"urn:ngsi-ld:ContextSourceRegistration:A505\",\r\n"
				+ "    \"type\": \"ContextSourceRegistration\",\r\n" + "    \"brandName\": {\r\n"
				+ "        \"type\": \"Property\",\r\n" + "        \"value\": \"GOREA\"\r\n" + "    },\r\n"
				+ "    \"information\": [\r\n" + "        {\r\n" + "            \"brandName\": {\r\n"
				+ "                \"type\": \"Property\",\r\n" + "                \"value\": \"Mercedes\"\r\n"
				+ "            },\r\n" + "            \"entities\": [\r\n" + "                {\r\n"
				+ "                    \"id\": \"urn:ngsi-ld:ContextSourceRegistration:5616438376726496\",\r\n"
				+ "                    \"type\": \"Vehicle1\"\r\n" + "                }\r\n" + "            ],\r\n"
				+ "            \"propertyNames\": [\r\n" + "                \"brandName\",\r\n"
				+ "                \"speed\"\r\n" + "            ],\r\n" + "            \"relationshipNames\": [\r\n"
				+ "                \"isParked\"\r\n" + "            ]\r\n" + "        }\r\n" + "    ] \r\n" + "}";

		payloadBadRequest = "{\r\n" + "    \"id\": \" 3\",\r\n" + "    \"type\": \"Test10.3\",\r\n"
				+ "    \"name\": {\r\n" + "        \"type\": \"Property\",\r\n" + "        \"value\": \"BMW\"\r\n"
				+ "    },\r\n" + "    \"speed\": {\r\n" + "        \"type\": \"Property\",\r\n"
				+ "        \"value\": 80\r\n" + "    }\r\n" + "}";

		updatePayload = "{\r\n" + "    \"brandName\": {\r\n" + "        \"type\": \"Property\",\r\n"
				+ "        \"value\": \"WEX\"\r\n" + "    }\r\n" + "}";

		updatePayloadBadRequest = "{\r\n" + "    \"brandName\": {\r\n" + "        \"type\": \"Property\",\r\n"
				+ "        \"value\": \"c\"\r\n" + "    },\r\n" + "    \"@context\": [\r\n"
				+ "        \"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.3.jsonld\"\r\n" + "    ]\r\n"
				+ "}";

	}

	@AfterEach
	public void teardown() {
		payload = null;
		payloadBadRequest = null;
		updatePayload = null;
		updatePayloadBadRequest = null;

	}

	/**
	 * this method is use for the csource registration
	 */
	@Test
	@Order(1)
	public void registerCSourceTest() throws Exception {
		ExtractableResponse<Response> response = given().body(payload)
				.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
				.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
				.post("/ngsi-ld/v1/csourceRegistrations/").then().statusCode(Status.CREATED.getStatusCode())
				.statusCode(201).extract();
		assertEquals(201, response.statusCode());
	}

	/**
	 * this method is use to test Already exist
	 */
	@Test
	@Order(2)
	public void registerCSourceAlreadyExistTest() {
		try {
			ExtractableResponse<Response> response = given().body(payload)
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
					.post("/ngsi-ld/v1/csourceRegistrations/").then().statusCode(Status.CONFLICT.getStatusCode())
					.statusCode(409).extract();
			assertEquals(409, response.statusCode());
		} catch (Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}
	}

	/**
	 * this method is use to test BadRequest
	 */
	@Test
	@Order(3)
	public void registerCSourceBadReqestTest() {
		try {
			ExtractableResponse<Response> response = given().body(payloadBadRequest)
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
					.post("/ngsi-ld/v1/csourceRegistrations/").then().statusCode(Status.BAD_REQUEST.getStatusCode())
					.statusCode(400).extract();
			assertEquals(400, response.statusCode());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * this method is use for update the csource registration
	 */
	@Test
	@Order(4)
	public void updateCSourceTest() throws Exception {

		ExtractableResponse<Response> response = given().body(updatePayload)
				.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
				.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
				.patch("/ngsi-ld/v1/csourceRegistrations/urn:ngsi-ld:ContextSourceRegistration:A505").then()
				.statusCode(Status.NO_CONTENT.getStatusCode()).statusCode(204).extract();
		assertEquals(204, response.statusCode());

	}

	/**
	 * this method is use for update the csource registration not Found
	 */
	@Test
	@Order(5)
	public void updateCSourceNotFoundTest() {
		try {
			ExtractableResponse<Response> response = given().body(updatePayload)
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
					.patch("/ngsi-ld/v1/csourceRegistrations/urn:ngsi-ld:ContextSourceRegistration:A506").then()
					.statusCode(Status.NOT_FOUND.getStatusCode()).statusCode(404).extract();
			assertEquals(404, response.statusCode());
		} catch (Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}

	}

	@Test
	@Order(6)
	public void updateCSourceBadRequestTest() throws Exception {
		ExtractableResponse<Response> response = given().body(updatePayloadBadRequest)
				.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
				.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
				.patch("/ngsi-ld/v1/csourceRegistrations/urn:ngsi-ld:ContextSourceRegistration:A505").then()
				.statusCode(Status.BAD_REQUEST.getStatusCode()).statusCode(400).extract();

		assertEquals(400, response.statusCode());

	}

	/**
	 * this method is use for update the csource registration BadRequest
	 */
	@Test
	@Order(7)
	public void updateCSourceNullTest() throws Exception {

		ExtractableResponse<Response> response = given().body(updatePayloadBadRequest)
				.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
				.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
				.patch("/ngsi-ld/v1/csourceRegistrations/null").then().statusCode(Status.BAD_REQUEST.getStatusCode())
				.statusCode(400).extract();
		assertEquals(400, response.statusCode());

	}

	/**
	 * this method is use for get the csource registration by Id
	 */
	@Test
	@Order(8)
	public void getCSourceByIdTest() {
		try {
			ExtractableResponse<Response> response = given()
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
					.get("/ngsi-ld/v1/csourceRegistrations/urn:ngsi-ld:ContextSourceRegistration:A505").then()
					.statusCode(Status.OK.getStatusCode()).statusCode(200).extract();
			assertEquals(200, response.statusCode());
		} catch (Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}
	}

	/**
	 * this method is use for get the csource registration by Id Not Found
	 */
	@Test
	@Order(9)
	public void getCSourceByIdNotFoundTest() throws Exception {

		ExtractableResponse<Response> response = given()
				.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
				.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
				.get("/ngsi-ld/v1/csourceRegistrations/urn:ngsi-ld:ContextSourceRegistration:A506").then()
				.statusCode(Status.NOT_FOUND.getStatusCode()).statusCode(404).extract();
		assertEquals(404, response.statusCode());

	}

	/**
	 * this method is use for get the csource registration by Id BadRequest
	 */
	@Test
	@Order(10)
	public void getCSourceByIdBadRequestTest() throws Exception {

		ExtractableResponse<Response> response = given()
				.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
				.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
				.get("/ngsi-ld/v1/csourceRegistrations/:ngsi-ld:ContextSourceRegistration:A506").then()
				.statusCode(Status.BAD_REQUEST.getStatusCode()).statusCode(400).extract();
		assertEquals(400, response.statusCode());

	}

	/**
	 * this method is use for get the discover csource registration
	 */

	@Test
	@Order(11)
	public void getDiscoverCSourceTest() throws Exception {

		ExtractableResponse<Response> response = given()
				.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
				.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
				.get("/ngsi-ld/v1/csourceRegistrations?type=Test1").then().statusCode(Status.OK.getStatusCode())
				.statusCode(200).extract();
		assertEquals(200, response.statusCode());

	}

	/**
	 * this method is use for get the discover csource registration by BrandName
	 */
	@Test
	@Order(12)
	public void getDiscoverCSourcebrandNameTest() throws Exception {

		ExtractableResponse<Response> response = given()
				.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
				.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
				.get("/ngsi-ld/v1/csourceRegistrations?type=Vehicle&attrs=brandName").then()
				.statusCode(Status.OK.getStatusCode()).statusCode(200).extract();
		assertEquals(200, response.statusCode());

	}

	/**
	 * this method is use for delete the Csource registration
	 */
	@Test
	@Order(13)
	public void deleteCsourceTest() {
		try {
			ExtractableResponse<Response> response = given()
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
					.delete("/ngsi-ld/v1/csourceRegistrations/urn:ngsi-ld:ContextSourceRegistration:A505").then()
					.statusCode(Status.NO_CONTENT.getStatusCode()).statusCode(204).extract();
			assertEquals(204, response.statusCode());
		} catch (Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}
	}

	/**
	 * this method is use for delete the Csource registration Not Found
	 */
	@Test
	@Order(14)
	public void deleteCsourceNotFoundTest() {
		try {
			ExtractableResponse<Response> response = given()
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
					.delete("/ngsi-ld/v1/csourceRegistrations/urn:ngsi-ld:ContextSourceRegistration:A506").then()
					.statusCode(Status.NOT_FOUND.getStatusCode()).statusCode(404).extract();
			assertEquals(404, response.statusCode());
		} catch (Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}

	}

	@Test
	@Order(15)
	public void deleteCsourceBadRequestTest() throws Exception {

		ExtractableResponse<Response> response = given()
				.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
				.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
				.delete("/ngsi-ld/v1/csourceRegistrations/urn:ng :ContextSourceRegistration:A505").then()
				.statusCode(Status.BAD_REQUEST.getStatusCode()).statusCode(400).extract();
		int statusCode = response.statusCode();
		assertEquals(400, statusCode);

	}

	/**
	 * this method is use for delete the Csource registration BadRequest
	 */
	@Test
	@Order(16)
	public void deleteCsourceBadRequestIdTest() throws Exception {
		try {
			ExtractableResponse<Response> response = given()
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
					.delete("/ngsi-ld/v1/csourceRegistrations/{registrationId}", " ").then()
					.statusCode(Status.BAD_REQUEST.getStatusCode()).statusCode(400).extract();
			assertEquals(400, response.statusCode());
		} catch (Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}

	}

}
