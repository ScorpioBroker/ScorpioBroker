package eu.neclab.ngsildbroker.historymanager.controller;

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
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.response.ExtractableResponse;
import io.restassured.response.Response;

@QuarkusTest
@TestMethodOrder(OrderAnnotation.class)
@TestProfile(CustomProfile.class)
public class HistoryControllerTest {

	private String temporalPayloadBad;
	private String temporalPayload;
	private String addAttrsPayload;
	private String payload;

	@BeforeEach
	public void setup() throws Exception {
		// @formatter:on
		temporalPayload = "{\r\n" + "    \"id\": \"urn:ngsi-ld:testunit:a0a\",\r\n"
				+ "    \"type\": \"AirQualityObserved\",\r\n" + "    \"airQualityLeve2\": [\r\n" + "        {\r\n"
				+ "            \"type\": \"Property\",\r\n" + "            \"value\": \"good\",\r\n"
				+ "            \"observedAt\": \"2018-08-07T12:00:00Z\",\r\n"
				+ "            \"instanceId\": \"urn:ngsi-ld:0afa5d4c-3ca7-4d2e-96c9-88a437a849dc\"\r\n"
				+ "        },\r\n" + "        {\r\n" + "            \"type\": \"Property\",\r\n"
				+ "            \"value\": \"unhealthy\",\r\n"
				+ "            \"observedAt\": \"2018-09-14T12:00:00Z\",\r\n"
				+ "            \"instanceId\": \"urn:ngsi-ld:0afa5d4c-3ca7-4d2e-96c9-99a437a849dc\"\r\n"
				+ "        }\r\n" + "    ]\r\n" + "}";

		temporalPayloadBad = "{\r\n" + "    \"id\": \"\",\r\n" + "    \"type\": \"AirQualityObse\",\r\n"
				+ "    \"airQualityLeve2\": [\r\n" + "        {\r\n" + "            \"type\": \"Property\",\r\n"
				+ "            \"value\": \"good\",\r\n" + "            \"observedAt\": \"2018-08-07T12:00:00Z\",\r\n"
				+ "            \"instanceId\": \"urn:ngsi-ld:0afa5d4c-3ca7-4d2e-96c9-88a437a849dc\"\r\n"
				+ "        },\r\n" + "        {\r\n" + "            \"type\": \"Property\",\r\n"
				+ "            \"value\": \"unhealthy\",\r\n"
				+ "            \"observedAt\": \"2018-09-14T12:00:00Z\",\r\n"
				+ "            \"instanceId\": \"urn:ngsi-ld:0afa5d4c-3ca7-4d2e-96c9-99a437a849dc\"\r\n"
				+ "        }\r\n" + "    ]\r\n" + "}";

		payload = "";

		addAttrsPayload = "{\r\n" + "    \"airQualityLevel1\": [\r\n" + "        {\r\n"
				+ "            \"type\": \"Property\",\r\n" + "            \"value\": \"good\",\r\n"
				+ "            \"observedAt\": \"2018-08-07T12:00:00Z\"\r\n" + "        }\r\n" + "    ]\r\n" + "}";

	}

	@AfterEach
	public void tearDown() {
		temporalPayload = null;
		payload = null;
		temporalPayloadBad = null;
		addAttrsPayload = null;
	}

	/**
	 * this method is use for create the temporalEntity
	 */
	@Test
	@Order(1)
	public void createTemporalEntityTest() throws Exception {
		ExtractableResponse<Response> response = given().body(temporalPayload)
				.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
				.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
				.post("/ngsi-ld/v1/temporal/entities").then().statusCode(Status.CREATED.getStatusCode()).statusCode(201)
				.extract();
		int statsCode = response.statusCode();
		assertEquals(201, statsCode);

	}

	/**
	 * this method is use to test Already Existed Entity
	 */
	@Test
	@Order(2)
	public void createTemporalEntityAlreadyExistTest() throws Exception {

		ExtractableResponse<Response> response = given().body(temporalPayload)
				.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
				.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
				.post("/ngsi-ld/v1/temporal/entities/").then().statusCode(Status.NO_CONTENT.getStatusCode())
				.statusCode(204).extract();

		assertEquals(204, response.statusCode());
		assertNotEquals(201, response.statusCode());

	}

	/**
	 * this method is use for create the temporalEntity Bad Request
	 */

	@Test
	@Order(3)
	public void createTemporalEntityBadRequestTest() throws Exception {

		ExtractableResponse<Response> response = given().body(temporalPayloadBad)
				.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
				.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
				.post("/ngsi-ld/v1/temporal/entities/").then().statusCode(Status.BAD_REQUEST.getStatusCode())
				.statusCode(400).extract();
		assertEquals(400, response.statusCode());
		assertNotEquals(200, response.statusCode());

	}

	/**
	 * this method is use for create the temporalEntity Bad Request with null
	 * payload
	 */

	@Test
	@Order(4)
	public void createTemporalEntityNullTest() throws Exception {
		ExtractableResponse<Response> response = given().body(payload)
				.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
				.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
				.post("/ngsi-ld/v1/temporal/entities/").then().statusCode(Status.BAD_REQUEST.getStatusCode())
				.statusCode(400).extract();
		assertEquals(400, response.statusCode());
		assertNotEquals(200, response.statusCode());

	}

	/**
	 * this method is use for get the temporalEntity
	 */
	@Test
	@Order(5)
	public void getTemporalEntityTest() throws Exception {
		ExtractableResponse<Response> response = given()
				.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
				.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
				.get("/ngsi-ld/v1/temporal/entities?type=Test1").then().statusCode(Status.OK.getStatusCode())
				.statusCode(200).extract();
		assertEquals(200, response.statusCode());
		assertNotEquals(400, response.statusCode());

	}

	/**
	 * this method is use for get the temporalEntity with BadRequest
	 */
	@Test
	@Order(6)
	public void getTemporalEntitiesBadRequestTest() throws Exception {
		ExtractableResponse<Response> response = given()
				.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
				.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
				.get("/ngsi-ld/v1/temporal/entities?typ=Test1").then().statusCode(Status.BAD_REQUEST.getStatusCode())
				.statusCode(400).extract();
		assertEquals(400, response.statusCode());
		assertNotEquals(200, response.statusCode());

	}

	/**
	 * this method is use for get the temporalEntity by ID
	 */
	@Test
	@Order(7)
	public void getTemporalEntitiesByIdTest() throws Exception {
		ExtractableResponse<Response> response = given()
				.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
				.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
				.get("/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:testunit:a0a").then()
				.statusCode(Status.OK.getStatusCode()).statusCode(200).extract();
		assertEquals(200, response.statusCode());
		assertNotEquals(400, response.statusCode());

	}

	/**
	 * this method is use for get the temporalEntity by ID Not Found
	 */
	@Test
	@Order(8)
	public void getTemporalEntitiesByIdNotFoundTest() throws Exception {
		ExtractableResponse<Response> response = given()
				.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
				.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
				.get("/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:testunit:a0").then()
				.statusCode(Status.NOT_FOUND.getStatusCode()).statusCode(404).extract();
		assertEquals(404, response.statusCode());
		assertNotEquals(200, response.statusCode());

	}

	/**
	 * this method is use for get the temporalEntity by ID BadRequest
	 */
	@Test
	@Order(9)
	public void getTemporalEntitiesByIdBadRequestTest() throws Exception {
		ExtractableResponse<Response> response = given()
				.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
				.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
				.get("/ngsi-ld/v1/temporal/entities").then().statusCode(Status.BAD_REQUEST.getStatusCode())
				.statusCode(400).extract();
		assertEquals(400, response.statusCode());
		assertNotEquals(200, response.statusCode());

	}

	/**
	 * this method is try to add Attrs to the temporalEntity with post request
	 */

	@Test
	@Order(10)
	public void addAttrib2TemopralEntityTest() throws Exception {
		ExtractableResponse<Response> response = given().body(addAttrsPayload)
				.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
				.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
				.post("/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:testunit:a0a/attrs").then()
				.statusCode(Status.NO_CONTENT.getStatusCode()).statusCode(204).extract();
		int statsCode = response.statusCode();
		assertEquals(204, statsCode);

	}

	/**
	 * this method is try to add Attrs to the temporalEntity with post request Attrs
	 * Not Found
	 */
	@Test
	@Order(11)
	public void addAttrib2TemopralEntityNotFoundTest() throws Exception {

		ExtractableResponse<Response> response = given().body(addAttrsPayload)
				.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
				.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
				.post("/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:testunit:a0/attrs").then()
				.statusCode(Status.NOT_FOUND.getStatusCode()).statusCode(404).extract();
		int statsCode = response.statusCode();
		assertEquals(404, statsCode);

	}

	/**
	 * this method is try to add Attrs to the temporalEntity with post request Attrs
	 * BadRequest
	 */
	@Test
	@Order(12)
	public void addAttrib2TemopralEntityBadRequestTest() throws Exception {

		ExtractableResponse<Response> response = given().body(payload)
				.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
				.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
				.post("/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:testunit:a0a/attrs").then()
				.statusCode(Status.BAD_REQUEST.getStatusCode()).statusCode(400).extract();
		int statsCode = response.statusCode();
		assertEquals(400, statsCode);

	}

	/**
	 * this method is try to modify the attribute of temporalEntity with Internal
	 * server error
	 */

	@Test
	@Order(13)
	public void modifyAttribInstanceTemporalEntityNotFoundTest() throws Exception {
		ExtractableResponse<Response> response = given().body(addAttrsPayload)
				.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
				.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
				.patch("/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:testunit:a0a/attrs/airQualityLeve2/urn:ngsi-ld:a4c30f3d--4bf8-9e22-36db297c396c")
				.then().statusCode(Status.NOT_FOUND.getStatusCode()).statusCode(404).extract();
		int statsCode = response.statusCode();
		assertEquals(404, statsCode);

	}

	/**
	 * this method is use for delete the temporalEntity by attribute
	 */
	@Test
	@Order(14)
	public void deleteTemporalEntityByAttrTest() throws Exception {

		ExtractableResponse<Response> response = given()
				.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
				.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
				.delete("/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:testunit:a0a/attrs/airQualityLeve2").then()
				.statusCode(Status.NO_CONTENT.getStatusCode()).statusCode(204).extract();
		int statsCode = response.statusCode();
		assertEquals(204, statsCode);

	}

	/**
	 * this method is use for delete the temporalEntity by attribute , attribute Not
	 * Found
	 */
	@Test
	@Order(15)
	public void deleteTemporalEntityByAttrNotFoundTest() throws Exception {
		ExtractableResponse<Response> response = given()
				.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
				.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
				.delete("/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:testunit:a0a/attrs/air").then()
				.statusCode(Status.NOT_FOUND.getStatusCode()).statusCode(404).extract();
		int statsCode = response.statusCode();
		assertEquals(404, statsCode);

	}

	/**
	 * this method is use for delete the temporalEntity by attribute BadRequest
	 */
	@Test
	@Order(16)
	public void deleteTemporalEntityByAttrBadRequestTest() throws Exception {
		ExtractableResponse<Response> response = given()
				.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
				.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
				.delete("/ngsi-ld/v1/temporal/entities/ -ld:testunit:153/attrs/airQualityLev").then()
				.statusCode(Status.BAD_REQUEST.getStatusCode()).statusCode(400).extract();
		int statsCode = response.statusCode();
		assertEquals(400, statsCode);

	}

	/**
	 * this method is use for delete the temporalEntity by instance , instance Not
	 * Found
	 */
	@Test
	@Order(17)
	public void deleteTemporalEntityAttrsByInstanceNotFoundTest() throws Exception {
		ExtractableResponse<Response> response = given()
				.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
				.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
				.delete("/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:testunit:a0a/attrs/airQualityLevel2/urn:ngsi-ld:9ad1acfd-5d4b-4c92-8463-852e192cd152\r\n")
				.then().statusCode(Status.NOT_FOUND.getStatusCode()).statusCode(404).extract();
		int statsCode = response.statusCode();
		assertEquals(404, statsCode);

	}

	/**
	 * this method is use for delete the temporalEntity by instance , instance
	 * BadRequest
	 */
	@Test
	@Order(18)
	public void deleteTemporalEntityAttrsByInstanceBadRequestTest() throws Exception {
		ExtractableResponse<Response> response = given()
				.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
				.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
				.delete("/ngsi-ld/v1/temporal/entities/-ld:testunit:a0a/attrs/airQualityLevel2/urn:ngsi-ld:9ad1acfd-5d4b-4c92-8463-852e192cd152")
				.then().statusCode(Status.BAD_REQUEST.getStatusCode()).statusCode(400).extract();
		int statsCode = response.statusCode();
		assertEquals(400, statsCode);

	}

	/**
	 * this method is use for delete the temporalEntity by id
	 */
	@Test
	@Order(19)
	public void deleteTemporalEntityByIdTest() throws Exception {
		ExtractableResponse<Response> response = given()
				.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
				.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
				.delete("/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:testunit:a0a").then()
				.statusCode(Status.NO_CONTENT.getStatusCode()).statusCode(204).extract();
		int statsCode = response.statusCode();
		assertEquals(204, statsCode);

	}

	/**
	 * this method is use for delete the temporalEntity by id Not Found
	 */
	@Test
	@Order(20)
	public void deleteTemporalEntityByIdNotFoundTest() throws Exception {
		ExtractableResponse<Response> response = given()
				.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
				.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
				.delete("/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:testunit:a0a").then()
				.statusCode(Status.NOT_FOUND.getStatusCode()).statusCode(404).extract();
		int statsCode = response.statusCode();
		assertEquals(404, statsCode);

	}

	/**
	 * this method is use for delete the temporalEntity by id BadRequest
	 */
	@Test
	@Order(21)
	public void deleteTemporalEntityByIdBadRequestTest() throws Exception {
		ExtractableResponse<Response> response = given()
				.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
				.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
				.delete("/ngsi-ld/v1/temporal/entities/{entityId}", " ").then()
				.statusCode(Status.BAD_REQUEST.getStatusCode()).statusCode(400).extract();
		int statsCode = response.statusCode();
		assertEquals(400, statsCode);

	}

}
