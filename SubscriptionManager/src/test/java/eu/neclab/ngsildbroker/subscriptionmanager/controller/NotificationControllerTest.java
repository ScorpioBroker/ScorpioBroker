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
public class NotificationControllerTest {

	private String qPayload;

	@BeforeEach
	public void setup() {

		qPayload = "{\r\n" + "        \"id\": \"ngsildbroker:notification:-4670513482090232260\",\r\n"
				+ "        \"type\": \"Notification\",\r\n"
				+ "        \"subscriptionId\": \"urn:ngsi-ld:Subscription:storeSubscription\",\r\n"
				+ "        \"notifiedAt\": \"2022-01-30T21:23:50Z\",\r\n" + "        \"data\": [ {\r\n"
				+ "  \"id\" : \"urn:ngsi-ld:test1\",\r\n" + "  \"type\" : \"Test2\",\r\n"
				+ "  \"createdAt\" : \"2022-01-30T21:21:29Z\",\r\n" + "  \"blubdiblub\" : {\r\n"
				+ "    \"type\" : \"Property\",\r\n" + "    \"value\" : 200\r\n" + "  },\r\n" + "  \"name\" : {\r\n"
				+ "    \"type\" : \"Property\",\r\n" + "    \"value\" : \"6-Stars\"\r\n" + "  },\r\n"
				+ "  \"speed\" : {\r\n" + "    \"type\" : \"Property\",\r\n" + "    \"value\" : 202\r\n" + "  },\r\n"
				+ "  \"location\" : {\r\n" + "    \"type\" : \"GeoProperty\",\r\n" + "    \"value\" : {\r\n"
				+ "      \"type\" : \"Point\",\r\n" + "      \"coordinates\" : [ 100, 100 ]\r\n" + "    }\r\n"
				+ "  },\r\n" + "  \"modifiedAt\" : \"2022-01-30T21:21:29Z\"\r\n" + "} ]\r\n" + "}";

	}

	@AfterEach
	public void tearDown() {
		qPayload = null;
	}

	/**
	 * this method is use for notify the subscription
	 */
	@Test
	@Order(1)
	public void notifyTest() {

		try {
			ExtractableResponse<Response> response = RestAssured.given().body(qPayload)
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
					.post("/remotenotify/{id}", "-4670513482090232260").then().statusCode(Status.OK.getStatusCode())
					.statusCode(200).extract();
			assertEquals(200, response.statusCode());

		} catch (Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}

	}

}