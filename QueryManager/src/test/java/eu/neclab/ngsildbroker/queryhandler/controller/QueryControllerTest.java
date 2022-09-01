package eu.neclab.ngsildbroker.queryhandler.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static io.restassured.RestAssured.given;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
import javax.ws.rs.core.Response.Status;
@QuarkusTest
@TestMethodOrder(OrderAnnotation.class)
@TestProfile(CustomProfile.class)
public class QueryControllerTest {

	private String entity;
	private String response = "";
	private List<String> entities;

	@BeforeEach
	public void setup() {
		entity = "[{\r\n" + "	\"http://example.org/vehicle/brandName\": [{\r\n"
				+ "		\"@type\": [\"https://uri.etsi.org/ngsi-ld/Property\"],\r\n"
				+ "		\"https://uri.etsi.org/ngsi-ld/hasValue\": [{\r\n" + "			\"@value\": \"Mercedes\"\r\n"
				+ "		}]\r\n" + "	}],\r\n" + "	\"https://uri.etsi.org/ngsi-ld/createdAt\": [{\r\n"
				+ "		\"@type\": \"https://uri.etsi.org/ngsi-ld/DateTime\",\r\n"
				+ "		\"@value\": \"2017-07-29T12:00:04Z\"\r\n" + "	}],\r\n"
				+ "	\"@id\": \"urn:ngsi-ld:Vehicle:A100\",\r\n" + "	\"http://example.org/common/isParked\": [{\r\n"
				+ "		\"https://uri.etsi.org/ngsi-ld/hasObject\": [{\r\n"
				+ "			\"@id\": \"urn:ngsi-ld:OffStreetParking:Downtown1\"\r\n" + "		}],\r\n"
				+ "		\"https://uri.etsi.org/ngsi-ld/observedAt\": [{\r\n"
				+ "			\"@type\": \"https://uri.etsi.org/ngsi-ld/DateTime\",\r\n"
				+ "			\"@value\": \"2017-07-29T12:00:04Z\"\r\n" + "		}],\r\n"
				+ "		\"http://example.org/common/providedBy\": [{\r\n"
				+ "			\"https://uri.etsi.org/ngsi-ld/hasObject\": [{\r\n"
				+ "				\"@id\": \"urn:ngsi-ld:Person:Bob\"\r\n" + "			}],\r\n"
				+ "			\"@type\": [\"https://uri.etsi.org/ngsi-ld/Relationship\"]\r\n" + "		}],\r\n"
				+ "		\"@type\": [\"https://uri.etsi.org/ngsi-ld/Relationship\"]\r\n" + "	}],\r\n"
				+ "	\"https://uri.etsi.org/ngsi-ld/location\": [{\r\n"
				+ "		\"@type\": [\"https://uri.etsi.org/ngsi-ld/GeoProperty\"],\r\n"
				+ "		\"https://uri.etsi.org/ngsi-ld/hasValue\": [{\r\n"
				+ "			\"@value\": \"{ \\\"type\\\":\\\"Point\\\", \\\"coordinates\\\":[ -8.5, 41.2 ] }\"\r\n"
				+ "		}]\r\n" + "	}],\r\n" + "	\"http://example.org/vehicle/speed\": [{\r\n"
				+ "		\"@type\": [\"https://uri.etsi.org/ngsi-ld/Property\"],\r\n"
				+ "		\"https://uri.etsi.org/ngsi-ld/hasValue\": [{\r\n" + "			\"@value\": 80\r\n"
				+ "		}]\r\n" + "	}],\r\n" + "	\"@type\": [\"http://example.org/vehicle/Vehicle\"]\r\n" + "}]";

		response = "{\r\n" + "	\"id\": \"urn:ngsi-ld:Vehicle:A100\",\r\n" + "	\"type\": \"Vehicle\",\r\n"
				+ "	\"brandName\": {\r\n" + "		\"type\": \"Property\",\r\n" + "		\"value\": \"Mercedes\"\r\n"
				+ "	},\r\n" + "	\"isParked\": {\r\n" + "		\"type\": \"Relationship\",\r\n"
				+ "		\"object\": \"urn:ngsi-ld:OffStreetParking:Downtown1\",\r\n"
				+ "		\"observedAt\": \"2017-07-29T12:00:04Z\",\r\n" + "		\"providedBy\": {\r\n"
				+ "			\"type\": \"Relationship\",\r\n" + "			\"object\": \"urn:ngsi-ld:Person:Bob\"\r\n"
				+ "		}\r\n" + "	},\r\n" + "	\"speed\": {\r\n" + "		\"type\": \"Property\",\r\n"
				+ "		\"value\": 80\r\n" + "	},\r\n" + "	\"createdAt\": \"2017-07-29T12:00:04Z\",\r\n"
				+ "	\"location\": {\r\n" + "		\"type\": \"GeoProperty\",\r\n"
				+ "		\"value\": { \"type\":\"Point\", \"coordinates\":[ -8.5, 41.2 ] }\r\n" + "	}\r\n" + "}";

		entities = new ArrayList<String>(Arrays.asList("{\r\n" + "    \"id\": \"urn:ngsi-ld:Vehicle:A101\",\r\n"
				+ "    \"type\": \"Vehicle\",\r\n" + "    \"brandName\": {\r\n" + "        \"type\": \"Property\",\r\n"
				+ "        \"value\": \"Mercedes\"\r\n" + "    },\r\n" + "    \"speed\": [\r\n" + "        {\r\n"
				+ "            \"type\": \"Property\",\r\n"
				+ "            \"datasetId\": \"urn:ngsi-ld:Property:speedometerA4567-speed\",\r\n"
				+ "            \"source\": {\r\n" + "                \"type\": \"Property\",\r\n"
				+ "                \"value\": \"Speedometer\"\r\n" + "            },\r\n"
				+ "            \"value\": 55\r\n" + "        },\r\n" + "        {\r\n"
				+ "            \"type\": \"Property\",\r\n" + "            \"source\": {\r\n"
				+ "                \"type\": \"Property\",\r\n" + "                \"value\": \"GPS\"\r\n"
				+ "            },\r\n" + "            \"value\": 60\r\n" + "        },\r\n" + "        {\r\n"
				+ "            \"type\": \"Property\",\r\n" + "            \"source\": {\r\n"
				+ "                \"type\": \"Property\",\r\n" + "                \"value\": \"GPS_NEW\"\r\n"
				+ "            },\r\n" + "            \"value\": 52.5\r\n" + "        }\r\n" + "    ],\r\n"
				+ "    \"location\": {\r\n" + "        \"type\": \"GeoProperty\",\r\n" + "        \"value\": {\r\n"
				+ "            \"type\": \"Point\",\r\n" + "            \"coordinates\": [\r\n"
				+ "                -8.5,\r\n" + "                41.2\r\n" + "            ]\r\n" + "        }\r\n"
				+ "    }\r\n" + "}"));

	}

	@AfterEach
	public void tearDown() {
		entity = null;
		response = null;
		entities = null;
	}

	/**
	 * this method is use to validate Enitity Not Found
	 */
	@Test
	@Order(1)
	public void getEntityByIdNotFoundTest() throws Exception {
		try {
			ExtractableResponse<Response> response = given().accept(AppConstants.NGB_APPLICATION_JSONLD).request()
					.contentType(AppConstants.NGB_APPLICATION_JSON).when()
					.get("/ngsi-ld/v1/entities/urn:ngsi-ld:Vehicle:A102").then().statusCode(404).extract();
			assertEquals(404, response.statusCode());
		} catch (Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}

	}
	
	/**
	 * this method is use to validate get query for entity type
	 */
	@Test
	@Order(2)
	public void getQueryForEntitiesTest() throws Exception {
		try {
			ExtractableResponse<Response> response = given().accept(AppConstants.NGB_APPLICATION_JSONLD).request()
					.contentType(AppConstants.NGB_APPLICATION_JSON).when().get("/ngsi-ld/v1/entities?type=v").then()
					.statusCode(Status.OK.getStatusCode()).statusCode(200).extract();
			assertEquals(200, response.statusCode());
		} catch (Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}

	}

	/**
	 * this method is use to Validate get query BadRequest
	 */
	@Test
	@Order(3)
	public void getQueryForEntitiesBadRequestTest() {
		try {
			ExtractableResponse<Response> response = given().accept(AppConstants.NGB_APPLICATION_JSONLD).request()
					.contentType(AppConstants.NGB_APPLICATION_JSON).when().get("/ngsi-ld/v1/entities?type1=Test50231.1")
					.then().statusCode(400).extract();
			assertEquals(400, response.statusCode());
		} catch (Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}

	}

	/**
	 * this method is use for get All Entites based on Type
	 */
	@Test
	@Order(4)
	public void getAllTypesTest() {
		try {
			ExtractableResponse<Response> response = given().accept(AppConstants.NGB_APPLICATION_JSONLD).request()
					.contentType(AppConstants.NGB_APPLICATION_JSON).when().get("/ngsi-ld/v1/types").then()
					.statusCode(Status.OK.getStatusCode()).statusCode(200).extract();
			assertEquals(200, response.statusCode());
		} catch (Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}

	}

	/**
	 * this method is use to validate get type Not Found
	 */
	@Test
	@Order(5)
	public void getTypeByIdNotFoundTest() throws Exception {
		try {
			ExtractableResponse<Response> response = given().accept(AppConstants.NGB_APPLICATION_JSONLD).request()
					.contentType(AppConstants.NGB_APPLICATION_JSON).when().get("/ngsi-ld/v1/types/{entityType}", "vhic")
					.then().statusCode(Status.NOT_FOUND.getStatusCode()).statusCode(404).extract();
			assertEquals(404, response.statusCode());
		} catch (Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}
	}

	/**
	 * this method is use to validate get all attribute 
	 */
	@Test
	@Order(6)
	public void getAllAttributesTest() throws Exception {
		try {
			ExtractableResponse<Response> response = given().accept(AppConstants.NGB_APPLICATION_JSONLD).request()
					.contentType(AppConstants.NGB_APPLICATION_JSON).when().get("/ngsi-ld/v1/attributes").then()
					.statusCode(Status.OK.getStatusCode()).statusCode(200).extract();
			assertEquals(200, response.statusCode());
		} catch (Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}

	}

	/**
	 * this method is use to validate get attribute Not Found
	 */
	@Test
	@Order(7)
	public void getAttributeByAttrNotFoundTest() throws Exception {
		try {
			ExtractableResponse<Response> response = given().accept(AppConstants.NGB_APPLICATION_JSONLD).request()
					.contentType(AppConstants.NGB_APPLICATION_JSON).when()
					.get("/ngsi-ld/v1/attributes/{attribute}", "bm1").then()
					.statusCode(Status.NOT_FOUND.getStatusCode()).statusCode(404).extract();
			assertEquals(404, response.statusCode());
		} catch (Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}

	}

}
