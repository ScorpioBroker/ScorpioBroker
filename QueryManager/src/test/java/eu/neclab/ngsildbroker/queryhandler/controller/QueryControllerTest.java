package eu.neclab.ngsildbroker.queryhandler.controller;

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import jakarta.ws.rs.core.HttpHeaders;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.mockito.Mockito;

import com.google.common.collect.Lists;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.results.QueryResult;
import eu.neclab.ngsildbroker.queryhandler.services.QueryService;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;

import io.quarkus.test.junit.mockito.MockitoConfig;
import io.restassured.RestAssured;
import io.restassured.response.ExtractableResponse;
import io.restassured.response.Response;
import io.smallrye.mutiny.Uni;

@QuarkusTest
@TestMethodOrder(OrderAnnotation.class)
@TestProfile(CustomProfile.class)
public class QueryControllerTest {

	private String entity;
	private String response = "";
	private List<String> entities;
	private String payloadContext = "";

	@MockitoConfig
	QueryService queryService;

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

		payloadContext = "{\r\n" + "    \"type\": \"Query\",\r\n" + "    \"entities\": [\r\n" + "        {\r\n"
				+ "            \"id\": \"urn:test:testentity1\",\r\n" + "            \"type\": \"TestType\"\r\n"
				+ "        }\r\n" + "    ],\r\n" + "    \"@context\": [\r\n"
				+ "        \"https://raw.githubusercontent.com/ScorpioBroker/ScorpioBroker/new_ci/testcontext.json\",\r\n"
				+ "        \"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.3.jsonld\"\r\n" + "    ]\r\n"
				+ "}";

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
	public void queryTypeTest() {
		QueryResult mockResutl = Mockito.mock(QueryResult.class);

		Mockito.when(queryService.query(any(), any(), anyBoolean(), any(), any(), any(), any(), any(), any(), any(),
				anyInt(), anyInt(), anyBoolean(), anyBoolean(), any(), any(), anyBoolean(), any(), any(), any(), any(),
				any(), any(), any(), any(), any(), any())).thenReturn(Uni.createFrom().item(mockResutl));

		ExtractableResponse<Response> response = given().accept(AppConstants.NGB_APPLICATION_JSONLD).request()
				.contentType(AppConstants.NGB_APPLICATION_JSON).when().get("/ngsi-ld/v1/entities?type=Vehicle").then()
				.extract();
		assertEquals(200, response.statusCode());

	}

	@Test
	public void queryAcceptHeaderTest() {
		QueryResult mockResutl = Mockito.mock(QueryResult.class);
		int limit = 200;
		Mockito.when(queryService.query(any(), any(), anyBoolean(), any(), any(), any(), any(), any(),
				any(), any(), anyInt(), anyInt(), anyBoolean(), anyBoolean(), any(), any(), anyBoolean(), any(), any(),
				any(), any(), any(), any(), any(), any(), any(), any())).thenReturn(Uni.createFrom().item(mockResutl));

		ExtractableResponse<Response> response = given().accept(AppConstants.NGB_APPLICATION_JSONLD).request()
				.contentType(AppConstants.NGB_APPLICATION_JSON).queryParam("limit", limit).when()
				.get("/ngsi-ld/v1/entities?type=Vehicle").then().extract();
		assertEquals(200, response.statusCode());

	}

	@Test
	public void getAllTypeswithDetailsTest() {

		List<Map<String, Object>> list = Lists.newArrayList();
		Map<String, Object> map = new HashMap<>();
		map.put("id", entity);
		list.add(map);
		Mockito.when(queryService.getTypesWithDetail(any(), anyBoolean(),any())).thenReturn(Uni.createFrom().item(list));
		Boolean details = true;

		ExtractableResponse<Response> response = given().accept(AppConstants.NGB_APPLICATION_JSONLD).request()
				.contentType(AppConstants.NGB_APPLICATION_JSON).queryParam("details", details).when()
				.get("/ngsi-ld/v1/types").then().extract();
		assertEquals(200, response.statusCode());

	}

	@Test
	public void getAllTypesWithOutDetailsTest() {

		List<Map<String, Object>> list = Lists.newArrayList();
		Map<String, Object> map = new HashMap<>();
		map.put("id", entity);
		list.add(map);
		Mockito.when(queryService.getTypesWithDetail(any(), anyBoolean(),any())).thenReturn(Uni.createFrom().item(list));
		Mockito.when(queryService.getTypes(any(), anyBoolean(),any())).thenReturn(Uni.createFrom().item(map));
		Boolean details = false;

		ExtractableResponse<Response> response = given().accept(AppConstants.NGB_APPLICATION_JSONLD).request()
				.contentType(AppConstants.NGB_APPLICATION_JSON).queryParam("details", details).when()
				.get("/ngsi-ld/v1/types").then().extract();
		assertEquals(200, response.statusCode());

	}

	@Test
	public void getTypeTest() {

		Map<String, Object> map = new HashMap<>();
		map.put("id", entity);

		Mockito.when(queryService.getType(any(), any(), anyBoolean(),any())).thenReturn(Uni.createFrom().item(map));
		Boolean details = false;

		ExtractableResponse<Response> response = given().accept(AppConstants.NGB_APPLICATION_JSONLD).request()
				.contentType(AppConstants.NGB_APPLICATION_JSON).queryParam("details", details).when()
				.get("/ngsi-ld/v1/types/{entityType}", "Vehicle").then().extract();
		assertEquals(200, response.statusCode());

	}

	@Test
	public void getAllAttributesTest() {

		Map<String, Object> map = new HashMap<>();
		map.put("id", entity);

		Mockito.when(queryService.getAttribs(any(), anyBoolean(),any())).thenReturn(Uni.createFrom().item(map));
		Boolean details = false;

		ExtractableResponse<Response> response = given().accept(AppConstants.NGB_APPLICATION_JSONLD).request()
				.contentType(AppConstants.NGB_APPLICATION_JSON).queryParam("details", details).when()
				.get("/ngsi-ld/v1/attributes").then().extract();
		assertEquals(200, response.statusCode());

	}

	@Test
	public void getAttributeTest() {

		Map<String, Object> map = new HashMap<>();
		map.put("id", entity);

		Mockito.when(queryService.getAttrib(any(), any(), anyBoolean(),any())).thenReturn(Uni.createFrom().item(map));
		Boolean details = true;

		ExtractableResponse<Response> response = given().accept(AppConstants.NGB_APPLICATION_JSONLD).request()
				.contentType(AppConstants.NGB_APPLICATION_JSON).queryParam("details", details).when()
				.get("/ngsi-ld/v1/attributes/{attribute}", "brandName").then().extract();
		assertEquals(200, response.statusCode());

	}

	@Test
	public void postQueryTest() {

		QueryResult mockResutl = Mockito.mock(QueryResult.class);
		int limit = 200;
		Mockito.when(queryService.query(any(), any(), anyBoolean(), any(), any(), any(), any(), any(),
				any(), any(), anyInt(), anyInt(), anyBoolean(), anyBoolean(), any(), any(), anyBoolean(), any(), any(),
				any(), any(), any(), any(), any(), any(), any(), any())).thenReturn(Uni.createFrom().item(mockResutl));

		ExtractableResponse<Response> response = RestAssured.given()
				.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
				.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD)
				.body("{\r\n" + "    \"type\": \"Query\",\r\n" + "    \"entities\": [\r\n" + "        {\r\n"
						+ "            \"id\": \"urn:test:testentity1\",\r\n" + "            \"type\": \"TestType\"\r\n"
						+ "        }\r\n" + "    ]\r\n" + "}")
				.when().queryParam("limit", limit).post("/ngsi-ld/v1/entityOperations/query").then().extract();
		int statusCode = response.statusCode();
		assertEquals(200, statusCode);

	}

	@Test
	@Order(1)
	public void postQueryContextTest() {

		QueryResult mockResutl = Mockito.mock(QueryResult.class);

		Mockito.when(queryService.query(any(), any(), anyBoolean(), any(), any(), any(), any(), any(),
				any(), any(), anyInt(), anyInt(), anyBoolean(), anyBoolean(), any(), any(), anyBoolean(), any(), any(),
				any(), any(), any(), any(), any(), any(), any(), any())).thenReturn(Uni.createFrom().item(mockResutl));

		ExtractableResponse<Response> response = RestAssured.given()
				.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
				.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).body(payloadContext).when()
				.post("/ngsi-ld/v1/entityOperations/query").then().extract();
		int statusCode = response.statusCode();
		assertEquals(400, statusCode);

	}

	@Test
	@Order(1)
	public void postQueryDefaultTest() {

		QueryResult mockResutl = Mockito.mock(QueryResult.class);

		Mockito.when(queryService.query(any(), any(), anyBoolean(), any(), any(), any(), any(), any(),
				any(), any(), anyInt(), anyInt(), anyBoolean(), anyBoolean(), any(), any(), anyBoolean(), any(), any(),
				any(), any(), any(), any(), any(), any(), any(), any())).thenReturn(Uni.createFrom().item(mockResutl));

		ExtractableResponse<Response> response = RestAssured.given()
				.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSONLD)
				.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).body(payloadContext).when()
				.post("/ngsi-ld/v1/entityOperations/query").then().extract();
		int statusCode = response.statusCode();
		assertEquals(400, statusCode);

	}

	@Test
	public void postQueryServiceTest() {

		QueryResult mockResutl = Mockito.mock(QueryResult.class);
		int limit = 200;
		Mockito.when(queryService.query(any(), any(), anyBoolean(), any(), any(), any(), any(), any(),
				any(), any(), anyInt(), anyInt(), anyBoolean(), anyBoolean(), any(), any(), anyBoolean(), any(), any(),
				any(), any(), any(), any(), any(), any(), any(), any())).thenReturn(Uni.createFrom().item(mockResutl));

		ExtractableResponse<Response> response = RestAssured.given()
				.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
				.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).body(" ").when()
				.queryParam("limit", limit).post("/ngsi-ld/v1/entityOperations/query").then().extract();
		int statusCode = response.statusCode();
		assertEquals(400, statusCode);

	}

}
