package eu.neclab.ngsildbroker.historymanager.controller;

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertEquals;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response.Status;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.junit.jupiter.api.Order;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.results.NGSILDOperationResult;
import eu.neclab.ngsildbroker.historyentitymanager.service.HistoryEntityService;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.mockito.InjectMock;
import io.restassured.response.ExtractableResponse;
import io.restassured.response.Response;
import io.smallrye.mutiny.Uni;

import static org.mockito.ArgumentMatchers.any;

@QuarkusTest
@TestProfile(CustomProfile.class)
public class HistoryControllerTest {

    private String temporalPayloadBad;
    private String temporalPayload;
    private String addAttrsPayload;
    private String payload;

    @InjectMock
    private HistoryEntityService historyEntityService;

    @BeforeEach
    public void setup() throws Exception {
        // @formatter:on
    	MockitoAnnotations.openMocks(this);
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
    public void createTemporalEntityTest() throws Exception {

        Mockito.when(historyEntityService.createEntry(any(), any(), any(), any()))
                .thenReturn(Uni.createFrom().item(new NGSILDOperationResult(AppConstants.CREATE_REQUEST, "urn:test:testentity1")));
        ExtractableResponse<Response> response = given().body(temporalPayload)
                .header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
                .header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
                .post("/ngsi-ld/v1/temporal/entities").then().statusCode(Status.CREATED.getStatusCode()).statusCode(201)
                .extract();
        int statsCode = response.statusCode();
        assertEquals(201, statsCode);

    }

    /**
     * this method is use to validate BadRequest
     */
    @Test
    public void createTemporalEntityBadRequestTest() throws Exception {

        Mockito.when(historyEntityService.createEntry(any(), any(), any(), any()))
                .thenReturn(Uni.createFrom().item(new NGSILDOperationResult(AppConstants.CREATE_REQUEST, "urn:test:testentity1")));
        ExtractableResponse<Response> response = given().body(temporalPayloadBad)
                .header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
                .header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
                .post("/ngsi-ld/v1/temporal/entities")
                .then()
                .extract();
        int statsCode = response.statusCode();
        assertEquals(400, statsCode);

    }


    /**
     * this method is use for delete the temporalEntity by Id
     */
    @Test
    public void deleteTemporalEntityTest() throws Exception {
        Mockito.when(historyEntityService.deleteEntry(any(), any(), any(), any()))
                .thenReturn(Uni.createFrom().item(new NGSILDOperationResult(AppConstants.DELETE_REQUEST, "urn:ngsi-ld:testunit:a0a")));

        ExtractableResponse<Response> response = given()
                .header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
                .header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
                .delete("/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:testunit:a0a/").then()
                .statusCode(Status.NO_CONTENT.getStatusCode()).statusCode(204).extract();
        int statsCode = response.statusCode();
        assertEquals(204, statsCode);

    }

    /**
     * this method is use for delete the temporalEntity by Id
     */
    @Test
    public void deleteTemporalEntityBadRequestTest() throws Exception {
        Mockito.when(historyEntityService.deleteEntry(any(), any(), any(), any()))
                .thenReturn(Uni.createFrom().item(new NGSILDOperationResult(AppConstants.DELETE_REQUEST, "urn:ngsi-ld:testunit:a0a")));

        ExtractableResponse<Response> response = given()
                .header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
                .header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
                .delete("/ngsi-ld/v1/temporal/entities/ :ngsi-ld:testunit:a0a/").then()
                .extract();
        int statsCode = response.statusCode();
        assertEquals(400, statsCode);

    }

    /**
     * this method is use for delete the temporalEntity by attribute
     */
    @Test
    public void deleteTemporalEntityByAttrTest() throws Exception {
        Mockito.when(historyEntityService.appendToEntry(any(), any(), any(), any(), any()))
                .thenReturn(Uni.createFrom().item(new NGSILDOperationResult(AppConstants.DELETE_REQUEST, "urn:ngsi-ld:testunit:a0a")));

        ExtractableResponse<Response> response = given()
                .header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
                .header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
                .delete("/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:testunit:a0a/attrs/airQualityLeve2").then()
                .extract();
        int statsCode = response.statusCode();
        assertEquals(204, statsCode);

    }

    /**
     * this method is use to validate BadRequest by attribute
     */
    @Test
    public void deleteTemporalEntityByAttrBadRequestTest() {
        Mockito.when(historyEntityService.appendToEntry(any(), any(), any(), any(), any()))
                .thenReturn(Uni.createFrom().item(new NGSILDOperationResult(AppConstants.DELETE_REQUEST, "urn:ngsi-ld:testunit:a0a")));

        ExtractableResponse<Response> response = given()
                .header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
                .header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
                .delete("/ngsi-ld/v1/temporal/entities/ :ngsi-ld:testunit:a0a/attrs/airQualityLeve2").then()
                .extract();
        int statsCode = response.statusCode();
        assertEquals(400, statsCode);

    }


    /**
     * this method is use for delete the temporalEntity by attribute
     */
    @Test
    public void addAttrib2TemopralEntityTest() throws Exception {
        Mockito.when(historyEntityService.appendToEntry(any(), any(), any(), any(), any()))
                .thenReturn(Uni.createFrom().item(new NGSILDOperationResult(AppConstants.DELETE_REQUEST, "urn:ngsi-ld:testunit:a0a")));

        ExtractableResponse<Response> response = given()
                .header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
                .header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD)
                .body(addAttrsPayload)
                .when()
                .post("/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:testunit:a0a/attrs/").then()
                .extract();
        int statsCode = response.statusCode();
        assertEquals(204, statsCode);

    }

    /**
     * this method is use to validate BadRequest by attribute
     */
    @Test
    public void addAttrib2TemopralEntityBadRequestTest() {
        Mockito.when(historyEntityService.appendToEntry(any(), any(), any(), any(), any()))
                .thenReturn(Uni.createFrom().item(new NGSILDOperationResult(AppConstants.DELETE_REQUEST, "urn:ngsi-ld:testunit:a0a")));

        ExtractableResponse<Response> response = given()
                .header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
                .header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD)
                .body(addAttrsPayload)
                .when()
                .post("/ngsi-ld/v1/temporal/entities/ :ngsi-ld:testunit:a0a/attrs").then()
                .extract();
        int statsCode = response.statusCode();
        assertEquals(400, statsCode);

    }


    /**
     * this method is use to  append attribute by using  instance id
     */
    @Test
    @Order(17)
    public void modifyAttribInstanceTemporalEntityTest() throws Exception {
        Mockito.when(historyEntityService.updateInstanceOfAttr(any(), any(), any(), any(), any(), any(), any()))
                .thenReturn(Uni.createFrom().item(new NGSILDOperationResult(AppConstants.DELETE_REQUEST, "urn:ngsi-ld:testunit:a0a")));

        ExtractableResponse<Response> response = given()
                .header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
                .header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD)
                .body(addAttrsPayload)
                .when()
                .patch("/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:testunit:a0a/attrs/airQualityLevel2/urn:ngsi-ld:9ad1acfd-5d4b-4c92-8463-852e192cd152/")
                .then().extract();
        int statsCode = response.statusCode();
        assertEquals(204, statsCode);

    }

    /**
     * this method is use to validate  BadRequest append attribute by using  instance id
     */
    @Test
    public void modifyAttribInstanceTemporalEntityBadRequestTest() throws Exception {
        Mockito.when(historyEntityService.updateInstanceOfAttr(any(), any(), any(), any(), any(), any(), any()))
                .thenReturn(Uni.createFrom().item(new NGSILDOperationResult(AppConstants.DELETE_REQUEST, "urn:ngsi-ld:testunit:a0a")));

        ExtractableResponse<Response> response = given()
                .header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
                .header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD)
                .body(addAttrsPayload)
                .when()
                .patch("/ngsi-ld/v1/temporal/entities/ :ngsi-ld:testunit:a0a/attrs/airQualityLevel2/urn:ngsi-ld:9ad1acfd-5d4b-4c92-8463-852e192cd152/")
                .then().extract();
        int statsCode = response.statusCode();
        assertEquals(400, statsCode);

    }

    /**
     * this method is use for delete the temporalEntity by instance
     */
    @Test
    public void deleteAtrribInstanceTemporalEntityTest() throws Exception {
        Mockito.when(historyEntityService.deleteInstanceOfAttr(any(), any(), any(), any(), any(), any()))
                .thenReturn(Uni.createFrom().item(new NGSILDOperationResult(AppConstants.DELETE_REQUEST, "urn:ngsi-ld:testunit:a0a")));

        ExtractableResponse<Response> response = given()
                .header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
                .header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
                .delete("/ngsi-ld/v1/temporal/entities/urn:ngsi-ld:testunit:a0a/attrs/airQualityLevel2/urn:ngsi-ld:9ad1acfd-5d4b-4c92-8463-852e192cd152")
                .then().extract();
        int statsCode = response.statusCode();
        assertEquals(204, statsCode);

    }


    /**
     * this method is use for delete the temporalEntity by instance
     */
    @Test
    public void deleteAtrribInstanceTemporalEntityBadRequestTest() throws Exception {
        Mockito.when(historyEntityService.deleteInstanceOfAttr(any(), any(), any(), any(), any(), any()))
                .thenReturn(Uni.createFrom().item(new NGSILDOperationResult(AppConstants.DELETE_REQUEST, "urn:ngsi-ld:testunit:a0a")));

        ExtractableResponse<Response> response = given()
                .header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
                .header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
                .delete("/ngsi-ld/v1/temporal/entities/ :ngsi-ld:testunit:a0a/attrs/airQualityLevel2/urn:ngsi-ld:9ad1acfd-5d4b-4c92-8463-852e192cd152\r\n")
                .then().extract();
        int statsCode = response.statusCode();
        assertEquals(400, statsCode);

    }


}
