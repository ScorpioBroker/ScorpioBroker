package eu.neclab.ngsildbroker.entityhandler.controller;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.results.NGSILDOperationResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.entityhandler.services.EntityService;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.mockito.InjectMock;
import io.restassured.RestAssured;
import io.restassured.response.ExtractableResponse;
import io.restassured.response.Response;
import io.smallrye.mutiny.Uni;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.mockito.Mockito;

import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response.Status;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;

@QuarkusTest
@TestProfile(CustomProfile.class)
@TestMethodOrder(OrderAnnotation.class)
public class EntityControllerTest {

    private String appendPayload;
    private String invalidAppendPayload;
    private String entityPayload;
    private String partialUpdatePayload;
    private String partialUpdateDefaultCasePayload;
    private String incorrectPayload;

    @InjectMock
    EntityService entityService;

    @BeforeEach
    public void setup() throws Exception { //@formatter:off
        entityPayload = "{\r\n"
                + "    \"id\": \"urn:test:testentity1\",\r\n"
                + "    \"type\": \"TestType\",\r\n"
                + "    \"stringproperty\": {\r\n"
                + "        \"type\": \"Property\",\r\n"
                + "        \"value\": \"teststring\"\r\n"
                + "    },\r\n"
                + "    \"intproperty\": {\r\n"
                + "        \"type\": \"Property\",\r\n"
                + "        \"value\": 123\r\n"
                + "    },\r\n"
                + "    \"floatproperty\": {\r\n"
                + "        \"type\": \"Property\",\r\n"
                + "        \"value\": 123.456\r\n"
                + "    },\r\n"
                + "    \"complexproperty\": {\r\n"
                + "        \"type\": \"Property\",\r\n"
                + "        \"value\": {\r\n"
                + "            \"some\": {\r\n"
                + "                \"more\": \"values\"\r\n"
                + "            }\r\n"
                + "        }\r\n"
                + "    },\r\n"
                + "    \"testrelationship\": {\r\n"
                + "        \"type\": \"Relationship\",\r\n"
                + "        \"object\": \"urn:testref1\"\r\n"
                + "    }\r\n"
                + "}";


        incorrectPayload = "{\r\n    \"ids\": \"urn:ngsi-ld:testunit:154\",\r\n    \"type\": \"Vehicle\",\r\n    \"brandName\": [\r\n        {\r\n              \r\n            \"type\": \"Property\",\r\n            \"value\": \"good\",\r\n            \"observedAt\": \"2018-08-07T12:00:00Z\"\r\n        }\r\n    ]\r\n}";

        appendPayload = "{\r\n" + "	\"brandName1\": {\r\n" +
                "		\"type\": \"Property\",\r\n" + "		\"value\": \"BMW\"\r\n" +
                "	}\r\n" + "}";
        invalidAppendPayload = "{\r\n" + "	\"brandName1\": {\r\n" +
                "		\"type\": \"property\",\r\n" + "		\"value\": \"BMW\"\r\n" +
                "	}\r\n" + "}";

        partialUpdatePayload = "{\r\n" + "		\"value\": 20,\r\n"
                + "		\"datasetId\": \"urn:ngsi-ld:Property:speedometerA4567-speed\"\r\n"
                + "}";

        partialUpdateDefaultCasePayload = "{\r\n" + "		\"value\": 11\r\n" + "}";

    }

    @AfterEach
    public void tearDown() {
        appendPayload = null;
        invalidAppendPayload = null;
        entityPayload = null;
        partialUpdatePayload = null;
        partialUpdateDefaultCasePayload = null;
        incorrectPayload = null;

    }

    /**
     * this method is use for create the entity
     */

    @Test
    public void createEntityTest() {

        Mockito.when(entityService.createEntity(any(), any(), any(), any()))
                .thenReturn(Uni.createFrom().item(new NGSILDOperationResult(AppConstants.CREATE_REQUEST, "urn:test:testentity1")));

        ExtractableResponse<Response> response = RestAssured.given()
                .header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
                .header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD)
                .body(entityPayload).when()
                .post("/ngsi-ld/v1/entities").then()
                .statusCode(201).statusCode(201).extract();
        int statusCode = response.statusCode();
        assertEquals(201, statusCode);


    }


    /**
     * this method is use for the entity if entity already exist
     */

    @Test
    public void createEntityAlreadyExistTest() {
        Mockito.when(entityService.createEntity(any(), any(), any(), any()))
                .thenReturn(Uni.createFrom().failure(new ResponseException(ErrorType.AlreadyExists)));
        ExtractableResponse<Response> response = RestAssured.given()
                .header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
                .header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD)
                .body(entityPayload).when()
                .post("/ngsi-ld/v1/entities").then()
                .statusCode(409).statusCode(409).extract();
        int statusCode = response.statusCode();
        assertEquals(409, statusCode);

    }


    //	/**
//	 * this method is validate for the bad request if create the entity
//	 */
    @Test
    public void createEntityBadRequestTest() {
        Mockito.when(entityService.createEntity(any(), any(), any(), any()))
                .thenReturn(Uni.createFrom().failure(new ResponseException(ErrorType.BadRequestData)));
        ExtractableResponse<Response> response = RestAssured.given()
                .header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
                .header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD)
                .body(incorrectPayload).when()
                .post("/ngsi-ld/v1/entities").then()
                .statusCode(400).statusCode(400).extract();
        int statusCode = response.statusCode();
        assertEquals(400, statusCode);


    }


    /**
     * THIS METHOD IS USE FOR APPEND THE ATTRIBUTE IN ENTITY
     */

    @Test
    public void updateEntityTest() {
        Mockito.when(entityService.updateEntity(any(), any(), any(), any(), any()))
                .thenReturn(Uni.createFrom()
                        .item(new NGSILDOperationResult(AppConstants.ENTITY_UPDATE_PAYLOAD, "urn:test:testentity")));
        String entityId = "urn:test:testentity";
        ExtractableResponse<Response> response = RestAssured.given()
                .header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
                .header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD)
                .body(appendPayload).when()
                .patch("/ngsi-ld/v1/entities/" + entityId + "/attrs").then()
                .statusCode(204).statusCode(204).extract();
        int statusCode = response.statusCode();
        assertEquals(204, statusCode);


    }

    /**
     * this method is validate the bad request if append the attribute in entity
     */
    @Test
    public void updateEntityBadRequestTest() {
        Mockito.when(entityService.updateEntity(any(), any(), any(), any(), any()))
                .thenReturn(Uni.createFrom()
                        .item(new NGSILDOperationResult(AppConstants.ENTITY_UPDATE_PAYLOAD, "urn:test:testentity")));
        String entityId = "urn:test:testentity1";
        ExtractableResponse<Response> response = RestAssured.given()
                .header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
                .header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD)
                .body(invalidAppendPayload).when()
                .patch("/ngsi-ld/v1/entities/" + entityId + "/attrs").then()
                .statusCode(400).extract();
        int statusCode = response.statusCode();
        assertEquals(400, statusCode);


    }


    @Test
    public void appendEntityTest() {
        Mockito.when(entityService.appendToEntity(any(), any(), any(), anyBoolean(), any(), any()))
                .thenReturn(Uni.createFrom()
                        .item(new NGSILDOperationResult(AppConstants.ENTITY_UPDATE_PAYLOAD, "urn:test:testentity1")));
        ExtractableResponse<Response> response = RestAssured.given()
                .header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
                .header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).body(appendPayload).when()
                .post("/ngsi-ld/v1/entities/{entityId}/attrs", "urn:test:testentity1").then()
                .statusCode(204).extract();
        int statusCode = response.statusCode();
        assertEquals(204, statusCode);

    }


    /**
     * this method is validate the bad request if append the attribute in entity
     */

    @Test
    public void appendEntityBadRequestTest() {
        Mockito.when(entityService.appendToEntity(any(), any(), any(), anyBoolean(), any(), any()))
                .thenReturn(Uni.createFrom()
                        .item(new NGSILDOperationResult(AppConstants.ENTITY_UPDATE_PAYLOAD, "urn:test:testentity1")));
        ExtractableResponse<Response> response = RestAssured.given()
                .header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
                .header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).body(invalidAppendPayload).when()
                .post("/ngsi-ld/v1/entities/{entityId}/attrs", "urn:test:testentity1").then()
                .statusCode(400).extract();
        int statusCode = response.statusCode();
        assertEquals(400, statusCode);

    }

    /**
     * this method is validate the QueryParam
     */

    @Test
    public void appendEntityTestQueryParam() {
        Mockito.when(entityService.appendToEntity(any(), any(), any(), anyBoolean(), any(), any()))
                .thenReturn(Uni.createFrom()
                        .item(new NGSILDOperationResult(AppConstants.ENTITY_UPDATE_PAYLOAD, "urn:test:testentity1")));
        String options = "noOverwrite";
        ExtractableResponse<Response> response = RestAssured.given()
                .header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
                .header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD)
                .body(appendPayload)
                .queryParam("options", options)
                .when()
                .post("/ngsi-ld/v1/entities/{entityId}/attrs", "urn:test:testentity1").then()
                .statusCode(204).extract();
        int statusCode = response.statusCode();
        assertEquals(204, statusCode);

    }


    /**
     * this method is use for partial update the attribute
     */

    @Test
    public void partialUpdateAttributeTest() {

        Mockito.when(entityService.partialUpdateAttribute(any(), any(), any(), any(), any(), any()))
                .thenReturn(Uni.createFrom()
                        .item(new NGSILDOperationResult(AppConstants.ENTITY_ATTRS_UPDATE_PAYLOAD, "urn:ngsi-ld:Vehicle:A101")));

        ExtractableResponse<Response> response = RestAssured.given()
                .header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
                .header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD)
                .body(partialUpdatePayload)
                .when()
                .patch("/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}", "urn:ngsi-ld:Vehicle:A101", "speed")
                .then()
                .statusCode(204).extract();
        int statusCode = response.statusCode();
        assertEquals(204, statusCode);

    }


    @Test
    public void partialUpdateAttributeBadRequestTest() {

        Mockito.when(entityService.partialUpdateAttribute(any(), any(), any(), any(), any(), any()))
                .thenReturn(Uni.createFrom()
                        .item(new NGSILDOperationResult(AppConstants.ENTITY_ATTRS_UPDATE_PAYLOAD, "urn:ngsi-ld:Vehicle:A101")));

        ExtractableResponse<Response> response = RestAssured.given()
                .header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
                .header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD)
                .body(partialUpdatePayload)
                .when()
                .patch("/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}", " :ngsi-ld:Vehicle:A101", "speed")
                .then()
                .extract();
        int statusCode = response.statusCode();
        assertEquals(400, statusCode);

    }

    /**
     * this method is use for partial update the attribute if datasetId is not exist
     */

    @Test
    public void partialUpdateAttributeNotExistTest() {
        Mockito.when(entityService.partialUpdateAttribute(any(), any(), any(), any(), any(), any()))
                .thenReturn(Uni.createFrom()
                        .item(new NGSILDOperationResult(AppConstants.ENTITY_ATTRS_UPDATE_PAYLOAD, "urn:ngsi-ld:Vehicle:A101")));

        ExtractableResponse<Response> response = RestAssured.given()
                .header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
                .header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).body(partialUpdatePayload).when()
                .patch("/ngsi-ld/v1/entitiess/{entityId}/attrs/{attrId}", "urn:ngsi-ld:Vehicle:A101", "speed12")
                .then().statusCode(Status.NOT_FOUND.getStatusCode()).extract();
        int statusCode = response.statusCode();
        assertEquals(404, statusCode);

    }


    /**
     * this method is use for partial update attribute default datasetId
     */

    @Test
    public void partialUpdateAttributeDefaultDatasetIdCaseTest() {
        Mockito.when(entityService.partialUpdateAttribute(any(), any(), any(), any(), any(), any()))
                .thenReturn(Uni.createFrom()
                        .item(new NGSILDOperationResult(AppConstants.ENTITY_ATTRS_UPDATE_PAYLOAD, "urn:ngsi-ld:Vehicle:A101")));

        ExtractableResponse<Response> response = RestAssured.given()
                .header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
                .header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD)
                .body(partialUpdateDefaultCasePayload).when()
                .patch("/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}", "urn:ngsi-ld:Vehicle:A101", "speed").then()
                .statusCode(Status.NO_CONTENT.getStatusCode()).statusCode(204).extract();
        int statusCode = response.statusCode();
        assertEquals(204, statusCode);

    }


    @Test
    public void deleteAttributeTest() {
        Mockito.when(entityService.deleteAttribute(any(), any(), any(), any(), anyBoolean(), any(), any()))
                .thenReturn(Uni.createFrom()
                        .item(new NGSILDOperationResult(AppConstants.DELETE_REQUEST, "urn:test:testentity1")));

        ExtractableResponse<Response> response = RestAssured.given()
                .header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
                .header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD)
                .when()
                .delete("/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}",
                        "urn:ngsi-ld:Vehicle:A101", "speed")
                .then().statusCode(204).extract();
        int statusCode = response.statusCode();
        assertEquals(204, statusCode);

    }

    @Test
    public void deleteAttributeBadRequestTest() {
        Mockito.when(entityService.deleteAttribute(any(), any(), any(), any(), anyBoolean(), any(), any()))
                .thenReturn(Uni.createFrom()
                        .item(new NGSILDOperationResult(AppConstants.DELETE_REQUEST, "urn:test:testentity1")));

        ExtractableResponse<Response> response = RestAssured.given()
                .header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
                .header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD)
                .when()
                .delete("/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}",
                        " :ngsi-ld:Vehicle:A101", "speed")
                .then().extract();
        int statusCode = response.statusCode();
        assertEquals(400, statusCode);

    }


    /**
     * this method is use for delete the entity
     */

    @Test
    public void deleteEntityTest() {
        Mockito.when(entityService.deleteAttribute(any(), any(), any(), any(), anyBoolean(), any(), any()))
                .thenReturn(Uni.createFrom()
                        .item(new NGSILDOperationResult(AppConstants.DELETE_REQUEST, "urn:test:testentity1")));
        ExtractableResponse<Response> response = RestAssured.given().accept(AppConstants.NGB_APPLICATION_JSONLD).request()
                .contentType(AppConstants.NGB_APPLICATION_JSON).when()
                .delete("/ngsi-ld/v1/entities/{entityId}", "urn:test:testentity1")
                .then().statusCode(204).extract();
        int statusCode = response.statusCode();
        assertEquals(204, statusCode);

    }

    @Test
    public void deleteEntityBadRequestTest() {
        Mockito.when(entityService.deleteAttribute(any(), any(), any(), any(), anyBoolean(), any(), any()))
                .thenReturn(Uni.createFrom()
                        .item(new NGSILDOperationResult(AppConstants.DELETE_REQUEST, "urn:test:testentity1")));
        ExtractableResponse<Response> response = RestAssured.given().accept(AppConstants.NGB_APPLICATION_JSONLD).request()
                .contentType(AppConstants.NGB_APPLICATION_JSON).when()
                .delete("/ngsi-ld/v1/entities/{entityId}", " :ngsi-ld:Vehicle:A101")
                .then().statusCode(400).extract();
        int statusCode = response.statusCode();
        assertEquals(400, statusCode);

    }


}
