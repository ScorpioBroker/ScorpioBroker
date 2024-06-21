package eu.neclab.ngsildbroker.entityhandler.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;

import java.util.ArrayList;
import java.util.List;

import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response.Status;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.mockito.Mockito;

import com.google.common.collect.Sets;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.results.CRUDSuccess;
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

@QuarkusTest
@TestMethodOrder(OrderAnnotation.class)
@TestProfile(CustomProfile.class)
public class EntityBatchControllerTest {

	private String payload;
	private String badRequestPayload;
	private String badRequestPayload1;
	private String deletePayload;
	private String BadRequestDeletePayload;

	@InjectMock
	EntityService entityService;

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

		List<NGSILDOperationResult> NGSILDOperationResultList = new ArrayList<>();

		NGSILDOperationResult opResult = new NGSILDOperationResult(AppConstants.CREATE_REQUEST, "urn:test:testentity1");
		opResult.addSuccess(new CRUDSuccess(null, null, null, Sets.newHashSet()));
		NGSILDOperationResultList.add(opResult);

		Mockito.when(entityService.createBatch(any(), any(), any(), anyBoolean(), any()))
				.thenReturn(Uni.createFrom().item(NGSILDOperationResultList));

		try {
			ExtractableResponse<Response> response = RestAssured.given().body(payload)
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD)
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

			Mockito.when(entityService.createEntity(any(), any(), any(), any())).thenReturn(Uni.createFrom()
					.item(new NGSILDOperationResult(AppConstants.CREATE_REQUEST, "urn:test:testentity1")));

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

		List<NGSILDOperationResult> NGSILDOperationResultList = new ArrayList<>();

		NGSILDOperationResult opResult = new NGSILDOperationResult(AppConstants.UPSERT_REQUEST, "urn:test:testentity1");
		opResult.addSuccess(new CRUDSuccess(null, null, null, Sets.newHashSet()));
		opResult.setWasUpdated(true);
		NGSILDOperationResultList.add(opResult);

		Mockito.when(entityService.appendBatch(any(), any(), any(), anyBoolean(),anyBoolean(), any()))
				.thenReturn(Uni.createFrom().item(NGSILDOperationResultList));

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

		List<NGSILDOperationResult> NGSILDOperationResultList = new ArrayList<>();

		NGSILDOperationResult opResult = new NGSILDOperationResult(AppConstants.UPSERT_REQUEST, "urn:test:testentity1");
		opResult.addSuccess(new CRUDSuccess(null, null, null, Sets.newHashSet()));
		opResult.setWasUpdated(true);
		NGSILDOperationResultList.add(opResult);

		Mockito.when(entityService.upsertBatch(any(), any(), any(), anyBoolean(), anyBoolean(), any()))
				.thenReturn(Uni.createFrom().item(NGSILDOperationResultList));

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

		List<NGSILDOperationResult> NGSILDOperationResultList = new ArrayList<>();

		NGSILDOperationResult opResult = new NGSILDOperationResult(AppConstants.DELETE_REQUEST, "urn:test:testentity1");
		opResult.addSuccess(new CRUDSuccess(null, null, null, Sets.newHashSet()));
		opResult.setWasUpdated(true);
		NGSILDOperationResultList.add(opResult);

		Mockito.when(entityService.deleteBatch(any(), any(), anyBoolean(), any()))
				.thenReturn(Uni.createFrom().item(NGSILDOperationResultList));

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

			List<NGSILDOperationResult> NGSILDOperationResultList = new ArrayList<>();
			NGSILDOperationResult opResult = new NGSILDOperationResult(AppConstants.DELETE_REQUEST,
					"urn:test:testentity1");
			opResult.addFailure(new ResponseException(ErrorType.InvalidRequest, ""));
			NGSILDOperationResultList.add(opResult);

			Mockito.when(entityService.deleteBatch(any(), any(), anyBoolean(), any()))
					.thenReturn(Uni.createFrom().item(NGSILDOperationResultList));

			ExtractableResponse<Response> response = RestAssured.given().body(BadRequestDeletePayload)
					.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
					.header(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSONLD).when()
					.post("/ngsi-ld/v1/entityOperations/delete").then().statusCode(Status.NOT_FOUND.getStatusCode())
					.statusCode(404).extract();
			assertEquals(404, response.statusCode());

		} catch (Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}

	}

}