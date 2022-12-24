package eu.neclab.ngsildbroker.historymanager.service;

import static org.mockito.ArgumentMatchers.any;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.jsonldjava.core.Context;
import com.google.common.collect.ArrayListMultimap;
import com.google.gson.Gson;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.datatypes.requests.AppendHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CreateEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CreateHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.EntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.results.CreateResult;
import eu.neclab.ngsildbroker.commons.datatypes.results.QueryResult;
import eu.neclab.ngsildbroker.commons.datatypes.results.UpdateResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.StorageFunctionsInterface;
import eu.neclab.ngsildbroker.commons.ngsiqueries.ParamsResolver;
import eu.neclab.ngsildbroker.commons.storage.StorageDAO;
import eu.neclab.ngsildbroker.historymanager.repository.HistoryDAO;
import io.agroal.api.AgroalDataSource;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.vertx.mutiny.pgclient.PgPool;

@QuarkusTest
public class HistoryServiceTest {

	StorageFunctionsInterface storageFunctions;

	@Mock
	private AgroalDataSource writerDataSource;

	@Mock
	private PgPool pgClient;

	@ConfigProperty(name = "scorpio.directDB", defaultValue = "true")
	boolean directDB;

	@Mock
	QueryResult queryResult;

	@Mock
	private HistoryDAO historyDAO;

	@Mock
	private StorageDAO storageDAO;

	@Mock
	ParamsResolver paramsResolver;

	@Mock
	MutinyEmitter<BaseRequest> kafkaSenderInterface;

	@InjectMocks
	@Spy
	private HistoryService historyService;
	@Mock
	EntityRequest request;

	private String temporalPayload;
	private String contextPayload;
	private String modifiyupdatePayload;
	private String modifiyPayload;
	private String tempupdatePayload;
	private String tempAppendPayload;
	private String entityPayload;
	private String olddata;
	private String tempupdatePartialAttributesPayload;
	private String updatePartialDefaultAttributesPayload;
	JsonNode updateJsonNode;
	JsonNode appendJsonNode;
	JsonNode blankNode;
	JsonNode payloadNode;
	JsonNode updatePartialAttributesNode;
	JsonNode updatePartialDefaultAttributesNode;

	ArrayListMultimap<String, String> multimaparr = ArrayListMultimap.create();

	@BeforeEach
	public void setUp() throws Exception {
		MockitoAnnotations.initMocks(this);
		ObjectMapper objectMapper = new ObjectMapper();

		temporalPayload = "{\r\n" + "  \"https://uri.etsi.org/ngsi-ld/default-context/brandName\" : [ {\r\n"
				+ "    \"@type\" : [ \"https://uri.etsi.org/ngsi-ld/Property\" ],\r\n"
				+ "    \"https://uri.etsi.org/ngsi-ld/hasValue\" : [ {\r\n" + "      \"@value\" : \"Mercedes\"\r\n"
				+ "    } ]\r\n" + "  } ],\r\n" + "  \"@id\" : \"urn:ngsi-ld:Vehicle:1\",\r\n"
				+ "  \"https://uri.etsi.org/ngsi-ld/default-context/speed\" : [ {\r\n"
				+ "    \"https://uri.etsi.org/ngsi-ld/observedAt\" : [ {\r\n"
				+ "      \"@value\" : \"2020-08-01T12:03:00Z\",\r\n"
				+ "      \"@type\" : \"https://uri.etsi.org/ngsi-ld/DateTime\"\r\n" + "    } ],\r\n"
				+ "    \"@type\" : [ \"https://uri.etsi.org/ngsi-ld/Property\" ],\r\n"
				+ "    \"https://uri.etsi.org/ngsi-ld/hasValue\" : [ {\r\n" + "      \"@value\" : 45\r\n"
				+ "    } ]\r\n" + "  }, {\r\n" + "    \"https://uri.etsi.org/ngsi-ld/observedAt\" : [ {\r\n"
				+ "      \"@value\" : \"2020-08-01T12:05:00Z\",\r\n"
				+ "      \"@type\" : \"https://uri.etsi.org/ngsi-ld/DateTime\"\r\n" + "    } ],\r\n"
				+ "    \"@type\" : [ \"https://uri.etsi.org/ngsi-ld/Property\" ],\r\n"
				+ "    \"https://uri.etsi.org/ngsi-ld/hasValue\" : [ {\r\n" + "      \"@value\" : 25\r\n"
				+ "    } ]\r\n" + "  }, {\r\n" + "    \"https://uri.etsi.org/ngsi-ld/observedAt\" : [ {\r\n"
				+ "      \"@value\" : \"2020-08-01T12:07:00Z\",\r\n"
				+ "      \"@type\" : \"https://uri.etsi.org/ngsi-ld/DateTime\"\r\n" + "    } ],\r\n"
				+ "    \"@type\" : [ \"https://uri.etsi.org/ngsi-ld/Property\" ],\r\n"
				+ "    \"https://uri.etsi.org/ngsi-ld/hasValue\" : [ {\r\n" + "      \"@value\" : 67\r\n"
				+ "    } ]\r\n" + "  } ],\r\n"
				+ "  \"@type\" : [ \"https://uri.etsi.org/ngsi-ld/default-context/Bus\" ]\r\n" + "}";

		tempupdatePayload = "{\r\n" + "  \"https://uri.etsi.org/ngsi-ld/default-context/brandName\" : [ {\r\n"
				+ "    \"@type\" : [ \"https://uri.etsi.org/ngsi-ld/Property\" ],\r\n"
				+ "    \"https://uri.etsi.org/ngsi-ld/hasValue\" : [ {\r\n" + "      \"@value\" : \"TATA\"\r\n"
				+ "    } ]\r\n" + "  } ]\r\n" + "}";
		tempupdatePartialAttributesPayload = "{\r\n"
				+ "  \"@type\" : [ \"https://uri.etsi.org/ngsi-ld/Property\" ],\r\n"
				+ "  \"https://uri.etsi.org/ngsi-ld/hasValue\" : [ {\r\n" + "    \"@value\" : \"TATA\"\r\n"
				+ "  } ]\r\n" + "}";
		updatePartialDefaultAttributesPayload = "{\r\n" + "		\"https://uri.etsi.org/ngsi-ld/hasValue\": [{\r\n"
				+ "			\"@value\": \"20\"\r\n" + "		}]\r\n" + "}";

		tempAppendPayload = "{\r\n" + "  \"https://uri.etsi.org/ngsi-ld/default-context/brandName1\" : [ {\r\n"
				+ "    \"@type\" : [ \"https://uri.etsi.org/ngsi-ld/Property\" ],\r\n"
				+ "    \"https://uri.etsi.org/ngsi-ld/hasValue\" : [ {\r\n" + "      \"@value\" : \"SUZUKI\"\r\n"
				+ "    } ]\r\n" + "  } ]\r\n" + "}";
		olddata = "{\r\n" + "   \"@id\":\"urn:ngsi-ld:Vehicle:1\",\r\n" + "   \"@type\":[\r\n"
				+ "      \"https://uri.etsi.org/ngsi-ld/default-context/Bus\"\r\n" + "   ],\r\n"
				+ "   \"https://uri.etsi.org/ngsi-ld/createdAt\":[\r\n" + "      {\r\n"
				+ "         \"@type\":\"https://uri.etsi.org/ngsi-ld/DateTime\",\r\n"
				+ "         \"@value\":\"2022-03-31T13:37:15.4903515Z\"\r\n" + "      }\r\n" + "   ],\r\n"
				+ "   \"https://uri.etsi.org/ngsi-ld/modifiedAt\":[\r\n" + "      {\r\n"
				+ "         \"@type\":\"https://uri.etsi.org/ngsi-ld/DateTime\",\r\n"
				+ "         \"@value\":\"2022-03-31T13:37:15.4903515Z\"\r\n" + "      }\r\n" + "   ],\r\n"
				+ "   \"https://uri.etsi.org/ngsi-ld/default-context/brandName\":[\r\n" + "      {\r\n"
				+ "         \"@type\":[\r\n" + "            \"https://uri.etsi.org/ngsi-ld/Property\"\r\n"
				+ "         ],\r\n" + "         \"https://uri.etsi.org/ngsi-ld/hasValue\":[\r\n" + "            {\r\n"
				+ "               \"@value\":\"Mercedes\"\r\n" + "            }\r\n" + "         ],\r\n"
				+ "         \"https://uri.etsi.org/ngsi-ld/createdAt\":[\r\n" + "            {\r\n"
				+ "               \"@type\":\"https://uri.etsi.org/ngsi-ld/DateTime\",\r\n"
				+ "               \"@value\":\"2022-03-31T13:37:15Z\"\r\n" + "            }\r\n" + "         ],\r\n"
				+ "         \"https://uri.etsi.org/ngsi-ld/instanceId\":[\r\n" + "            {\r\n"
				+ "               \"@id\":\"urn:ngsi-ld:4b156346-a89f-46d2-8716-f3f447ca4b43\"\r\n"
				+ "            }\r\n" + "         ],\r\n" + "         \"https://uri.etsi.org/ngsi-ld/modifiedAt\":[\r\n"
				+ "            {\r\n" + "               \"@type\":\"https://uri.etsi.org/ngsi-ld/DateTime\",\r\n"
				+ "               \"@value\":\"2022-03-31T13:37:15Z\"\r\n" + "            }\r\n" + "         ]\r\n"
				+ "      }\r\n" + "   ]\r\n" + "}";

		contextPayload = "{\r\n" + "	\"@context\": [{\r\n" + "		 \r\n"
				+ "		\"brandName\": \"http://example.org/vehicle/brandName\"\r\n" + "	  \r\n" + "	}] \r\n" + "}";
		modifiyPayload = "{\r\n" + "    \"id\": \"urn:ngsi-ld:testunit:154\",\r\n"
				+ "    \"type\": \"AirQualityObserved\",\r\n" + "    \"airQualityLeve2\": [\r\n" + "        {\r\n"
				+ "            \"type\": \"Property\",\r\n" + "            \"value\": \"ASW\",\r\n"
				+ "            \"instanceId\": \"urn:ngsi-ld:9d8948ce-e82a-4273-a222-a9cfb25d1f11\",\r\n"
				+ "            \"observedAt\": \"2018-08-07T12:00:00Z\"\r\n" + "        },\r\n" + "        {\r\n"
				+ "            \"type\": \"Property\",\r\n" + "            \"value\": \"good\",\r\n"
				+ "            \"instanceId\": \"urn:ngsi-ld:ba0434dc-26b1-4f48-9869-ce90dcb18f8b\",\r\n"
				+ "            \"observedAt\": \"2018-08-07T12:00:00Z\"\r\n" + "        },\r\n" + "        {\r\n"
				+ "            \"type\": \"Property\",\r\n" + "            \"value\": \"unhealthy\",\r\n"
				+ "            \"instanceId\": \"urn:ngsi-ld:56177122-d10e-4e1e-b55c-438fe95d9cd2\",\r\n"
				+ "            \"observedAt\": \"2018-09-14T12:00:00Z\"\r\n" + "        }\r\n" + "    ] \r\n" + "}";

		modifiyupdatePayload = "{\r\n" + "    \"airQualityLeve2\": [\r\n" + "        {\r\n"
				+ "            \"type\": \"Property\",\r\n" + "            \"value\": \"ASW\",\r\n"
				+ "            \"observedAt\": \"2018-08-07T12:00:00Z\"\r\n" + "        }\r\n" + "    ]\r\n" + "}";

		updateJsonNode = objectMapper.readTree(tempupdatePayload);
		appendJsonNode = objectMapper.readTree(tempAppendPayload);
		blankNode = objectMapper.createObjectNode();
		payloadNode = objectMapper.readTree(temporalPayload);
		updatePartialAttributesNode = objectMapper.readTree(tempupdatePartialAttributesPayload);
		updatePartialDefaultAttributesNode = objectMapper.readTree(updatePartialDefaultAttributesPayload);
		directDB = true;
	}

	@AfterEach
	public void teardown() {
		String temporalPayload = null;
		String contextPayload = null;
		String modifiyupdatePayload = null;
		String modifiyPayload = null;
		String tempupdatePayload = null;
		String tempAppendPayload = null;
		String entityPayload = null;
		String olddata = null;
		String tempupdatePartialAttributesPayload = null;
		String updatePartialDefaultAttributesPayload = null;

	}

	/**
	 * this method is use for create temporal entity HistoryService
	 */

	@Test
	public void createTemporalEntityTest() throws Exception {
		try {
			ArrayListMultimap<String, String> multimaparr = ArrayListMultimap.create();
			multimaparr.put("content-type", "application/json");
			Gson gson = new Gson();
			Map<String, Object> resolved = gson.fromJson(temporalPayload, Map.class);
			CreateHistoryEntityRequest request = new CreateHistoryEntityRequest(AppConstants.INTERNAL_NULL_KEY,
					resolved, false);
			CreateResult cr = new CreateResult(request.getId(), Boolean.parseBoolean(toString()));
			Mockito.when(historyDAO.isTempEntityExist(any(), any())).thenReturn(Uni.createFrom().item(cr));
			CreateResult result = historyService.createEntry(AppConstants.INTERNAL_NULL_KEY, resolved).await()
					.indefinitely();
			Assertions.assertEquals("urn:ngsi-ld:Vehicle:1", result.getEntityId());
			Mockito.verify(historyService).createEntry(any(), any());
		} catch (Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}
	}

	/**
	 * this method is use for check the temporal entity is already exist
	 */
	@Test
	public void createTemporalEntityThrowsAlreadyExistTest() throws ResponseException, Exception {
		ArrayListMultimap<String, String> multimaparr = ArrayListMultimap.create();
		multimaparr.put("content-type", "application/json");
		Gson gson = new Gson();
		Map<String, Object> resolved = gson.fromJson(temporalPayload, Map.class);
		EntityRequest request = new CreateEntityRequest(resolved, AppConstants.INTERNAL_NULL_KEY);
		CreateResult cr = new CreateResult(request.getId(), Boolean.parseBoolean(toString()));
		ResponseException re = new ResponseException(ErrorType.AlreadyExists, request.getId() + "already exists");
		Mockito.when(historyDAO.isTempEntityExist(any(), any())).thenReturn(Uni.createFrom().failure(re));
		try {
			historyService.createEntry(AppConstants.INTERNAL_NULL_KEY, resolved).await().indefinitely();
		} catch (Exception e) {
			Assertions.assertEquals(request.getId() + "already exists", e.getMessage());
			Mockito.verify(historyService).createEntry(any(), any());
		}

	}

	/**
	 * this method is use for append the field or attribute in Temporal entity
	 */
	@Test
	public void appendTemporalFieldTest() throws Exception {
		multimaparr.put("content-type", "application/json");
		UpdateResult updateResult = new UpdateResult();
		ArrayListMultimap<String, String> entityIds = ArrayListMultimap.create();
		entityIds.put(AppConstants.INTERNAL_NULL_KEY, "urn:ngsi-ld:Vehicle:1");
		Gson gson = new Gson();
		Map<String, Object> resolved = gson.fromJson(tempAppendPayload, Map.class);
		String[] optionsArray = new String[0];
		AppendHistoryEntityRequest request = new AppendHistoryEntityRequest(AppConstants.INTERNAL_NULL_KEY, resolved,
				"urn:ngsi-ld:Vehicle:1");
		updateResult = historyService
				.appendToEntry(AppConstants.INTERNAL_NULL_KEY, "urn:ngsi-ld:Vehicle:1", resolved, optionsArray).await()
				.indefinitely();
		Assertions.assertEquals(updateResult.getUpdated(), request.getUpdateResult().getUpdated());
		Mockito.verify(historyService).handleRequest(any());
	}

	/**
	 * this method is use for append the field or attribute in temporal entity if
	 * entity id is not exist
	 */
	@Test
	public void appendFieldTemporalEntityNotExistTest() {

		ArrayListMultimap<String, String> entityIds = ArrayListMultimap.create();
		entityIds.put(AppConstants.INTERNAL_NULL_KEY, "urn:ngsi-ld:Vehicle:2");
		multimaparr.put("content-type", "application/json");
		Gson gson = new Gson();
		String[] optionsArray = new String[0];
		Map<String, Object> resolved = gson.fromJson(tempAppendPayload, Map.class);
		ResponseException responseException = new ResponseException(ErrorType.NotFound,
				"urn:ngsi-ld:Vehicle:2 was not found");
		Mockito.when(historyDAO.getTemporalEntity(any(), any()))
				.thenReturn(Uni.createFrom().failure(responseException));
		try {
			historyService
					.appendToEntry(AppConstants.INTERNAL_NULL_KEY, "urn:ngsi-ld:Vehicle:1", resolved, optionsArray)
					.await().indefinitely();
		} catch (Exception e) {
			Assertions.assertEquals("urn:ngsi-ld:Vehicle:2 was not found", e.getMessage());
		}
	}

	/**
	 * this method is use for update temporal entity
	 */
	@Test
	public void modifyAttribInstanceTemporalEntityIdTest() throws Exception {

		ArrayListMultimap<String, String> entityIds = ArrayListMultimap.create();
		entityIds.put(AppConstants.INTERNAL_NULL_KEY, "urn:ngsi-ld:Vehicle:1");
		multimaparr.put("content-type", "application/json");
		Gson gson = new Gson();
		Map<String, Object> OdlResolved = gson.fromJson(olddata, Map.class);
		Map<String, Object> tempResolved = gson.fromJson(tempupdatePayload, Map.class);
		QueryParams qp = new QueryParams();
		List<Map<String, String>> temp1 = new ArrayList<Map<String, String>>();
		HashMap<String, String> temp2 = new HashMap<String, String>();
		temp2.put(NGSIConstants.JSON_LD_ID, "urn:ngsi-ld:Vehicle:1");
		temp1.add(temp2);
		qp.setEntities(temp1);
		qp.setAttrs("https://uri.etsi.org/ngsi-ld/default-context/brandName");
		qp.setInstanceId("urn:ngsi-ld:9c7690ed-eba4-4d95-a28b-4584f953f8ab");
		qp.setIncludeSysAttrs(true);

		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		list.add(OdlResolved);
		QueryResult queryResult = new QueryResult(list, null, ErrorType.None, -1, true);
		Context context = Mockito.mock(Context.class);
		Mockito.when(historyDAO.query(any())).thenReturn(Uni.createFrom().item(queryResult));

		historyService.modifyAttribInstanceTemporalEntity(AppConstants.INTERNAL_NULL_KEY, "urn:ngsi-ld:Vehicle:1",
				tempResolved, "https://uri.etsi.org/ngsi-ld/default-context/brandName",
				"urn:ngsi-ld:9c7690ed-eba4-4d95-a28b-4584f953f8ab", context);
		Mockito.verify(historyService).modifyAttribInstanceTemporalEntity(any(), any(), any(), any(), any(), any());
	}

	/**
	 * this method is use for delete the temporal entity
	 */
	@Test
	public void deleteTemporalByIdTest() {
		try {
			multimaparr.put("content-type", "application/json");
			MockitoAnnotations.initMocks(this);
			ArrayListMultimap<String, String> entityIds = ArrayListMultimap.create();
			entityIds.put(AppConstants.INTERNAL_NULL_KEY, "urn:ngsi-ld:Vehicle:1");
			boolean result = historyService.deleteEntry(AppConstants.INTERNAL_NULL_KEY, "urn:ngsi-ld:Vehicle:1").await()
					.indefinitely();
			Assertions.assertEquals(result, true);
			Mockito.verify(historyService).handleRequest(any());
		} catch (Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}
	}

	/**
	 * this method is use for delete the temporal entity if id is null
	 */
	@Test
	public void deleteTemporalByIdNullTest() {
		try {
			multimaparr.put("content-type", "application/json");
			MockitoAnnotations.initMocks(this);
			ArrayListMultimap<String, String> entityIds = ArrayListMultimap.create();
			entityIds.put(AppConstants.INTERNAL_NULL_KEY, "urn:ngsi-ld:Vehicle:1");
			try {
				historyService.deleteEntry(AppConstants.INTERNAL_NULL_KEY, null).await().indefinitely();
			} catch (Exception e) {
				Assertions.assertEquals("empty entity id not allowed", e.getMessage());
				Mockito.verify(historyService).deleteEntry(any(), any());
			}
		} catch (Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}
	}

	/**
	 * this method is use for all delete the temporal entity if id is not exist
	 */
	@Test
	public void deleteAllAttributeInstanceIfEnityNotExistTest() {
		try {
			ArrayListMultimap<String, String> entityIds = ArrayListMultimap.create();
			entityIds.put(AppConstants.INTERNAL_NULL_KEY, "urn:ngsi-ld:Vehicle:2");
			multimaparr.put("content-type", "application/json");
			ResponseException responseException = new ResponseException(ErrorType.NotFound,
					"urn:ngsi-ld:Vehicle:2 was not found");
			Mockito.when(historyDAO.getTemporalEntity(any(), any()))
					.thenReturn(Uni.createFrom().failure(responseException));

			try {
				historyService.deleteEntry(AppConstants.INTERNAL_NULL_KEY, "urn:ngsi-ld:Vehicle:2").await()
						.indefinitely();
			} catch (Exception e) {
				Assertions.assertEquals("urn:ngsi-ld:Vehicle:2" + " was not found", e.getMessage());
				Mockito.verify(historyService).deleteEntry(any(), any());
			}
		} catch (Exception ex) {
			Assertions.fail();
			ex.printStackTrace();
		}
	}

}
