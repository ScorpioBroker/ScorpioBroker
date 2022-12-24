package eu.neclab.ngsildbroker.entityhandler.services;

import io.agroal.api.AgroalDataSource;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.vertx.mutiny.pgclient.PgPool;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import static org.mockito.ArgumentMatchers.any;
import java.util.HashMap;
import java.util.Map;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ArrayListMultimap;
import com.google.gson.Gson;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.slf4j.LoggerFactory;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.AppendEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.EntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.UpdateEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.results.CreateResult;
import eu.neclab.ngsildbroker.commons.datatypes.results.UpdateResult;
import eu.neclab.ngsildbroker.commons.storage.StorageDAO;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.utils.JsonUtils;

@QuarkusTest
@TestMethodOrder(OrderAnnotation.class)
public class EntityServiceTest {

	@Mock
	private AgroalDataSource writerDataSource;

	@Mock
	private PgPool pgClient;;

	@ConfigProperty(name = "scorpio.directDB", defaultValue = "true")
	boolean directDB;

	@Mock
	private EntityInfoDAO entityInfoDAO;
	@Mock
	private StorageDAO storageDAO;

	@Mock
	MutinyEmitter<BaseRequest> kafkaSenderInterface;

	@InjectMocks
	@Spy
	private EntityService entityService;
	@Mock
	EntityRequest request;

	org.slf4j.Logger logger = LoggerFactory.getLogger("SampleLogger");
	String updatePayload;
	String updatePayloadforwrongattr;
	String appendPayload;
	String entityPayload;
	String updatePartialAttributesPayload;
	String updatePartialDefaultAttributesPayload;
	JsonNode updateJsonNode;
	JsonNode appendJsonNode;
	JsonNode blankNode;
	JsonNode payloadNode;
	JsonNode updatePartialAttributesNode;
	JsonNode updatePartialDefaultAttributesNode;
	private String enitityIdNotPayload;
	private String entityAttrPayload;
	private String updateAttrPayload;
	private String entityPayloadIdNotFound;
	ArrayListMultimap<String, String> multimaparr = ArrayListMultimap.create();
	ArrayListMultimap<String, String> entityIds = ArrayListMultimap.create();

	static JsonLdOptions opts = new JsonLdOptions(JsonLdOptions.JSON_LD_1_1);

	@BeforeEach
	public void setUp() throws Exception {
		MockitoAnnotations.initMocks(this);
		pgClient.preparedQuery("SELECT $1");
		ObjectMapper objectMapper = new ObjectMapper();

		enitityIdNotPayload = "{\r\n" + "    \"id\": \" \",\r\n" + "    \"type\": \"Vehicle\",\r\n"
				+ "   \"brandName\": {\r\n" + "        \"type\": \"Property\",\r\n" + "        \"value\": \"Swift\"\r\n"
				+ "    },\r\n" + "    \"isParked\": {\r\n" + "        \"type\": \"Relationship\",\r\n"
				+ "        \"providedBy\": {\r\n" + "            \"type\": \"Relationship\",\r\n"
				+ "            \"object\": \"urn:ngsi-ld:Person:Bob\"\r\n" + "        },\r\n"
				+ "        \"object\": \"urn:ngsi-ld:OffStreetParking:Downtown1\"\r\n" + "    },\r\n"
				+ "    \"speed\": {\r\n" + "        \"type\": \"Property\",\r\n" + "        \"value\": 85\r\n"
				+ "    },\r\n" + "    \"location\": {\r\n" + "        \"type\": \"GeoProperty\",\r\n"
				+ "        \"value\": {\r\n" + "            \"type\": \"Point\",\r\n"
				+ "            \"coordinates\": [\r\n" + "                -8.6,\r\n" + "                41.6\r\n"
				+ "            ]\r\n" + "        }\r\n" + "    }\r\n" + "}";

		entityPayload = "{\r\n" + "    \"http://example.org/vehicle/brandName\": [\r\n" + "      {\r\n"
				+ "        \"@type\":[\r\n" + "    \"https://uri.etsi.org/ngsi-ld/Property\"],\r\n"
				+ "    \"https://uri.etsi.org/ngsi-ld/hasValue\": [{\r\n" + "     \"@value\": \"Mercedes\"\r\n"
				+ "      }]\r\n" + "    }],\r\n" + "        \"https://uri.etsi.org/ngsi-ld/createdAt\": [\r\n"
				+ "          {\r\n" + "            \"@type\": \"https://uri.etsi.org/ngsi-ld/DateTime\",\r\n"
				+ "            \"@value\": \"2018-06-01T12:03:00Z\"\r\n" + "          }\r\n" + "        ],\r\n"
				+ "     \"@id\": \"urn:ngsi-ld:Vehicle:A103\",\r\n"
				+ "    \"https://uri.etsi.org/ngsi-ld/modifiedAt\":[{\r\n"
				+ "     \"@value\": \"2017-07-29T12:00:04Z\",\r\n"
				+ "     \"@type\": \"https://uri.etsi.org/ngsi-ld/DateTime\"}],\r\n"
				+ "     \"http://example.org/vehicle/speed\": [\r\n" + "     {\r\n"
				+ "    \"https://uri.etsi.org/ngsi-ld/datasetId\": [\r\n" + "     {\r\n"
				+ "     \"@id\": \"urn:ngsi-ld:Property:speedometerA4567-speed\"\r\n" + "      }\r\n" + "      ],\r\n"
				+ "    \"https://uri.etsi.org/ngsi-ld/default-context/source\":[\r\n" + "     {\r\n"
				+ "     \"@type\":[\r\n" + "    \"https://https://uri.etsi.org/ngsi-ld/Property\"],\r\n"
				+ "    \"https://uri.etsi.org/ngsi-ld/hasValue\":[\r\n" + "		{\r\n"
				+ "     \"@value\": \"Speedometer\"\r\n" + "      }\r\n" + "      ]\r\n" + "      }\r\n"
				+ "      ],\r\n" + "       \"@type\":[\r\n" + "    \"https://uri.etsi.org/ngsi-ld/Property\"],\r\n"
				+ "     \"https://uri.etsi.org/ngsi-ld/hasValue\":[\r\n" + "		{\r\n" + "    \"value\":55\r\n"
				+ "      }\r\n" + "        ]\r\n" + "      },\r\n" + "     {\r\n"
				+ "    \"https://uri.etsi.org/ngsi-ld/default-context/source\":[\r\n" + "     {\r\n"
				+ "     \"@type\":[\r\n" + "    \"https://https://uri.etsi.org/ngsi-ld/Property\"],\r\n"
				+ "    \"https://uri.etsi.org/ngsi-ld/hasValue\":[\r\n" + "		{\r\n" + "     \"@value\": \"GPS\"\r\n"
				+ "      }\r\n" + "      ]\r\n" + "     }\r\n" + "      ],\r\n" + "     \"@type\":[\r\n"
				+ "    \"https://https://uri.etsi.org/ngsi-ld/Property\"],\r\n"
				+ "     \"https://uri.etsi.org/ngsi-ld/hasValue\":[\r\n" + "	  {\r\n" + "    \"value\":10\r\n"
				+ "      }\r\n" + "     ]\r\n" + "     }\r\n" + "     ],\r\n" + "     \"@type\":[\r\n"
				+ "    \"http://example.org/vehicle/Vehicle\"]\r\n" + "  }\r\n";

		updatePayload = "{\r\n" + "	\"http://example.org/vehicle/brandName\": [{\r\n"
				+ "		\"@type\": [\"https://uri.etsi.org/ngsi-ld/Property\"],\r\n"
				+ "		\"https://uri.etsi.org/ngsi-ld/hasValue\": [{\r\n" + "			\"@value\": \"AUDI\"\r\n"
				+ "		}]\r\n" + "	}]\r\n" + "}";

		entityPayloadIdNotFound = "{\r\n" + "    \"http://example.org/vehicle/brandName\": [\r\n" + "      {\r\n"
				+ "        \"@type\":[\r\n" + "    \"https://uri.etsi.org/ngsi-ld/Property\"],\r\n"
				+ "    \"https://uri.etsi.org/ngsi-ld/hasValue\": [{\r\n" + "     \"@value\": \"Mercedes\"\r\n"
				+ "      }]\r\n" + "    }],\r\n" + "        \"https://uri.etsi.org/ngsi-ld/createdAt\": [\r\n"
				+ "          {\r\n" + "            \"@type\": \"https://uri.etsi.org/ngsi-ld/DateTime\",\r\n"
				+ "            \"@value\": \"2018-06-01T12:03:00Z\"\r\n" + "          }\r\n" + "        ],\r\n"
				+ "     \"@id\": \"\",\r\n" + "    \"https://uri.etsi.org/ngsi-ld/modifiedAt\":[{\r\n"
				+ "     \"@value\": \"2017-07-29T12:00:04Z\",\r\n"
				+ "     \"@type\": \"https://uri.etsi.org/ngsi-ld/DateTime\"}],\r\n"
				+ "     \"http://example.org/vehicle/speed\": [\r\n" + "     {\r\n"
				+ "    \"https://uri.etsi.org/ngsi-ld/datasetId\": [\r\n" + "     {\r\n"
				+ "     \"@id\": \"urn:ngsi-ld:Property:speedometerA4567-speed\"\r\n" + "      }\r\n" + "      ],\r\n"
				+ "    \"https://uri.etsi.org/ngsi-ld/default-context/source\":[\r\n" + "     {\r\n"
				+ "     \"@type\":[\r\n" + "    \"https://https://uri.etsi.org/ngsi-ld/Property\"],\r\n"
				+ "    \"https://uri.etsi.org/ngsi-ld/hasValue\":[\r\n" + "		{\r\n"
				+ "     \"@value\": \"Speedometer\"\r\n" + "      }\r\n" + "      ]\r\n" + "      }\r\n"
				+ "      ],\r\n" + "       \"@type\":[\r\n" + "    \"https://uri.etsi.org/ngsi-ld/Property\"],\r\n"
				+ "     \"https://uri.etsi.org/ngsi-ld/hasValue\":[\r\n" + "		{\r\n" + "    \"value\":55\r\n"
				+ "      }\r\n" + "        ]\r\n" + "      },\r\n" + "     {\r\n"
				+ "    \"https://uri.etsi.org/ngsi-ld/default-context/source\":[\r\n" + "     {\r\n"
				+ "     \"@type\":[\r\n" + "    \"https://https://uri.etsi.org/ngsi-ld/Property\"],\r\n"
				+ "    \"https://uri.etsi.org/ngsi-ld/hasValue\":[\r\n" + "		{\r\n" + "     \"@value\": \"GPS\"\r\n"
				+ "      }\r\n" + "      ]\r\n" + "     }\r\n" + "      ],\r\n" + "     \"@type\":[\r\n"
				+ "    \"https://https://uri.etsi.org/ngsi-ld/Property\"],\r\n"
				+ "     \"https://uri.etsi.org/ngsi-ld/hasValue\":[\r\n" + "	  {\r\n" + "    \"value\":10\r\n"
				+ "      }\r\n" + "     ]\r\n" + "     }\r\n" + "     ],\r\n" + "     \"@type\":[\r\n"
				+ "    \"http://example.org/vehicle/Vehicle\"]\r\n" + "  }\r\n";

		updatePayload = "{\r\n" + "	\"http://example.org/vehicle/brandName\": [{\r\n"
				+ "		\"@type\": [\"https://uri.etsi.org/ngsi-ld/Property\"],\r\n"
				+ "		\"https://uri.etsi.org/ngsi-ld/hasValue\": [{\r\n" + "			\"@value\": \"AUDI\"\r\n"
				+ "		}]\r\n" + "	}]\r\n" + "}";

		updatePayloadforwrongattr = "{\r\n" + "	\"http://example.org/vehicle/brandName1\": [{\r\n"
				+ "		\"@type\": [\"https://uri.etsi.org/ngsi-ld/Property\"],\r\n"
				+ "		\"https://uri.etsi.org/ngsi-ld/hasValue\": [{\r\n" + "			\"@value\": \"AUDI\"\r\n"
				+ "		}]\r\n" + "	}]\r\n" + "}";
		updatePartialAttributesPayload = "{\r\n" + "	\"https://uri.etsi.org/ngsi-ld/datasetId\": [{\r\n"
				+ "		\"@id\": \"urn:ngsi-ld:Property:speedometerA4567-speed\" \r\n" + "	}],\r\n"
				+ "		\"https://uri.etsi.org/ngsi-ld/hasValue\": [{\r\n" + "			\"@value\": \"20\"\r\n"
				+ "		}]\r\n" + "}";
		updatePartialDefaultAttributesPayload = "{\r\n" + "		\"https://uri.etsi.org/ngsi-ld/hasValue\": [{\r\n"
				+ "			\"@value\": \"20\"\r\n" + "		}]\r\n" + "}";

		appendPayload = "{\r\n" + "	\"http://example.org/vehicle/brandName1\": [{\r\n"
				+ "		\"@type\": [\"https://uri.etsi.org/ngsi-ld/Property\"],\r\n"
				+ "		\"https://uri.etsi.org/ngsi-ld/hasValue\": [{\r\n" + "			\"@value\": \"BMW\"\r\n"
				+ "		}]\r\n" + "	}]\r\n" + "}";

		entityAttrPayload = "{\r\n" + "    \"id\": \"urn:ngsi-ld:Vehicle:A100\",\r\n" + "    \"type\": \"Vehicle\",\r\n"
				+ "    \"brandName\": {\r\n" + "        \"type\": \"Property\",\r\n"
				+ "        \"value\": \"Mercedes\"\r\n" + "    }\r\n" + "}";

		updateAttrPayload = "{\r\n" + "    \"brandName\": {\r\n" + "        \"type\": \"Property\",\r\n"
				+ "        \"value\": \"AUDI\"\r\n" + "    }\r\n" + "}";

		// @formatter:on

		updateJsonNode = objectMapper.readTree(updatePayload);
		appendJsonNode = objectMapper.readTree(appendPayload);
		blankNode = objectMapper.createObjectNode();
		payloadNode = objectMapper.readTree(entityPayload);
		updatePartialAttributesNode = objectMapper.readTree(updatePartialAttributesPayload);
		updatePartialDefaultAttributesNode = objectMapper.readTree(updatePartialDefaultAttributesPayload);
		directDB = true;
	}

	@AfterEach
	public void tearDown() {
		updatePayload = null;
		appendPayload = null;
		entityPayload = null;
		updatePartialAttributesPayload = null;
		updatePartialDefaultAttributesPayload = null;
	}

	/**
	 * this method is use for create the entity
	 */
	@Test
	@Order(1)
	public void createMessageIdTest() {
		try {
			multimaparr.put("content-type", "application/json");
			Gson gson = new Gson();
			Map<String, Object> resolved = gson.fromJson(entityPayload, Map.class);
			CreateResult result = entityService.createEntry(AppConstants.INTERNAL_NULL_KEY, resolved).await().indefinitely();
			Assertions.assertEquals("urn:ngsi-ld:Vehicle:A103", result.getEntityId());
			Mockito.verify(entityService).createEntry(any(),any());
		} catch (Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}
	}

	/**
	 * this method is use for verify entity id is null
	 */
	@Test
	@Order(2)
	public void createMessageNullTest() {
		try {
		multimaparr.put("content-type", "application/json");
		Gson gson = new Gson();
		Map<String, Object> resolved = gson.fromJson(enitityIdNotPayload, Map.class);
		CreateResult s = entityService.createEntry(AppConstants.INTERNAL_NULL_KEY, resolved).await().indefinitely();
		Assertions.assertEquals(null, s.getEntityId());
		Mockito.verify(entityService).createEntry(any(),any());
		}catch(Exception e) {
			e.printStackTrace();
			Assertions.fail();
		}
	}

	/**
	 * this method is use for update the entity if Attribute id is not exist
	 */
	@Test
	@Order(3)
	public void updateMessageAttrIdNotExistTest() throws Exception {
		try {
			ArrayListMultimap<String, String> entityIds = ArrayListMultimap.create();
			entityIds.put(AppConstants.INTERNAL_NULL_KEY, "urn:ngsi-ld:Vehicle:A103");
			UpdateResult updateResult = new UpdateResult();
			Map<String, Object> m = new HashMap<String, Object>();
			m.put(appendPayload, entityIds);
			Mockito.when(entityInfoDAO.getEntity(any(), any())).thenReturn(Uni.createFrom().item(m));
			multimaparr.put("content-type", "application/json");
			Gson gson = new Gson();
			Map<String, Object> resolved = gson.fromJson(updatePayloadforwrongattr, Map.class);
			Map<String, Object> entityBody = (Map<String, Object>) JsonUtils.fromString(entityPayload);
			UpdateEntityRequest request = new UpdateEntityRequest(AppConstants.INTERNAL_NULL_KEY, "urn:ngsi-ld:Vehicle:A103", entityBody,
					resolved, null);
			updateResult = entityService.updateEntry(AppConstants.INTERNAL_NULL_KEY, entityPayload, resolved).await().indefinitely();
			Assertions.assertEquals(request.getUpdateResult().getNotUpdated(), updateResult.getNotUpdated());
			Mockito.verify(entityService).updateEntry(any(), any(), any());
		} catch (Exception ex) {
			Assertions.fail();
			ex.printStackTrace();
		}
	}

	/**
	 * this method is use for update attribute if attribute not found
	 */
	@Test
	public void updateMessageAttrNotFound() {
		try {
			ArrayListMultimap<String, String> entityIds = ArrayListMultimap.create();
			entityIds.put(AppConstants.INTERNAL_NULL_KEY, "urn:ngsi-ld:Vehicle:A100");
			UpdateResult updateResult = new UpdateResult();
			Map<String, Object> m = new HashMap<String, Object>();
			m.put("@value", entityPayload);
			Mockito.when(entityInfoDAO.getEntity(any(), any())).thenReturn(Uni.createFrom().item(m));
			multimaparr.put("content-type", "application/json");
			Gson gson = new Gson();
			Map<String, Object> resolved = gson.fromJson(updatePayload, Map.class);
			Map<String, Object> entityBody = (Map<String, Object>) JsonUtils.fromString(entityPayload);
			UpdateEntityRequest request = new UpdateEntityRequest(AppConstants.INTERNAL_NULL_KEY, "urn:ngsi-ld:Vehicle:A103", entityBody,
					resolved, null);
			updateResult = entityService.updateEntry(AppConstants.INTERNAL_NULL_KEY, "urn:ngsi-ld:Vehicle:A103", resolved).await()
					.indefinitely();
			Assertions.assertNotEquals(request.getUpdateResult().getUpdated(), updateResult.getNotUpdated());
			Mockito.verify(entityService, Mockito.times(1)).updateEntry(any(), any(), any());
		} catch (Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}
	}

	/**
	 * this method is use for append the field Attribute
	 */
	@Test
	public void appendFieldTest()  {
		try {
		MockitoAnnotations.initMocks(this);
		ArrayListMultimap<String, String> entityIds = ArrayListMultimap.create();
		entityIds.put(AppConstants.INTERNAL_NULL_KEY, "urn:ngsi-ld:Vehicle:A103");
		UpdateResult updateResult = new UpdateResult();
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("@id", entityIds);
		Mockito.when(entityInfoDAO.getEntity(any(), any())).thenReturn(Uni.createFrom().item(m));
		multimaparr.put("content-type", "application/json");
		Gson gson = new Gson();
		Map<String, Object> resolved = gson.fromJson(appendPayload, Map.class);
		Map<String, Object> entityBody = (Map<String, Object>) JsonUtils.fromString(entityPayload);
		AppendEntityRequest request = new AppendEntityRequest(AppConstants.INTERNAL_NULL_KEY, "urn:ngsi-ld:Vehicle:A103", entityBody,
				resolved, null);
		String[] options = null;
		updateResult = entityService.appendToEntry(AppConstants.INTERNAL_NULL_KEY, "urn:ngsi-ld:Vehicle:A103", resolved, options).await()
				.indefinitely();
		Assertions.assertEquals(request.getUpdateResult().getUpdated(), updateResult.getUpdated());
		Mockito.verify(entityService).appendToEntry(any(), any(), any(), any());
		}catch(Exception e) {
			Assertions.fail();
		}

	}

	/**
	 * this method is use to valied Id Not Found
	 */
	@Test
	public void appendFieldNotFoundTest() throws Exception {
		MockitoAnnotations.initMocks(this);
		ArrayListMultimap<String, String> entityIds = ArrayListMultimap.create();
		entityIds.put(AppConstants.INTERNAL_NULL_KEY, null);
		UpdateResult updateResult = new UpdateResult();
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("@id", entityIds);
		Mockito.when(entityInfoDAO.getEntity(any(), any())).thenReturn(Uni.createFrom().item(m));
		multimaparr.put("content-type", "application/json");
		Gson gson = new Gson();
		Map<String, Object> resolved = gson.fromJson(appendPayload, Map.class);
		Map<String, Object> entityBody = (Map<String, Object>) JsonUtils.fromString(entityPayloadIdNotFound);
		AppendEntityRequest request = new AppendEntityRequest(AppConstants.INTERNAL_NULL_KEY, " ", entityBody, resolved, null);
		try {
			entityService.appendToEntry(AppConstants.INTERNAL_NULL_KEY, null, resolved, null).await().indefinitely();
		} catch (Exception e) {
			Assertions.assertEquals("empty entity id is not allowed", e.getMessage());
			Mockito.verify(entityService).appendToEntry(any(), any(), any(), any());
		}

	}

	/**
	 * this method is use to Validate partial update attribute but Attribute Not
	 * Found
	 */
	@Test
	public void updatePartialAttributeNotFoundTest() throws Exception {

		MockitoAnnotations.initMocks(this);
		ArrayListMultimap<String, String> entityIds = ArrayListMultimap.create();
		entityIds.put(AppConstants.INTERNAL_NULL_KEY, "urn:ngsi-ld:Vehicle:A103");
		UpdateResult updateResult = new UpdateResult();
		// Mockito.doReturn(entityPayload).when(entityInfoDAO).getEntity(any(), any());
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("@id", entityIds);
		Mockito.when(entityInfoDAO.getEntity(any(), any())).thenReturn(Uni.createFrom().item(m));
		multimaparr.put("content-type", "application/json");
		Gson gson = new Gson();
		Map<String, Object> resolved = gson.fromJson(updatePartialAttributesPayload, Map.class);
		Map<String, Object> entityBody = (Map<String, Object>) JsonUtils.fromString(entityPayload);
		UpdateEntityRequest request = new UpdateEntityRequest(AppConstants.INTERNAL_NULL_KEY, "urn:ngsi-ld:Vehicle:A103", entityBody,
				resolved, "http://example.org/vehicle/speed");

		try {
			entityService.partialUpdateEntity(AppConstants.INTERNAL_NULL_KEY, "urn:ngsi-ld:Vehicle:A103",
					"http://example.org/vehicle/speed", resolved).await().indefinitely();
		} catch (Exception e) {
			Assertions.assertEquals("Provided attribute is not present", e.getMessage());
			Mockito.verify(entityService).partialUpdateEntity(any(), any(), any(), any());
		}
	}

	/**
	 * this method is use to Validate partial update attribute but Id Not Found
	 */
	@Test
	public void updatePartialAttributeIdFoundTest() throws Exception {
		MockitoAnnotations.initMocks(this);
		ArrayListMultimap<String, String> entityIds = ArrayListMultimap.create();
		entityIds.put(AppConstants.INTERNAL_NULL_KEY, "urn:ngsi-ld:Vehicle:A103");
		UpdateResult updateResult = new UpdateResult();
		// Mockito.doReturn(entityPayload).when(entityInfoDAO).getEntity(any(), any());
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("@id", entityIds);
		Mockito.when(entityInfoDAO.getEntity(any(), any())).thenReturn(Uni.createFrom().item(m));
		multimaparr.put("content-type", "application/json");
		Gson gson = new Gson();
		Map<String, Object> resolved = gson.fromJson(updatePartialAttributesPayload, Map.class);
		Map<String, Object> entityBody = (Map<String, Object>) JsonUtils.fromString(entityPayload);
		UpdateEntityRequest request = new UpdateEntityRequest(AppConstants.INTERNAL_NULL_KEY, "urn:ngsi-ld:Vehicle:A103", entityBody,
				resolved, "http://example.org/vehicle/speed");

		try {
			entityService.partialUpdateEntity(AppConstants.INTERNAL_NULL_KEY, null, "http://example.org/vehicle/speed", resolved).await()
					.indefinitely();
		} catch (Exception e) {
			Assertions.assertEquals("empty entity id not allowed", e.getMessage());
			Mockito.verify(entityService).partialUpdateEntity(any(), any(), any(), any());
		}
	}

	/**
	 * this method is use to Validate delete Attribute but Attribute Not Found
	 */
	@Test
	public void deleteAttributeNotFoundTest() throws Exception {
		ArrayListMultimap<String, String> entityIds = ArrayListMultimap.create();
		entityIds.put(AppConstants.REQUEST_ID, "urn:ngsi-ld:Vehicle:A103");
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("attrs", "http://example.org/vehicle/speed");
		Mockito.when(entityInfoDAO.getEntity(any(), any())).thenReturn(Uni.createFrom().item(m));
		Map<String, Object> entityBody = (Map<String, Object>) JsonUtils.fromString(entityPayload);
		multimaparr.put("content-type", "application/json");
		try {
			entityService.deleteAttribute(AppConstants.INTERNAL_NULL_KEY, "urn:ngsi-ld:Vehicle:A103", "http://example.org/vehicle/speed",
					null, null).await().indefinitely();
		} catch (Exception e) {
			Assertions.assertEquals("Attribute is not present", e.getMessage());
			Mockito.verify(entityService).deleteAttribute(any(), any(), any(), any(), any());
		}

	}

	/**
	 * this method is use to Validate delete Attribute but Id Not Found
	 */
	@Test
	public void deleteAttributeInstanceIdNullTest() throws Exception {
		ArrayListMultimap<String, String> entityIds = ArrayListMultimap.create();
		entityIds.put(AppConstants.REQUEST_ID, "urn:ngsi-ld:Vehicle:A103");
		Map<String, Object> m = new HashMap<String, Object>();
		m.put("attrs", "http://example.org/vehicle/speed");
		Mockito.when(entityInfoDAO.getEntity(any(), any())).thenReturn(Uni.createFrom().item(m));
		Map<String, Object> entityBody = (Map<String, Object>) JsonUtils.fromString(entityPayload);
		multimaparr.put("content-type", "application/json");
		try {
			entityService.deleteAttribute(AppConstants.INTERNAL_NULL_KEY, null, "http://example.org/vehicle/speed", null, null).await()
					.indefinitely();
		} catch (Exception e) {
			Assertions.assertEquals("empty entity id not allowed", e.getMessage());
			Mockito.verify(entityService).deleteAttribute(any(), any(), any(), any(), any());
		}
	}

	/**
	 * this method is use to Validate delete Entry
	 */
	@Test
	public void deleteEntityTest() throws Exception {
		try {
		ArrayListMultimap<String, String> entityIds = ArrayListMultimap.create();
		entityIds.put(AppConstants.REQUEST_ID, "urn:ngsi-ld:Vehicle:A103");
		Map<String, Object> m = new HashMap<String, Object>();
		m.put(entityPayload, entityIds);
		Mockito.when(entityInfoDAO.getEntity(any(), any())).thenReturn(Uni.createFrom().item(m));
		Map<String, Object> entityBody = (Map<String, Object>) JsonUtils.fromString(entityPayload);
		multimaparr.put("content-type", "application/json");
		boolean result = entityService.deleteEntry(AppConstants.INTERNAL_NULL_KEY, "urn:ngsi-ld:Vehicle:A103").await().indefinitely();
		Assertions.assertEquals(true, result);
		Mockito.verify(entityService).deleteEntry(any(), any());
		}catch(Exception e) {
			Assertions.fail();
		}
	}

	/**
	 * this method is use to Validate delete Entry but Entity id Not Found
	 */
	@Test
	public void deleteEntityNotFoundTest() throws Exception {
		ArrayListMultimap<String, String> entityIds = ArrayListMultimap.create();
		entityIds.put(AppConstants.REQUEST_ID, "urn:ngsi-ld:Vehicle:A103");
		Map<String, Object> m = new HashMap<String, Object>();
		m.put(entityPayload, entityIds);
		Mockito.when(entityInfoDAO.getEntity(any(), any())).thenReturn(Uni.createFrom().item(m));
		Map<String, Object> entityBody = (Map<String, Object>) JsonUtils.fromString(entityPayload);
		multimaparr.put("content-type", "application/json");
		try {
			entityService.deleteEntry(AppConstants.INTERNAL_NULL_KEY, null).await().indefinitely();
		} catch (Exception e) {
			Assertions.assertEquals("empty entity id not allowed", e.getMessage());
			Mockito.verify(entityService).deleteEntry(any(), any());
		}
	}

}
