package eu.neclab.ngsildbroker.entityhandler.services;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.sql.DataSource;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.modules.junit4.PowerMockRunnerDelegate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.support.TransactionTemplate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.ArrayListMultimap;
import com.google.gson.Gson;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.DBWriteTemplates;
import eu.neclab.ngsildbroker.commons.datatypes.requests.AppendEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CreateEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.EntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.UpdateEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.results.UpdateResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.storage.StorageDAO;
import eu.neclab.ngsildbroker.entityhandler.config.EntityTopicMap;

@TestPropertySource(properties = { "scorpio.directDB= true", })

@RunWith(PowerMockRunner.class)

@PowerMockRunnerDelegate(SpringRunner.class)
@PowerMockIgnore({ "javax.management.*" })
@TestPropertySource(properties = { "scorpio.directDB=true" })
public class EntityServiceTest {

	@MockBean
	private ObjectMapper objectMapper;
	@Value("${scorpio.directDB}")
	boolean directDB;

	@Mock
	HttpServletRequest request;
	@MockBean
	private EntityTopicMap entityTopicMap;

	@Mock
	EntityInfoDAO entityInfoDAO;
	@Mock
	StorageDAO storageDAO;

	@Mock
	DBWriteTemplates templates;

	@Mock
	private JdbcTemplate writerJdbcTemplate;

	@Mock
	private TransactionTemplate writerTransactionTemplate;

	@Mock
	private JdbcTemplate writerJdbcTemplateWithTransaction;

	@Mock
	private DBWriteTemplates defaultTemplates;

	@Mock
	private DataSource writerDataSource;

	@InjectMocks
	@Spy
	private EntityService entityService;

	@Rule
	public ExpectedException thrown = ExpectedException.none();

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

	ArrayListMultimap<String, String> multimaparr = ArrayListMultimap.create();
	ArrayListMultimap<String, String> entityIds = ArrayListMultimap.create();

	static JsonLdOptions opts = new JsonLdOptions(JsonLdOptions.JSON_LD_1_1);

	@Before
	public void setUp() throws Exception {
		MockitoAnnotations.initMocks(this);
		writerJdbcTemplate.execute("SELECT 1"); // create connection pool and connect to database
		DataSourceTransactionManager transactionManager = new DataSourceTransactionManager(writerDataSource);
		writerJdbcTemplateWithTransaction = new JdbcTemplate(transactionManager.getDataSource());
		writerTransactionTemplate = new TransactionTemplate(transactionManager);
		this.defaultTemplates = new DBWriteTemplates(writerJdbcTemplateWithTransaction, writerTransactionTemplate,
				writerJdbcTemplate);
		ObjectMapper objectMapper = new ObjectMapper();

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

		// @formatter:on

		updateJsonNode = objectMapper.readTree(updatePayload);
		appendJsonNode = objectMapper.readTree(appendPayload);
		blankNode = objectMapper.createObjectNode();
		payloadNode = objectMapper.readTree(entityPayload);
		updatePartialAttributesNode = objectMapper.readTree(updatePartialAttributesPayload);
		updatePartialDefaultAttributesNode = objectMapper.readTree(updatePartialDefaultAttributesPayload);
		directDB = true;
	}

	@After
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
	public void createMessageTest() {
		try {
			multimaparr.put("content-type", "application/json");
			Gson gson = new Gson();
			Map<String, Object> resolved = gson.fromJson(entityPayload, Map.class);
			String id = entityService.createEntry(multimaparr, resolved).getEntityId();
			Assert.assertEquals("urn:ngsi-ld:Vehicle:A103", id);
			verify(entityService).createEntry(any(), any());
		} catch (Exception ex) {
			Assert.fail();
		}

	}

	/**
	 * this method is use for check the entity is already exist
	 */
	@Test
	public void createMessageThrowsAlreadyExistTest() throws ResponseException, Exception {
		MockitoAnnotations.initMocks(this);
		ArrayListMultimap<String, String> entityIds = ArrayListMultimap.create();
		entityIds.put(AppConstants.INTERNAL_NULL_KEY, "urn:ngsi-ld:Vehicle:A103");
		ReflectionTestUtils.setField(entityService, "directDB", true);
		multimaparr.put("content-type", "application/json");
		Gson gson = new Gson();
		Map<String, Object> resolved = gson.fromJson(entityPayload, Map.class);
		EntityRequest request = new CreateEntityRequest(resolved, multimaparr);
		Mockito.doThrow(new ResponseException(ErrorType.AlreadyExists, request.getId() + " already exists"))
				.when(entityService).createEntry(multimaparr, resolved);
		try {
			entityService.createEntry(multimaparr, resolved);
		} catch (Exception e) {
			Assert.assertEquals(request.getId() + " already exists", e.getMessage());
		}

	}

	/**
	 * this method is use for update the entity if Attribute is not same
	 */
	@Test
	public void updateMessageAttrIsNotExistTest() throws Exception {
		try {
			ArrayListMultimap<String, String> entityIds = ArrayListMultimap.create();
			entityIds.put(AppConstants.INTERNAL_NULL_KEY, "urn:ngsi-ld:Vehicle:A103");
			ReflectionTestUtils.setField(entityService, "directDB", true);
			UpdateResult updateResult = new UpdateResult();
			Mockito.doReturn(entityPayload).when(entityInfoDAO).getEntity(any(), any());
			multimaparr.put("content-type", "application/json");
			Gson gson = new Gson();
			Map<String, Object> resolved = gson.fromJson(updatePayloadforwrongattr, Map.class);
			Map<String, Object> entityBody = (Map<String, Object>) JsonUtils.fromString(entityPayload);
			UpdateEntityRequest request = new UpdateEntityRequest(multimaparr, "urn:ngsi-ld:Vehicle:A103", entityBody,
					resolved, null);
			updateResult = entityService.updateEntry(multimaparr, "urn:ngsi-ld:Vehicle:A103", resolved);
			Assert.assertEquals(request.getUpdateResult().getNotUpdated(), updateResult.getNotUpdated());

		} catch (Exception ex) {
			Assert.fail();
		}
	}

	/**
	 * this method is use for update the entity if Entity id is null
	 */
	@Test
	public void updateMessageEntityIDNULLTest() throws Exception {
		try {
			ArrayListMultimap<String, String> entityIds = ArrayListMultimap.create();
			entityIds.put(AppConstants.INTERNAL_NULL_KEY, "urn:ngsi-ld:Vehicle:A103");
			ReflectionTestUtils.setField(entityService, "directDB", true);

			Mockito.doReturn(entityPayload).when(entityInfoDAO).getEntity(any(), any());
			multimaparr.put("content-type", "application/json");
			Gson gson = new Gson();
			Map<String, Object> resolved = gson.fromJson(updatePayload, Map.class);
			try {
				entityService.updateEntry(multimaparr, "", resolved);
			} catch (Exception e) {
				Assert.assertEquals("empty entity id not allowed", e.getMessage());
			}

		} catch (Exception ex) {
			Assert.fail();
		}
	}

	/**
	 * this method is use for update the entity
	 */
	@Test
	public void updateMessageTest() throws Exception {
		try {
			ArrayListMultimap<String, String> entityIds = ArrayListMultimap.create();
			entityIds.put(AppConstants.INTERNAL_NULL_KEY, "urn:ngsi-ld:Vehicle:A103");
			ReflectionTestUtils.setField(entityService, "directDB", true);
			UpdateResult updateResult = new UpdateResult();
			Mockito.doReturn(entityPayload).when(entityInfoDAO).getEntity(any(), any());
			multimaparr.put("content-type", "application/json");
			Gson gson = new Gson();
			Map<String, Object> resolved = gson.fromJson(updatePayload, Map.class);
			Map<String, Object> entityBody = (Map<String, Object>) JsonUtils.fromString(entityPayload);
			UpdateEntityRequest request = new UpdateEntityRequest(multimaparr, "urn:ngsi-ld:Vehicle:A103", entityBody,
					resolved, null);

			updateResult = entityService.updateEntry(multimaparr, "urn:ngsi-ld:Vehicle:A103", resolved);
			Assert.assertEquals(request.getUpdateResult().getUpdated(), updateResult.getUpdated());
			verify(entityService).updateEntry(any(), any(), any());
		} catch (Exception ex) {
			Assert.fail();
		}
	}

	/**
	 * this method is use for append the field or attribute in entity
	 */
	@Test
	public void appendFieldTest() {
		try {
			MockitoAnnotations.initMocks(this);
			ArrayListMultimap<String, String> entityIds = ArrayListMultimap.create();
			entityIds.put(AppConstants.INTERNAL_NULL_KEY, "urn:ngsi-ld:Vehicle:A103");
			ReflectionTestUtils.setField(entityService, "directDB", true);
			UpdateResult updateResult = new UpdateResult();
			Mockito.doReturn(entityPayload).when(entityInfoDAO).getEntity(any(), any());
			multimaparr.put("content-type", "application/json");
			Gson gson = new Gson();
			Map<String, Object> resolved = gson.fromJson(appendPayload, Map.class);
			Map<String, Object> entityBody = (Map<String, Object>) JsonUtils.fromString(entityPayload);
			AppendEntityRequest request = new AppendEntityRequest(multimaparr, "urn:ngsi-ld:Vehicle:A103", entityBody,
					resolved, null);
			updateResult = entityService.appendToEntry(multimaparr, "urn:ngsi-ld:Vehicle:A103", resolved, null);
			Assert.assertEquals(request.getUpdateResult().getUpdated(), updateResult.getUpdated());
			verify(entityService).appendToEntry(any(), any(), any(), any());
		} catch (Exception ex) {
			Assert.fail();
		}
	}

	/**
	 * this method is use for the update partial attribute field
	 */
	@Test
	public void updatePartialAttributeFieldTest() {
		try {

			MockitoAnnotations.initMocks(this);
			ArrayListMultimap<String, String> entityIds = ArrayListMultimap.create();
			entityIds.put(AppConstants.INTERNAL_NULL_KEY, "urn:ngsi-ld:Vehicle:A103");

			ReflectionTestUtils.setField(entityService, "directDB", true);
			UpdateResult updateResult = new UpdateResult();
			Mockito.doReturn(entityPayload).when(entityInfoDAO).getEntity(any(), any());
			multimaparr.put("content-type", "application/json");
			Gson gson = new Gson();
			Map<String, Object> resolved = gson.fromJson(updatePartialAttributesPayload, Map.class);
			Map<String, Object> entityBody = (Map<String, Object>) JsonUtils.fromString(entityPayload);
			UpdateEntityRequest request = new UpdateEntityRequest(multimaparr, "urn:ngsi-ld:Vehicle:A103", entityBody,
					resolved, "http://example.org/vehicle/speed");
			updateResult = entityService.partialUpdateEntity(multimaparr, "urn:ngsi-ld:Vehicle:A103",
					"http://example.org/vehicle/speed", resolved);
			Assert.assertEquals(request.getUpdateResult().getUpdated(), updateResult.getUpdated());
			verify(entityService).partialUpdateEntity(any(), any(), any(), any());
		} catch (Exception ex) {
			Assert.fail();
		}
	}

	/**
	 * this method is use for the update partial default attribute field
	 */
	@Test
	public void updatePartialDefaultAttributeFieldTest() {
		try {
			ArrayListMultimap<String, String> entityIds = ArrayListMultimap.create();
			entityIds.put(AppConstants.INTERNAL_NULL_KEY, "urn:ngsi-ld:Vehicle:A103");
			ReflectionTestUtils.setField(entityService, "directDB", true);
			UpdateResult updateResult = new UpdateResult();
			Mockito.doReturn(entityPayload).when(entityInfoDAO).getEntity(any(), any());
			multimaparr.put("content-type", "application/json");
			Gson gson = new Gson();
			Map<String, Object> resolved = gson.fromJson(updatePartialDefaultAttributesPayload, Map.class);
			Map<String, Object> entityBody = (Map<String, Object>) JsonUtils.fromString(entityPayload);
			UpdateEntityRequest request = new UpdateEntityRequest(multimaparr, "urn:ngsi-ld:Vehicle:A103", entityBody,
					resolved, "http://example.org/vehicle/speed");
			updateResult = entityService.partialUpdateEntity(multimaparr, "urn:ngsi-ld:Vehicle:A103",
					"http://example.org/vehicle/speed", resolved);
			Assert.assertEquals(request.getUpdateResult().getUpdated(), updateResult.getUpdated());
			verify(entityService).partialUpdateEntity(any(), any(), any(), any());
		} catch (Exception ex) {
			Assert.fail();
		}
	}

	/**
	 * this method is use for all delete the attribute
	 */
	@Test
	public void deleteAllAttributeInstanceTest() {
		try {
			ArrayListMultimap<String, String> entityIds = ArrayListMultimap.create();
			entityIds.put(AppConstants.INTERNAL_NULL_KEY, "urn:ngsi-ld:Vehicle:A103");
			ReflectionTestUtils.setField(entityService, "directDB", true);
			Mockito.doReturn(entityPayload).when(entityInfoDAO).getEntity(any(), any());
			multimaparr.put("content-type", "application/json");
			boolean result = entityService.deleteAttribute(multimaparr, "urn:ngsi-ld:Vehicle:A103",
					"http://example.org/vehicle/speed", null, null);
			Assert.assertEquals(true, result);
			verify(entityService).deleteAttribute(any(), any(), any(), any(), any());
		} catch (Exception ex) {
			Assert.fail();
		}
	}

	/**
	 * this method is use for all delete the attribute if entity id is not exist
	 */
	@Test
	public void deleteAllAttributeInstanceIfEnityNotExistTest() {
		try {
			ArrayListMultimap<String, String> entityIds = ArrayListMultimap.create();
			entityIds.put(AppConstants.INTERNAL_NULL_KEY, "urn:ngsi-ld:Vehicle:A104");
			ReflectionTestUtils.setField(entityService, "directDB", true);
			Mockito.doReturn(entityPayload).when(entityInfoDAO).getEntity(any(), any());
			multimaparr.put("content-type", "application/json");
			try {
				entityService.deleteAttribute(multimaparr, "urn:ngsi-ld:Vehicle:A103",
						"http://example.org/vehicle/speed", null, null);
			} catch (Exception e) {
				Assert.assertEquals("Entity Id " + "urn:ngsi-ld:Vehicle:A103" + " not found", e.getMessage());
			}
		} catch (Exception ex) {
			Assert.fail();
		}
	}

}
