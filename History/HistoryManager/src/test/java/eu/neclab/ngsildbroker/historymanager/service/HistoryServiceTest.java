
package eu.neclab.ngsildbroker.historymanager.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.powermock.modules.junit4.PowerMockRunnerDelegate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.transaction.support.TransactionTemplate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.google.common.collect.ArrayListMultimap;
import com.google.gson.Gson;
import com.zaxxer.hikari.HikariConfig;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.DBWriteTemplates;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.datatypes.requests.AppendHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CreateEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CreateHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.EntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.results.QueryResult;
import eu.neclab.ngsildbroker.commons.datatypes.results.UpdateResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.StorageFunctionsInterface;
import eu.neclab.ngsildbroker.commons.ngsiqueries.ParamsResolver;
import eu.neclab.ngsildbroker.commons.storage.StorageDAO;
import eu.neclab.ngsildbroker.historymanager.repository.HistoryDAO;

@RunWith(SpringRunner.class)
@TestPropertySource(properties = { "scorpio.directDB= true", })

@SpringBootTest

@AutoConfigureMockMvc
@PowerMockRunnerDelegate(SpringRunner.class)
public class HistoryServiceTest {

	@Value("${scorpio.directDB}")
	boolean directDB;

	@Autowired
	private MockMvc mockMvc;

	@Mock
	HistoryDAO historyDAO = mock(HistoryDAO.class);

	@Mock
	QueryResult queryResult;

	@Mock
	StorageDAO storageDAO = mock(StorageDAO.class);;

	@Mock
	ParamsResolver paramsResolver;

	@Mock
	JdbcTemplate template;

	@MockBean
	DBWriteTemplates defaultTemplates;

	@InjectMocks

	@Spy
	private HistoryService historyService;

	@Mock
	private JdbcTemplate writerJdbcTemplate;

	@Mock
	private DataSource writerDataSource;

	@Mock
	private HikariConfig hikariConfig;
	private Map<Object, DataSource> resolvedDataSources = new HashMap<>();
	private TransactionTemplate writerTransactionTemplate;
	private JdbcTemplate writerJdbcTemplateWithTransaction;

	private HashMap<String, DBWriteTemplates> tenant2Templates = new HashMap<String, DBWriteTemplates>();

	StorageFunctionsInterface storageFunctions;

	URI uri;

	private String temporalPayload;

	String tempupdatePayload;
	String tempAppendPayload;
	String entityPayload;
	String olddata;
	String tempupdatePartialAttributesPayload;
	String updatePartialDefaultAttributesPayload;
	JsonNode updateJsonNode;
	JsonNode appendJsonNode;
	JsonNode blankNode;
	JsonNode payloadNode;
	JsonNode updatePartialAttributesNode;
	JsonNode updatePartialDefaultAttributesNode;
	ArrayListMultimap<String, String> multimaparr = ArrayListMultimap.create();

	@Before
	public void setUp() throws Exception {
		uri = new URI(AppConstants.HISTORY_URL + "urn:ngsi-ld:testunit:151");
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

		updateJsonNode = objectMapper.readTree(tempupdatePayload);
		appendJsonNode = objectMapper.readTree(tempAppendPayload);
		blankNode = objectMapper.createObjectNode();
		payloadNode = objectMapper.readTree(temporalPayload);
		updatePartialAttributesNode = objectMapper.readTree(tempupdatePartialAttributesPayload);
		updatePartialDefaultAttributesNode = objectMapper.readTree(updatePartialDefaultAttributesPayload);
		directDB = true;
	}

	/**
	 * this method is use test "createTemporalEntityFromBinding" method of
	 * HistoryService
	 */

	@Test
	public void createTemporalEntityFromBindingTest() {
		try {
			multimaparr.put("content-type", "application/json");
			Gson gson = new Gson();
			Map<String, Object> resolved = gson.fromJson(temporalPayload, Map.class);
			CreateHistoryEntityRequest request = new CreateHistoryEntityRequest(multimaparr, resolved, false);
			String result = historyService.createEntry(multimaparr, resolved).getEntityId();
			assertEquals(result, request.getId());
			verify(historyService, times(1)).createEntry(any(), any());
			verify(historyService).handleRequest(any());
		} catch (Exception e) {
			Assert.fail();
			e.printStackTrace();
		}
	}

	/**
	 * this method is use for check the temporal entity is already exist
	 */
	@Test
	public void createTemporalEntityThrowsAlreadyExistTest() throws ResponseException, Exception {
		MockitoAnnotations.initMocks(this);
		ArrayListMultimap<String, String> entityIds = ArrayListMultimap.create();
		entityIds.put(AppConstants.INTERNAL_NULL_KEY, "urn:ngsi-ld:Vehicle:2");
		ReflectionTestUtils.setField(historyService, "directDB", true);
		multimaparr.put("content-type", "application/json");
		Gson gson = new Gson();
		Map<String, Object> resolved = gson.fromJson(temporalPayload, Map.class);
		EntityRequest request = new CreateEntityRequest(resolved, multimaparr);
		Mockito.doThrow(new ResponseException(ErrorType.AlreadyExists, request.getId() + " already exists"))
				.when(historyService).createEntry(multimaparr, resolved);
		try {
			historyService.createEntry(multimaparr, resolved);
		} catch (Exception e) {
			Assert.assertEquals(request.getId() + " already exists", e.getMessage());
		}

	}

	/**
	 * this method is use for append the field or attribute in Temporal entity
	 */
	@Test
	public void appendTemporalFieldTest() {
		try {
			multimaparr.put("content-type", "application/json");
			UpdateResult updateResult = new UpdateResult();
			MockitoAnnotations.initMocks(this);
			ArrayListMultimap<String, String> entityIds = ArrayListMultimap.create();
			entityIds.put(AppConstants.INTERNAL_NULL_KEY, "urn:ngsi-ld:Vehicle:1");
			ReflectionTestUtils.setField(historyService, "directDB", true);
			Gson gson = new Gson();
			Map<String, Object> resolved = gson.fromJson(tempAppendPayload, Map.class);
			String[] optionsArray = new String[0];
			AppendHistoryEntityRequest request = new AppendHistoryEntityRequest(multimaparr, resolved,
					"urn:ngsi-ld:Vehicle:1");
			updateResult = historyService.appendToEntry(multimaparr, "urn:ngsi-ld:Vehicle:1", resolved, optionsArray);
			Assert.assertEquals(updateResult.getUpdated(), request.getUpdateResult().getUpdated());
			verify(historyService).handleRequest(any());
		} catch (Exception e) {
			Assert.fail();
			e.printStackTrace();
		}
	}

	/**
	 * this method is use for append the field or attribute in temporal entity if
	 * entity id is not exist
	 */
	@Test
	public void appendFieldTemporalEntityNotExistTest() {
		try {
			MockitoAnnotations.initMocks(this);
			ArrayListMultimap<String, String> entityIds = ArrayListMultimap.create();
			entityIds.put(AppConstants.INTERNAL_NULL_KEY, "urn:ngsi-ld:Vehicle:2");
			ReflectionTestUtils.setField(historyService, "directDB", true);
			multimaparr.put("content-type", "application/json");
			Gson gson = new Gson();
			String[] optionsArray = new String[0];
			Map<String, Object> resolved = gson.fromJson(tempAppendPayload, Map.class);
			try {
				historyService.appendToEntry(multimaparr, "urn:ngsi-ld:Vehicle:1", resolved, optionsArray);
			} catch (Exception e) {
				Assert.assertEquals("You cannot create an attribute on a none existing entity", e.getMessage());
			}
		} catch (Exception ex) {
			Assert.fail();
		}
	}

	/**
	 * this method is use for all delete the temporal entity
	 */
	@Test
	public void deleteTemporalByIdTest() {
		try {
			multimaparr.put("content-type", "application/json");
			MockitoAnnotations.initMocks(this);
			ArrayListMultimap<String, String> entityIds = ArrayListMultimap.create();
			entityIds.put(AppConstants.INTERNAL_NULL_KEY, "urn:ngsi-ld:Vehicle:1");
			ReflectionTestUtils.setField(historyService, "directDB", true);
			boolean result = historyService.deleteEntry(multimaparr, "urn:ngsi-ld:Vehicle:1");
			assertEquals(result, true);
			verify(historyService).handleRequest(any());
		} catch (Exception e) {
			Assert.fail();
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
			ReflectionTestUtils.setField(historyService, "directDB", true);
			multimaparr.put("content-type", "application/json");
			try {
				historyService.deleteEntry(multimaparr, "urn:ngsi-ld:Vehicle:1");
			} catch (Exception e) {
				Assert.assertEquals("urn:ngsi-ld:Vehicle:1" + " not found", e.getMessage());
			}
		} catch (Exception ex) {
			Assert.fail();
		}
	}

	/**
	 * this method is use for update temporal entity
	 */
	@Test
	public void updateTemporalFieldTest() throws Exception {
		try {
			ArrayListMultimap<String, String> entityIds = ArrayListMultimap.create();
			entityIds.put(AppConstants.INTERNAL_NULL_KEY, "urn:ngsi-ld:Vehicle:1");
			multimaparr.put("content-type", "application/json");
			Context context = JsonLdProcessor.getCoreContextClone();
			Gson gson = new Gson();
			Map<String, Object> resolved = gson.fromJson(tempupdatePayload, Map.class);
			QueryParams qp = new QueryParams();
			List<Map<String, String>> temp1 = new ArrayList<Map<String, String>>();
			HashMap<String, String> temp2 = new HashMap<String, String>();
			temp2.put(NGSIConstants.JSON_LD_ID, "urn:ngsi-ld:Vehicle:1");
			temp1.add(temp2);
			qp.setEntities(temp1);
			qp.setAttrs("https://uri.etsi.org/ngsi-ld/default-context/brandName");
			qp.setInstanceId("urn:ngsi-ld:9c7690ed-eba4-4d95-a28b-4584f953f8ab");
			qp.setIncludeSysAttrs(true);
			List<String> list = new ArrayList<String>();
			list.add(olddata);
			QueryResult queryResult = new QueryResult(null, null, ErrorType.None, -1, true);
			queryResult.setActualDataString(list);
			when(historyDAO.query(any())).thenReturn(queryResult);
			historyService.modifyAttribInstanceTemporalEntity(multimaparr, "urn:ngsi-ld:Vehicle:1", resolved,
					"https://uri.etsi.org/ngsi-ld/default-context/brandName",
					"urn:ngsi-ld:9c7690ed-eba4-4d95-a28b-4584f953f8ab", context);
			verify(historyService, times(1)).handleRequest(any());
		} catch (Exception ex) {
			Assert.fail();
		}
	}

	/**
	 * this method is use for update temporal entity if entity is not found
	 */
	@Test
	public void updateTemporalFieldIfEntityNotExistTest() throws Exception {
		try {
			ArrayListMultimap<String, String> entityIds = ArrayListMultimap.create();
			entityIds.put(AppConstants.INTERNAL_NULL_KEY, "urn:ngsi-ld:Vehicle:1");
			multimaparr.put("content-type", "application/json");
			Context context = JsonLdProcessor.getCoreContextClone();
			Gson gson = new Gson();
			Map<String, Object> resolved = gson.fromJson(tempupdatePayload, Map.class);
			QueryParams qp = new QueryParams();
			List<Map<String, String>> temp1 = new ArrayList<Map<String, String>>();
			HashMap<String, String> temp2 = new HashMap<String, String>();
			temp2.put(NGSIConstants.JSON_LD_ID, "urn:ngsi-ld:Vehicle:1");
			temp1.add(temp2);
			qp.setEntities(temp1);
			qp.setAttrs("https://uri.etsi.org/ngsi-ld/default-context/brandName");
			qp.setInstanceId("urn:ngsi-ld:9c7690ed-eba4-4d95-a28b-4584f953f8ab");
			qp.setIncludeSysAttrs(true);
			List<String> list = new ArrayList<String>();
			QueryResult queryResult = new QueryResult(null, null, ErrorType.None, -1, true);
			queryResult.setActualDataString(list);
			when(historyDAO.query(any())).thenReturn(queryResult);
			try {
				historyService.modifyAttribInstanceTemporalEntity(multimaparr, "urn:ngsi-ld:Vehicle:1", resolved,
						"https://uri.etsi.org/ngsi-ld/default-context/brandName",
						"urn:ngsi-ld:9c7690ed-eba4-4d95-a28b-4584f953f8ab", context);
			} catch (Exception e) {
				Assert.assertEquals("Entity not found", e.getMessage());
			}

		} catch (Exception ex) {
			Assert.fail();
		}
	}

}