package eu.neclab.ngsildbroker.registryhandler.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.powermock.modules.junit4.PowerMockRunnerDelegate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.util.ReflectionTestUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ArrayListMultimap;
import com.google.gson.Gson;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.AppendCSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CreateCSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.results.UpdateResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.storage.StorageDAO;
import eu.neclab.ngsildbroker.registryhandler.repository.CSourceDAO;

@RunWith(SpringRunner.class)
@SpringBootTest

@AutoConfigureMockMvc
@PowerMockRunnerDelegate(SpringRunner.class)
public class CSourceServiceTest {

	@InjectMocks
	@Spy
	CSourceService csourceService;

	@Value("${scorpio.registry.autorecording:active}")
	String AUTO_REG_STATUS;

	@MockBean
	ObjectMapper objectMapper;

	@Mock
	CSourceDAO csourceInfoDAO;

	@Mock
	StorageDAO storageDAO = mock(StorageDAO.class);

	String payload;
	String entitypayload;
	String updatePayload;
	String headers;
	JsonNode blankNode;
	JsonNode payloadNode;
	ArrayListMultimap<String, String> multimaparr = ArrayListMultimap.create();

	@Before
	public void setup() throws Exception {
		ObjectMapper objectMapper = new ObjectMapper();
		// @formatter:off			
		payload = "{\r\n"
				+ "  \"https://uri.etsi.org/ngsi-ld/description\" : [ {\r\n"
				+ "    \"@value\" : \"DescriptionExample\"\r\n"
				+ "  } ],\r\n"
				+ "  \"https://uri.etsi.org/ngsi-ld/endpoint\" : [ {\r\n"
				+ "    \"@value\" : \"http://my.csource.org:1026\"\r\n"
				+ "  } ],\r\n"
				+ "  \"@id\" : \"urn:ngsi-ld:ContextSourceRegistration:csr4\",\r\n"
				+ "  \"https://uri.etsi.org/ngsi-ld/information\" : [ {\r\n"
				+ "    \"https://uri.etsi.org/ngsi-ld/entities\" : [ {\r\n"
				+ "      \"@id\" : \"urn:ngsi-ld:Vehicle:A456\",\r\n"
				+ "      \"@type\" : [ \"https://uri.etsi.org/ngsi-ld/default-context/Vehicle\" ]\r\n"
				+ "    } ],\r\n"
				+ "    \"https://uri.etsi.org/ngsi-ld/properties\" : [ {\r\n"
				+ "      \"@id\" : \"https://uri.etsi.org/ngsi-ld/default-context/brandName\"\r\n"
				+ "    }, {\r\n"
				+ "      \"@id\" : \"https://uri.etsi.org/ngsi-ld/default-context/speed\"\r\n"
				+ "    } ],\r\n"
				+ "    \"https://uri.etsi.org/ngsi-ld/relationships\" : [ {\r\n"
				+ "      \"@id\" : \"https://uri.etsi.org/ngsi-ld/default-context/isParked\"\r\n"
				+ "    } ]\r\n"
				+ "  } ],\r\n"
				+ "  \"https://uri.etsi.org/ngsi-ld/name\" : [ {\r\n"
				+ "    \"@value\" : \"NameExample\"\r\n"
				+ "  } ],\r\n"
				+ "  \"@type\" : [ \"https://uri.etsi.org/ngsi-ld/ContextSourceRegistration\" ]\r\n"
				+ "}";
		updatePayload="{\r\n"
				+ "  \"https://uri.etsi.org/ngsi-ld/description\" : [ {\r\n"
				+ "    \"@value\" : \"DescriptionExample\"\r\n"
				+ "  } ],\r\n"
				+ "  \"https://uri.etsi.org/ngsi-ld/endpoint\" : [ {\r\n"
				+ "    \"@value\" : \"http://my.csource.org:1026\"\r\n"
				+ "  } ],\r\n"
				+ "  \"@id\" : \"urn:ngsi-ld:ContextSourceRegistration:csr\",\r\n"
				+ "  \"https://uri.etsi.org/ngsi-ld/information\" : [ {\r\n"
				+ "    \"https://uri.etsi.org/ngsi-ld/entities\" : [ {\r\n"
				+ "      \"@id\" : \"urn:ngsi-ld:Vehicle:A456\",\r\n"
				+ "      \"@type\" : [ \"https://uri.etsi.org/ngsi-ld/default-context/Vehicle\" ]\r\n"
				+ "    } ],\r\n"
				+ "    \"https://uri.etsi.org/ngsi-ld/properties\" : [ {\r\n"
				+ "      \"@id\" : \"https://uri.etsi.org/ngsi-ld/default-context/brandName\"\r\n"
				+ "    }, {\r\n"
				+ "      \"@id\" : \"https://uri.etsi.org/ngsi-ld/default-context/speed\"\r\n"
				+ "    }, {\r\n"
				+ "      \"@id\" : \"https://uri.etsi.org/ngsi-ld/default-context/brandName\"\r\n"
				+ "    } ],\r\n"
				+ "    \"https://uri.etsi.org/ngsi-ld/relationships\" : [ {\r\n"
				+ "      \"@id\" : \"https://uri.etsi.org/ngsi-ld/default-context/isParked\"\r\n"
				+ "    } ]\r\n"
				+ "  } ],\r\n"
				+ "  \"https://uri.etsi.org/ngsi-ld/name\" : [ {\r\n"
				+ "    \"@value\" : \"NameExample\"\r\n"
				+ "  } ],\r\n"
				+ "  \"@type\" : [ \"https://uri.etsi.org/ngsi-ld/ContextSourceRegistration\" ]\r\n"
				+ "}";
		
		entitypayload = "{\r\n"				
				+ "    \"https://uri.etsi.org/ngsi-ld/entities\" : [ {\r\n"
				+ "      \"@id\" : \"urn:ngsi-ld:Vehicle:A456\",\r\n"
				+ "      \"@type\" : [ \"https://uri.etsi.org/ngsi-ld/default-context/Vehicle\" ]\r\n"
				+ "    } ],\r\n"				
				+ "}";
		
		// @formatter:on
		headers = "{content-length=[883], x-forwarded-proto=[http], postman-token=[8f71bb12-8223-44a4-9322-9853fae06baa], x-forwarded-port=[9090], x-forwarded-for=[0:0:0:0:0:0:0:1], accept=[*/*], ngsild-tenant=[csource1], x-forwarded-host=[localhost:9090], host=[DLLT-9218.nectechnologies.in:1030], content-type=[application/json], connection=[Keep-Alive], accept-encoding=[gzip, deflate], user-agent=[PostmanRuntime/7.6.0]}";
		payloadNode = objectMapper.readTree(payload.getBytes());

	}

	@After
	public void teardown() {
		payload = null;
		updatePayload = null;
		blankNode = null;
	}

	/**
	 * this method is use for create CSource Registry
	 */

	@Test
	public void registerCSourceTest() {
		try {
			multimaparr.put("content-type", "application/json");
			Gson gson = new Gson();
			Map<String, Object> resolved = gson.fromJson(payload, Map.class);
			CSourceRequest request = new CreateCSourceRequest(resolved, multimaparr,
					"urn:ngsi-ld:ContextSourceRegistration:csr4");
			when(csourceInfoDAO.storeRegistryEntry(request)).thenReturn(true);
			csourceInfoDAO.storeRegistryEntry(request);
			String id = csourceService.createEntry(multimaparr, resolved).getEntityId();
			Assert.assertEquals("urn:ngsi-ld:ContextSourceRegistration:csr4", id);
		} catch (Exception ex) {
			Assert.fail();
		}
	}

	/**
	 * this method is use for update CSource Registry
	 */

	@Test
	public void updateCSourceTest() {
		try {

			ArrayListMultimap<String, String> csourceIds = ArrayListMultimap.create();
			csourceIds.put(AppConstants.INTERNAL_NULL_KEY, "urn:ngsi-ld:ContextSourceRegistration:csr3");
			// when(csourceInfoDAO.getAllIds()).thenReturn(csourceIds);
			HashMap<String, List<String>> result = new HashMap<String, List<String>>();
			List<String> entities = new ArrayList<String>();
			result.put(AppConstants.INTERNAL_NULL_KEY, entities);
			when(csourceInfoDAO.getAllEntities()).thenReturn(result);
			ReflectionTestUtils.setField(csourceService, "AUTO_REG_STATUS", "active");
			Method postConstruct = CSourceService.class.getDeclaredMethod("loadStoredEntitiesDetails"); // methodName,parameters
			postConstruct.setAccessible(true);
			postConstruct.invoke(csourceService);
			multimaparr.put("content-type", "application/json");
			UpdateResult updateResult = new UpdateResult();
			Gson gson = new Gson();
			Map<String, Object> resolved = gson.fromJson(updatePayload, Map.class);
			Map<String, Object> originalresolved = gson.fromJson(payload, Map.class);
			String[] optionsArray = new String[0];
			AppendCSourceRequest request = new AppendCSourceRequest(multimaparr,
					"urn:ngsi-ld:ContextSourceRegistration:csr3", originalresolved, resolved, optionsArray);
			when(csourceInfoDAO.getEntity(any(), any())).thenReturn(payload);
			updateResult = csourceService.appendToEntry(multimaparr, "urn:ngsi-ld:ContextSourceRegistration:csr3",
					resolved, optionsArray);
			Assert.assertEquals(updateResult.getUpdated(), request.getUpdateResult().getUpdated());

		} catch (Exception ex) {
			Assert.fail();
		}
	}

	/**
	 * this method is use for update CSource Registry if ID is not exist
	 */
	@Test
	public void updateCSourceRegIdNotExistTest() {
		try {

			ArrayListMultimap<String, String> csourceIds = ArrayListMultimap.create();
			csourceIds.put(AppConstants.INTERNAL_NULL_KEY, "urn:ngsi-ld:ContextSourceRegistration:csr4");
			// when(csourceInfoDAO.getAllIds()).thenReturn(csourceIds);
			HashMap<String, List<String>> result = new HashMap<String, List<String>>();
			List<String> entities = new ArrayList<String>();
			result.put(AppConstants.INTERNAL_NULL_KEY, entities);
			when(csourceInfoDAO.getAllEntities()).thenReturn(result);
			ReflectionTestUtils.setField(csourceService, "AUTO_REG_STATUS", "active");
			Method postConstruct = CSourceService.class.getDeclaredMethod("loadStoredEntitiesDetails"); // methodName,parameters
			postConstruct.setAccessible(true);
			postConstruct.invoke(csourceService);
			multimaparr.put("content-type", "application/json");
			UpdateResult updateResult = new UpdateResult();
			Gson gson = new Gson();
			Map<String, Object> resolved = gson.fromJson(updatePayload, Map.class);
			Map<String, Object> originalresolved = gson.fromJson(payload, Map.class);
			String[] optionsArray = new String[0];
			AppendCSourceRequest request = new AppendCSourceRequest(multimaparr,
					"urn:ngsi-ld:ContextSourceRegistration:csr3", originalresolved, resolved, optionsArray);
			when(csourceInfoDAO.getEntity(any(), any())).thenReturn(payload);
			updateResult = csourceService.appendToEntry(multimaparr, "urn:ngsi-ld:ContextSourceRegistration:csr3",
					resolved, optionsArray);
			Assert.assertEquals(updateResult.getUpdated(), request.getUpdateResult().getUpdated());

		} catch (Exception ex) {
			Assert.fail();
		}
	}

	/**
	 * this method is use for delete CSource Registry
	 */
	@Test
	public void deleteCSorceTest() throws Exception {
		try {

			ArrayListMultimap<String, String> csourceIds = ArrayListMultimap.create();
			csourceIds.put(AppConstants.INTERNAL_NULL_KEY, "urn:ngsi-ld:ContextSourceRegistration:csr3");
			// when(csourceInfoDAO.getAllIds()).thenReturn(csourceIds);
			HashMap<String, List<String>> result = new HashMap<String, List<String>>();
			List<String> entities = new ArrayList<String>();
			result.put(AppConstants.INTERNAL_NULL_KEY, entities);
			when(csourceInfoDAO.getAllEntities()).thenReturn(result);
			ReflectionTestUtils.setField(csourceService, "AUTO_REG_STATUS", "active");
			Method postConstruct = CSourceService.class.getDeclaredMethod("loadStoredEntitiesDetails"); // methodName,parameters
			postConstruct.setAccessible(true);
			postConstruct.invoke(csourceService);
			multimaparr.put("content-type", "application/json");
			when(csourceInfoDAO.getEntity(any(), any())).thenReturn(payload);
			boolean deleteresult = csourceService.deleteEntry(multimaparr,
					"urn:ngsi-ld:ContextSourceRegistration:csr3");
			Assert.assertEquals(true, deleteresult);
		} catch (Exception ex) {
			Assert.fail();
		}
	}

	/**
	 * this method is use for delete CSource Registry if ID is not exist
	 */
	@Test
	public void deleteCSorceIDNotExistTest() throws Exception {
		try {

			ArrayListMultimap<String, String> csourceIds = ArrayListMultimap.create();
			csourceIds.put(AppConstants.INTERNAL_NULL_KEY, "urn:ngsi-ld:ContextSourceRegistration:csr3");
			// when(csourceInfoDAO.getAllIds()).thenReturn(csourceIds);
			HashMap<String, List<String>> result = new HashMap<String, List<String>>();
			List<String> entities = new ArrayList<String>();
			result.put(AppConstants.INTERNAL_NULL_KEY, entities);
			when(csourceInfoDAO.getAllEntities()).thenReturn(result);
			ReflectionTestUtils.setField(csourceService, "AUTO_REG_STATUS", "active");
			Method postConstruct = CSourceService.class.getDeclaredMethod("loadStoredEntitiesDetails"); // methodName,parameters
			postConstruct.setAccessible(true);
			postConstruct.invoke(csourceService);
			multimaparr.put("content-type", "application/json");
			String registrationid = "urn:ngsi-ld:ContextSourceRegistration:csr3";
			when(csourceInfoDAO.getEntity(any(), any()))
					.thenThrow(new ResponseException(ErrorType.NotFound, registrationid + " not found"));
			try {
				csourceService.deleteEntry(multimaparr, "urn:ngsi-ld:ContextSourceRegistration:csr4");
			} catch (Exception e) {
				Assert.assertEquals(registrationid + " not found", e.getMessage());
			}

		} catch (Exception ex) {
			Assert.fail();
		}
	}

	/**
	 * this method is use for get CSource Registry by Id
	 */
	@Test
	public void getCSourceRegistrationByIdTest() throws Exception {
		try {

			ArrayListMultimap<String, String> csourceIds = ArrayListMultimap.create();
			csourceIds.put(AppConstants.INTERNAL_NULL_KEY, "urn:ngsi-ld:ContextSourceRegistration:csr3");
			// when(csourceInfoDAO.getAllIds()).thenReturn(csourceIds);
			HashMap<String, List<String>> result = new HashMap<String, List<String>>();
			List<String> entities = new ArrayList<String>();
			result.put(AppConstants.INTERNAL_NULL_KEY, entities);
			when(csourceInfoDAO.getAllEntities()).thenReturn(result);
			ReflectionTestUtils.setField(csourceService, "AUTO_REG_STATUS", "active");
			Method postConstruct = CSourceService.class.getDeclaredMethod("loadStoredEntitiesDetails"); // methodName,parameters
			postConstruct.setAccessible(true);
			postConstruct.invoke(csourceService);
			multimaparr.put("content-type", "application/json");
			when(csourceInfoDAO.getEntity(any(), any())).thenReturn(payload);
			String getresult = csourceService.getCSourceRegistrationById(null,
					"urn:ngsi-ld:ContextSourceRegistration:csr3");
			Assert.assertEquals(payload, getresult);
		} catch (Exception ex) {
			Assert.fail();
		}
	}

	/**
	 * this method is use for get CSource Registry if ID is not exist
	 */
	@Test
	public void getCSourceRegistrationByIdNotExistTest() throws Exception {
		try {

			ArrayListMultimap<String, String> csourceIds = ArrayListMultimap.create();
			csourceIds.put(AppConstants.INTERNAL_NULL_KEY, "urn:ngsi-ld:ContextSourceRegistration:csr3");
			HashMap<String, List<String>> result = new HashMap<String, List<String>>();
			List<String> entities = new ArrayList<String>();
			result.put(AppConstants.INTERNAL_NULL_KEY, entities);
			when(csourceInfoDAO.getAllEntities()).thenReturn(result);
			ReflectionTestUtils.setField(csourceService, "AUTO_REG_STATUS", "active");
			Method postConstruct = CSourceService.class.getDeclaredMethod("loadStoredEntitiesDetails");
			postConstruct.setAccessible(true);
			postConstruct.invoke(csourceService);
			multimaparr.put("content-type", "application/json");
			String registrationid = "urn:ngsi-ld:ContextSourceRegistration:csr3";
			when(csourceInfoDAO.getEntity(any(), any()))
					.thenThrow(new ResponseException(ErrorType.NotFound, registrationid + " not found"));
			try {
				csourceService.getCSourceRegistrationById(null, "urn:ngsi-ld:ContextSourceRegistration:csr4");
			} catch (Exception e) {
				Assert.assertEquals(registrationid + " not found", e.getMessage());
			}

		} catch (Exception ex) {
			Assert.fail();
		}
	}

	/**
	 * this method is use for get CSource Registry by id if tenant is null
	 */
	@Test
	public void getCSourceRegistrationTest() throws Exception {
		try {

			ArrayListMultimap<String, String> csourceIds = ArrayListMultimap.create();
			csourceIds.put(AppConstants.INTERNAL_NULL_KEY, "urn:ngsi-ld:ContextSourceRegistration:csr3");
			HashMap<String, List<String>> result = new HashMap<String, List<String>>();
			List<String> entities = new ArrayList<String>();
			result.put(AppConstants.INTERNAL_NULL_KEY, entities);
			when(csourceInfoDAO.getAllEntities()).thenReturn(result);
			ReflectionTestUtils.setField(csourceService, "AUTO_REG_STATUS", "active");
			Method postConstruct = CSourceService.class.getDeclaredMethod("loadStoredEntitiesDetails"); // methodName,parameters
			postConstruct.setAccessible(true);
			postConstruct.invoke(csourceService);
			multimaparr.put("content-type", "application/json");
			String tenantId = AppConstants.INTERNAL_NULL_KEY;
			ReflectionTestUtils.setField(csourceService, "directDB", true);
			List<String> csourceresult = new ArrayList<String>();
			csourceresult.add(payload);
			when(csourceInfoDAO.getAllRegistrations(any())).thenReturn(csourceresult);
			List<String> getresult = csourceService.getCSourceRegistrations(tenantId);
			Assert.assertEquals(csourceresult, getresult);
		} catch (Exception ex) {
			Assert.fail();
		}
	}

}
