package eu.neclab.ngsildbroker.registryhandler.service;

import static org.mockito.ArgumentMatchers.any;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ArrayListMultimap;
import com.google.gson.Gson;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.AppendCSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CreateCSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.results.CreateResult;
import eu.neclab.ngsildbroker.commons.datatypes.results.UpdateResult;
import eu.neclab.ngsildbroker.commons.storage.StorageDAO;
import eu.neclab.ngsildbroker.registryhandler.repository.CSourceDAO;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;

@QuarkusTest
@TestMethodOrder(OrderAnnotation.class)
public class CSourceServiceTest {
	@InjectMocks
	@Spy
	CSourceService csourceService;

	@ConfigProperty(name = "scorpio.registry.autoregmode", defaultValue = "types")
	String AUTO_REG_MODE;

	@ConfigProperty(name = "scorpio.registry.autorecording", defaultValue = "active")
	String AUTO_REG_STATUS;

	@ConfigProperty(name = "scorpio.topics.registry")
	String CSOURCE_TOPIC;
	@Mock
	ObjectMapper objectMapper;

	@Mock
	CSourceDAO csourceDAO;

	@Mock
	MutinyEmitter<BaseRequest> kafkaSenderInterface;

	@Mock
	StorageDAO storageDAO = Mockito.mock(StorageDAO.class);

	String payload;
	private String payloadNotFound;

	String entitypayload;
	String updatePayload;
	String headers;
	JsonNode blankNode;
	JsonNode payloadNode;
	ArrayListMultimap<String, String> multimaparr = ArrayListMultimap.create();

	@BeforeEach
	public void setup() throws Exception {
		MockitoAnnotations.initMocks(this);

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
		
		
		payloadNotFound = "{\r\n"
				+ "  \"https://uri.etsi.org/ngsi-ld/description\" : [ {\r\n"
				+ "    \"@value\" : \"DescriptionExample\"\r\n"
				+ "  } ],\r\n"
				+ "  \"https://uri.etsi.org/ngsi-ld/endpoint\" : [ {\r\n"
				+ "    \"@value\" : \"http://my.csource.org:1026\"\r\n"
				+ "  } ],\r\n"
				+ "  \"@id\" : \"\",\r\n"
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

	@AfterEach
	public void teardown() {
		payload = null;
		updatePayload = null;
		blankNode = null;
	}

	/**
	 * this method is use for create CSource Registry
	 */
	@Test
	public void registerCSourceTest() throws Exception {

		multimaparr.put("content-type", "application/json");
		Gson gson = new Gson();
		Map<String, Object> resolved = gson.fromJson(payload, Map.class);
		CSourceRequest request = new CreateCSourceRequest(resolved, AppConstants.INTERNAL_NULL_KEY,
				"urn:ngsi-ld:ContextSourceRegistration:csr4");
		Mockito.when(csourceDAO.storeRegistryEntry(any())).thenReturn(Uni.createFrom().voidItem());
		CreateResult result = csourceService.createEntry(AppConstants.INTERNAL_NULL_KEY, resolved).await()
				.indefinitely();
		Assertions.assertEquals("urn:ngsi-ld:ContextSourceRegistration:csr4", result.getEntityId());
		Mockito.verify(csourceService).createEntry(any(), any());
	}

	/**
	 * this method is use for create CSource Registry but Id not provided
	 */
	@Test
	public void registerCSourceIdNotProvideTest() throws Exception {
		multimaparr.put("content-type", "application/json");
		Gson gson = new Gson();
		Map<String, Object> resolved = gson.fromJson(payload, Map.class);
		resolved.put(NGSIConstants.JSON_LD_ID, null);
		CSourceRequest request = new CreateCSourceRequest(resolved, AppConstants.INTERNAL_NULL_KEY,
				"urn:ngsi-ld:ContextSourceRegistration:csr4");
		Mockito.when(csourceDAO.storeRegistryEntry(any())).thenReturn(Uni.createFrom().voidItem());
		CreateResult result = csourceService.createEntry(AppConstants.INTERNAL_NULL_KEY, resolved).await()
				.indefinitely();
		Assertions.assertNotEquals(null, result.getEntityId());
		Mockito.verify(csourceService).createEntry(any(), any());
	}

	/**
	 * this method is use to validate CSource Registry Id is null
	 */
	@Test
	public void registerCSourceIdNullFoundTest() throws Exception {
		multimaparr.put("content-type", "application/json");
		Gson gson = new Gson();
		Map<String, Object> resolved = gson.fromJson(payloadNotFound, Map.class);
		CSourceRequest request = new CreateCSourceRequest(resolved, AppConstants.INTERNAL_NULL_KEY,
				"urn:ngsi-ld:ContextSourceRegistration:csr4");
		Mockito.when(csourceDAO.storeRegistryEntry(any())).thenReturn(Uni.createFrom().voidItem());
		CreateResult result = csourceService.createEntry(AppConstants.INTERNAL_NULL_KEY, resolved).await()
				.indefinitely();
		Assertions.assertEquals("", result.getEntityId());
		Mockito.verify(csourceService).createEntry(any(), any());
	}

	/**
	 * this method is use to Validate update CSource Registry
	 */
	@Test
	public void updateCSourceTest() throws Exception {
		try {
			ArrayListMultimap<String, String> csourceIds = ArrayListMultimap.create();
			csourceIds.put(AppConstants.INTERNAL_NULL_KEY, "urn:ngsi-ld:ContextSourceRegistration:csr3");
			HashMap<String, List<Map<String, Object>>> result = new HashMap<String, List<Map<String, Object>>>();
			List<Map<String, Object>> entities = new ArrayList<Map<String, Object>>();
			result.put(AppConstants.INTERNAL_NULL_KEY, entities);
			multimaparr.put("content-type", "application/json");
			UpdateResult updateResult = new UpdateResult();
			Gson gson = new Gson();
			Map<String, Object> resolved = gson.fromJson(updatePayload, Map.class);
			Map<String, Object> originalresolved = gson.fromJson(payload, Map.class);
			String[] optionsArray = new String[0];
			AppendCSourceRequest request = new AppendCSourceRequest(AppConstants.INTERNAL_NULL_KEY,
					"urn:ngsi-ld:ContextSourceRegistration:csr3", originalresolved, resolved, optionsArray);
			Mockito.when(csourceDAO.getEntity(any(), any())).thenReturn(Uni.createFrom().item(originalresolved));
			updateResult = csourceService.appendToEntry(AppConstants.INTERNAL_NULL_KEY,
					"urn:ngsi-ld:ContextSourceRegistration:csr3", resolved, optionsArray).await().indefinitely();
			Assertions.assertEquals(updateResult.getUpdated(), request.getUpdateResult().getUpdated());
			Mockito.verify(csourceService).appendToEntry(any(), any(), any(), any());
		} catch (Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}

	}

	/**
	 * this method is use to Validate update CSource Registry Bad Request
	 */
	@Test
	public void updateCSourceBadRequestTest() throws Exception {
		ArrayListMultimap<String, String> csourceIds = ArrayListMultimap.create();
		csourceIds.put(AppConstants.INTERNAL_NULL_KEY, "urn:ngsi-ld:ContextSourceRegistration:csr3");
		HashMap<String, List<Map<String, Object>>> result = new HashMap<String, List<Map<String, Object>>>();
		List<Map<String, Object>> entities = new ArrayList<Map<String, Object>>();
		result.put(AppConstants.INTERNAL_NULL_KEY, entities);
		multimaparr.put("content-type", "application/json");
		UpdateResult updateResult = new UpdateResult();
		Gson gson = new Gson();
		Map<String, Object> resolved = gson.fromJson(updatePayload, Map.class);
		Map<String, Object> originalresolved = gson.fromJson(payload, Map.class);
		String[] optionsArray = new String[0];
		AppendCSourceRequest request = new AppendCSourceRequest(AppConstants.INTERNAL_NULL_KEY,
				"urn:ngsi-ld:ContextSourceRegistration:csr3", originalresolved, resolved, optionsArray);
		Mockito.when(csourceDAO.getEntity(any(), any())).thenReturn(Uni.createFrom().item(originalresolved));
		try {
			csourceService.appendToEntry(AppConstants.INTERNAL_NULL_KEY, null, resolved, optionsArray).await()
					.indefinitely();
		} catch (Exception e) {
			Assertions.assertEquals("empty entity id is not allowed", e.getMessage());
			Mockito.verify(csourceService).appendToEntry(any(), any(), any(), any());
		}
	}

	/**
	 * this method is use for get CSource Registry by Id
	 */
	@Test
	public void getRegistrationByIdTest() {
		try {
			ArrayListMultimap<String, String> csourceIds = ArrayListMultimap.create();
			csourceIds.put(AppConstants.INTERNAL_NULL_KEY, "urn:ngsi-ld:ContextSourceRegistration:csr3");
			HashMap<String, List<Map<String, Object>>> result = new HashMap<String, List<Map<String, Object>>>();
			List<Map<String, Object>> entities = new ArrayList<Map<String, Object>>();
			result.put(AppConstants.INTERNAL_NULL_KEY, entities);
			Gson gson = new Gson();
			Map<String, Object> originalresolved = gson.fromJson(payload, Map.class);
			multimaparr.put("content-type", "application/json");
			Mockito.when(csourceDAO.getRegistrationById(any(), any()))
					.thenReturn(Uni.createFrom().item(originalresolved));
			Map<String, Object> getresult = csourceService
					.getRegistrationById("urn:ngsi-ld:ContextSourceRegistration:csr3", null).await().indefinitely();
			Assertions.assertEquals(originalresolved, getresult);
			Mockito.verify(csourceService).getRegistrationById(any(), any());
		} catch (Exception e) {
			Assertions.fail();
			e.printStackTrace();
		}
	}

	/**
	 * this method is use for get update CSource Registry by Id
	 */
	@Test
	public void updateCSourceEntrybyIdTest() throws Exception {
		ArrayListMultimap<String, String> csourceIds = ArrayListMultimap.create();
		csourceIds.put(AppConstants.INTERNAL_NULL_KEY, "urn:ngsi-ld:ContextSourceRegistration:csr3");
		multimaparr.put("content-type", "application/json");
		UpdateResult updateResult = new UpdateResult();
		Gson gson = new Gson();
		Map<String, Object> resolved = gson.fromJson(updatePayload, Map.class);
		Map<String, Object> originalresolved = gson.fromJson(payload, Map.class);
		String[] optionsArray = new String[0];
		AppendCSourceRequest request = new AppendCSourceRequest(AppConstants.INTERNAL_NULL_KEY,
				"urn:ngsi-ld:ContextSourceRegistration:csr3", originalresolved, resolved, optionsArray);
		Mockito.when(csourceDAO.getEntity(any(), any())).thenReturn(Uni.createFrom().item(originalresolved));
		try {
			csourceService
					.updateEntry(AppConstants.INTERNAL_NULL_KEY, "urn:ngsi-ld:ContextSourceRegistration:csr3", resolved)
					.await().indefinitely();
		} catch (Exception e) {
			Assertions.assertEquals("not supported in registry", e.getMessage());
		}

	}

	/**
	 * this method is use to Validate delete Entry Method
	 */
	@Test
	public void deleteEntryTest() {
		try {
			ArrayListMultimap<String, String> csourceIds = ArrayListMultimap.create();
			csourceIds.put(AppConstants.INTERNAL_NULL_KEY, "urn:ngsi-ld:ContextSourceRegistration:csr3");
			Gson gson = new Gson();
			Map<String, Object> resolved = gson.fromJson(updatePayload, Map.class);
			Map<String, Object> originalresolved = gson.fromJson(payload, Map.class);
			multimaparr.put("content-type", "application/json");
			Mockito.when(csourceDAO.getEntity(any(), any())).thenReturn(Uni.createFrom().item(originalresolved));
			boolean deleteresult = csourceService
					.deleteEntry(AppConstants.INTERNAL_NULL_KEY, "urn:ngsi-ld:ContextSourceRegistration:csr3").await()
					.indefinitely();
			Assertions.assertEquals(true, deleteresult);
			Mockito.verify(csourceService).deleteEntry(any(), any());
		} catch (Exception ex) {
			Assertions.fail();

		}
	}

	/**
	 * this method is use to Validate delete Entry Method Id is NUll
	 */
	@Test
	public void deleteEntryBadRequestTest() {
		ArrayListMultimap<String, String> csourceIds = ArrayListMultimap.create();
		csourceIds.put(AppConstants.INTERNAL_NULL_KEY, "urn:ngsi-ld:ContextSourceRegistration:csr3");
		Gson gson = new Gson();
		Map<String, Object> resolved = gson.fromJson(updatePayload, Map.class);
		Map<String, Object> originalresolved = gson.fromJson(payload, Map.class);
		multimaparr.put("content-type", "application/json");
		Mockito.when(csourceDAO.getEntity(any(), any())).thenReturn(Uni.createFrom().item(originalresolved));
		try {
			csourceService.deleteEntry(AppConstants.INTERNAL_NULL_KEY, null).await().indefinitely();
		} catch (Exception e) {
			Assertions.assertEquals("empty entity id not allowed", e.getMessage());
			Mockito.verify(csourceService).deleteEntry(any(), any());
		}
	}

	/**
	 * this method is use to Validate handleEntityDelete method with Null id
	 */
	@Test
	public void handleEntityDeleteIdNUllTest() {
		try {
			ArrayListMultimap<String, String> csourceIds = ArrayListMultimap.create();
			csourceIds.put(AppConstants.INTERNAL_NULL_KEY, "urn:ngsi-ld:ContextSourceRegistration:csr3");
			Gson gson = new Gson();
			Map<String, Object> resolved = gson.fromJson(updatePayload, Map.class);
			Map<String, Object> originalresolved = gson.fromJson(payload, Map.class);
			BaseRequest br = new BaseRequest();
			br.setFinalPayload(originalresolved);
			br.setTenant(AppConstants.INTERNAL_NULL_KEY);
			multimaparr.put("content-type", "application/json");
			Mockito.when(csourceDAO.getEntity(any(), any())).thenReturn(Uni.createFrom().item(originalresolved));
			csourceService.handleEntityDelete(br);
			Mockito.verify(csourceService).handleEntityDelete(any());
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * this method is use to Validate csourceTimerTask
	 */
	@Test
	public void csourceTimerTaskTest() throws Exception {

		ArrayListMultimap<String, String> csourceIds = ArrayListMultimap.create();
		csourceIds.put(AppConstants.INTERNAL_NULL_KEY, "urn:ngsi-ld:ContextSourceRegistration:csr3");
		HashMap<String, List<Map<String, Object>>> result = new HashMap<String, List<Map<String, Object>>>();
		List<Map<String, Object>> entities = new ArrayList<Map<String, Object>>();
		result.put(AppConstants.INTERNAL_NULL_KEY, entities);
		multimaparr.put("content-type", "application/json");
		Gson gson = new Gson();
		Map<String, Object> originalresolved = gson.fromJson(payload, Map.class);
		originalresolved.put(NGSIConstants.NGSI_LD_EXPIRES, "https://uri.etsi.org/ngsi-ld/expiresAt");
		originalresolved.put(NGSIConstants.JSON_LD_ID, "@id");
		Mockito.when(csourceDAO.getEntity(any(), any())).thenReturn(Uni.createFrom().item(originalresolved));
		try {
			csourceService.csourceTimerTask(AppConstants.INTERNAL_NULL_KEY, originalresolved);
		} catch (Exception e) {
			e.getMessage();
		}

	}

}
