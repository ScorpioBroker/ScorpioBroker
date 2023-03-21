package eu.neclab.ngsildbroker.entityhandler.services;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.jsonldjava.core.Context;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.RegistrationEntry;
import eu.neclab.ngsildbroker.commons.datatypes.requests.AppendEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BatchRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CreateEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteAttributeRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.UpdateEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.results.NGSILDOperationResult;
import eu.neclab.ngsildbroker.entityhandler.controller.CustomProfile;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.ext.web.client.WebClient;

@QuarkusTest
@TestMethodOrder(OrderAnnotation.class)
@TestProfile(CustomProfile.class)
public class EntityServiceTest {

	@InjectMocks
	private EntityService entityService;

	@Mock
	EntityInfoDAO entityDAO;

	@Mock
	MutinyEmitter<BaseRequest> entityEmitter;

	@Mock
	MutinyEmitter<BatchRequest> batchEmitter;

	@Mock
	Vertx vertx;

	@Mock
	WebClient webClient;

	@Mock
	Context context;

	String jsonLdObject;
	String tenant = "tenant";
	String entityId = "urn:test:testentity2";
	String entityPayload = null;
	Map<String, Object> resolved = null;

	@BeforeEach
	public void setUp() throws Exception {

		MockitoAnnotations.openMocks(this);
		entityService.webClient = webClient;
		entityService.vertx = vertx;
		entityService.entityEmitter = entityEmitter;
		entityService.batchEmitter = batchEmitter;

		Table<String, String, RegistrationEntry> registriesMap = HashBasedTable.create();
		Uni<Table<String, String, RegistrationEntry>> uniRegistriesMap = Uni.createFrom().item(registriesMap);
		when(entityDAO.getAllRegistries()).thenReturn(uniRegistriesMap);

		jsonLdObject = "{\"https://uri.etsi.org/ngsi-ld/default-context/complexproperty\":[{\"@type\":"
				+ "[\"https://uri.etsi.org/ngsi-ld/Property\"],\"https://uri.etsi.org/ngsi-ld/hasValue\":"
				+ "[{\"https://uri.etsi.org/ngsi-ld/default-context/some\":[{\"https://uri.etsi.org/ngsi-ld/default-context/more\":"
				+ "[{\"@value\":\"values\"}]}]}]}],\"https://uri.etsi.org/ngsi-ld/default-context/floatproperty\":[{\"@type\":"
				+ "[\"https://uri.etsi.org/ngsi-ld/Property\"],\"https://uri.etsi.org/ngsi-ld/hasValue\":"
				+ "[{\"@value\":123.456}]}],\"@id\":\"urn:test:testentity2\",\"https://uri.etsi.org/ngsi-ld/default-context/intproperty\":"
				+ "[{\"@type\":[\"https://uri.etsi.org/ngsi-ld/Property\"],\"https://uri.etsi.org/ngsi-ld/hasValue\":"
				+ "[{\"@value\":123}]}],\"https://uri.etsi.org/ngsi-ld/default-context/stringproperty\":[{\"@type\":"
				+ "[\"https://uri.etsi.org/ngsi-ld/Property\"],\"https://uri.etsi.org/ngsi-ld/hasValue\":"
				+ "[{\"@value\":\"teststring\"}]}],\"https://uri.etsi.org/ngsi-ld/default-context/testrelationship\":"
				+ "[{\"https://uri.etsi.org/ngsi-ld/hasObject\":[{\"@id\":\"urn:testref1\"}],\"@type\":"
				+ "[\"https://uri.etsi.org/ngsi-ld/Relationship\"]}]}";

		ObjectMapper objectMapper = new ObjectMapper();
		resolved = objectMapper.readValue(jsonLdObject, Map.class);
	}

	@AfterEach
	public void tearDown() {
		jsonLdObject = null;
	}

	@Test
	@Order(1)
	public void createEntryTest() throws Exception {

		CreateEntityRequest request = new CreateEntityRequest(tenant, resolved, null);

		Uni<Void> createEntityRes = Uni.createFrom().voidItem();
		when(entityDAO.createEntity(any())).thenReturn(createEntityRes);

		Uni<Void> emitterResponse = Uni.createFrom().nullItem();
		when(entityEmitter.send(request)).thenReturn(emitterResponse);

		when(context.compactIri(anyString())).thenReturn("");

		NGSILDOperationResult operationResult = entityService.createEntity(tenant, resolved, context).await()
				.indefinitely();

		assertEquals(entityId, operationResult.getEntityId());
		assertEquals(1, operationResult.getSuccesses().size());
		assertEquals(0, operationResult.getFailures().size());
		verify(entityDAO, times(1)).createEntity(any());
	}

	@Test
	@Order(2)
	public void updateEntryTest() {

		UpdateEntityRequest request = new UpdateEntityRequest(tenant, entityId, resolved, null, null);

		Uni<Void> updateEntityRes = Uni.createFrom().voidItem();
		when(entityDAO.updateEntity(any())).thenReturn(updateEntityRes);

		Uni<Void> emitterResponse = Uni.createFrom().nullItem();
		when(entityEmitter.send(request)).thenReturn(emitterResponse);

		NGSILDOperationResult operationResult = entityService.updateEntity(tenant, entityId, resolved, context).await()
				.indefinitely();

		assertEquals(entityId, operationResult.getEntityId());
		assertEquals(1, operationResult.getSuccesses().size());
		assertEquals(0, operationResult.getFailures().size());
		verify(entityDAO, times(1)).updateEntity(any());
	}

	@Test
	@Order(3)
	public void appendEntryTest() {

		Uni<Set<String>> appendEntityRes = Uni.createFrom().item(new HashSet<>(0));
		when(entityDAO.appendToEntity2(any(), anyBoolean())).thenReturn(appendEntityRes);

		Uni<Void> emitterResponse = Uni.createFrom().nullItem();
		when(entityEmitter.send(any(AppendEntityRequest.class))).thenReturn(emitterResponse);

		NGSILDOperationResult operationResult = entityService.appendToEntity(tenant, entityId, resolved, false, context)
				.await().indefinitely();

		assertEquals(entityId, operationResult.getEntityId());
		assertEquals(1, operationResult.getSuccesses().size());
		assertEquals(0, operationResult.getFailures().size());
		verify(entityDAO, times(1)).appendToEntity2(any(), anyBoolean());
		verify(entityEmitter, times(1)).send(any(AppendEntityRequest.class));
	}

	@Test
	@Order(4)
	public void partialUpdateAttributeTest() {

		Uni<Void> partialUpdateAttributeRes = Uni.createFrom().nullItem();
		when(entityDAO.partialUpdateAttribute(any())).thenReturn(partialUpdateAttributeRes);

		NGSILDOperationResult operationResult = entityService
				.partialUpdateAttribute(tenant, entityId, "brandName", resolved, context).await().indefinitely();

		assertEquals(entityId, operationResult.getEntityId());
		assertEquals(0, operationResult.getFailures().size());
		verify(entityDAO, times(1)).partialUpdateAttribute(any());
	}

	@Test
	@Order(5)
	public void deleteAttributeTest() throws Exception {
		try {

			Uni<Map<String, Object>> deleteAttributeRes = Uni.createFrom().item(new HashMap());
			when(entityDAO.deleteAttribute(any())).thenReturn(deleteAttributeRes);

			Uni<Void> emitterResponse = Uni.createFrom().nullItem();
			when(entityEmitter.send(any(DeleteAttributeRequest.class))).thenReturn(emitterResponse);

			NGSILDOperationResult operationResult = entityService
					.deleteAttribute(tenant, entityId, "brandName", "datasetId", false, context).await().indefinitely();

			verify(entityDAO, times(1)).deleteAttribute(any());
			verify(entityEmitter, times(1)).send(any(DeleteAttributeRequest.class));

			assertEquals(entityId, operationResult.getEntityId());

		} catch (Exception e) {
			Assertions.fail();
		}
	}

	@Test
	@Order(6)
	public void deleteEntityTest() throws Exception {
		try {

			Uni<Map<String, Object>> deleteEntityRes = Uni.createFrom().item(new HashMap());
			when(entityDAO.deleteEntity(any())).thenReturn(deleteEntityRes);

			Uni<Void> emitterResponse = Uni.createFrom().nullItem();
			when(entityEmitter.send(any(DeleteEntityRequest.class))).thenReturn(emitterResponse);

			NGSILDOperationResult operationResult = entityService.deleteEntity(tenant, entityId, context).await()
					.indefinitely();

			verify(entityDAO, times(1)).deleteEntity(any());
			verify(entityEmitter, times(1)).send(any(DeleteEntityRequest.class));

			assertEquals(entityId, operationResult.getEntityId());

		} catch (Exception e) {
			Assertions.fail();
		}

	}

	@Test
	@Order(7)
	public void createBatchTest() throws Exception {

		List<Map<String, Object>> expandedEntities = new ArrayList<>();
		expandedEntities.add(resolved);

		List<Context> contextList = new ArrayList<>();
		contextList.add(context);

		Map<String, Object> entityBatchDaoRes = new HashMap<>();
		List<String> successes = new ArrayList<>();
		successes.add(entityId);
		List<Map<String, String>> failures = new ArrayList<>();
		entityBatchDaoRes.put("success", successes);
		entityBatchDaoRes.put("failure", failures);

		Uni<Void> emitterResponse = Uni.createFrom().nullItem();
		when(batchEmitter.send(any(BatchRequest.class))).thenReturn(emitterResponse);

		Uni<Map<String, Object>> createEntityRes = Uni.createFrom().item(entityBatchDaoRes);
		when(entityDAO.batchCreateEntity(any())).thenReturn(createEntityRes);

		List<NGSILDOperationResult> operationResultList = entityService
				.createBatch(tenant, expandedEntities, contextList, true).await().indefinitely();

		assertEquals(1, operationResultList.size());
		verify(entityDAO, times(1)).batchCreateEntity(any());
		verify(batchEmitter, times(1)).send(any(BatchRequest.class));
	}

	@Test
	@Order(9)
	public void createBatchFailureTest() throws Exception {

		List<Map<String, Object>> expandedEntities = new ArrayList<>();
		expandedEntities.add(resolved);

		List<Context> contextList = new ArrayList<>();
		contextList.add(context);

		Map<String, Object> entityBatchDaoRes = new HashMap<>();
		List<String> successes = new ArrayList<>();
		List<Map<String, String>> failures = new ArrayList<>();

		Map<String, String> fail = new HashMap<>();
		fail.put(entityId, AppConstants.SQL_ALREADY_EXISTS);
		fail.put("urn:test:testentity", "Internal custom error");
		failures.add(fail);

		entityBatchDaoRes.put("success", successes);
		entityBatchDaoRes.put("failure", failures);

		Uni<Void> emitterResponse = Uni.createFrom().nullItem();
		when(batchEmitter.send(any(BatchRequest.class))).thenReturn(emitterResponse);

		Uni<Map<String, Object>> createEntityRes = Uni.createFrom().item(entityBatchDaoRes);
		when(entityDAO.batchCreateEntity(any())).thenReturn(createEntityRes);

		List<NGSILDOperationResult> operationResultList = entityService
				.createBatch(tenant, expandedEntities, contextList, true).await().indefinitely();

		assertEquals(2, operationResultList.size());
		verify(entityDAO, times(1)).batchCreateEntity(any());
		verify(batchEmitter, times(1)).send(any(BatchRequest.class));
	}

	@Test
	@Order(10)
	public void appendBatchTest() throws Exception {

		List<Map<String, Object>> expandedEntities = new ArrayList<>();
		expandedEntities.add(resolved);

		List<Context> contextList = new ArrayList<>();
		contextList.add(context);

		Map<String, Object> entityBatchDaoRes = new HashMap<>();
		List<String> successes = new ArrayList<>();
		successes.add(entityId);
		List<Map<String, String>> failures = new ArrayList<>();
		entityBatchDaoRes.put("success", successes);
		entityBatchDaoRes.put("failure", failures);

		Uni<Void> emitterResponse = Uni.createFrom().nullItem();
		when(batchEmitter.send(any(BatchRequest.class))).thenReturn(emitterResponse);

		Uni<Map<String, Object>> createEntityRes = Uni.createFrom().item(entityBatchDaoRes);
		when(entityDAO.batchAppendEntity(any())).thenReturn(createEntityRes);

		List<NGSILDOperationResult> operationResultList = entityService
				.appendBatch(tenant, expandedEntities, contextList, true).await().indefinitely();

		assertEquals(1, operationResultList.size());
		verify(entityDAO, times(1)).batchAppendEntity(any());
		verify(batchEmitter, times(1)).send(any(BatchRequest.class));
	}

	@Test
	@Order(11)
	public void appendBatchFailureTest() throws Exception {

		List<Map<String, Object>> expandedEntities = new ArrayList<>();
		expandedEntities.add(resolved);

		List<Context> contextList = new ArrayList<>();
		contextList.add(context);

		Map<String, Object> entityBatchDaoRes = new HashMap<>();
		List<String> successes = new ArrayList<>();
		List<Map<String, String>> failures = new ArrayList<>();

		Map<String, String> fail = new HashMap<>();
		fail.put(entityId, AppConstants.SQL_NOT_FOUND);
		fail.put("urn:test:testentity", "Internal custom error");
		failures.add(fail);

		entityBatchDaoRes.put("success", successes);
		entityBatchDaoRes.put("failure", failures);

		Uni<Void> emitterResponse = Uni.createFrom().nullItem();
		when(batchEmitter.send(any(BatchRequest.class))).thenReturn(emitterResponse);

		Uni<Map<String, Object>> createEntityRes = Uni.createFrom().item(entityBatchDaoRes);
		when(entityDAO.batchAppendEntity(any())).thenReturn(createEntityRes);

		List<NGSILDOperationResult> operationResultList = entityService
				.appendBatch(tenant, expandedEntities, contextList, true).await().indefinitely();

		assertEquals(2, operationResultList.size());
		verify(entityDAO, times(1)).batchAppendEntity(any());
		verify(batchEmitter, times(1)).send(any(BatchRequest.class));
	}

	@Test
	@Order(12)
	public void upsertBatchTest() throws Exception {

		List<Map<String, Object>> expandedEntities = new ArrayList<>();
		expandedEntities.add(resolved);

		List<Context> contextList = new ArrayList<>();
		contextList.add(context);

		Map<String, Object> entityBatchDaoRes = new HashMap<>();
		List<String> successes = new ArrayList<>();
		successes.add(entityId);
		List<Map<String, String>> failures = new ArrayList<>();
		entityBatchDaoRes.put("success", successes);
		entityBatchDaoRes.put("failure", failures);

		Uni<Void> emitterResponse = Uni.createFrom().nullItem();
		when(batchEmitter.send(any(BatchRequest.class))).thenReturn(emitterResponse);

		Uni<Map<String, Object>> createEntityRes = Uni.createFrom().item(entityBatchDaoRes);
		when(entityDAO.batchUpsertEntity(any())).thenReturn(createEntityRes);

		List<NGSILDOperationResult> operationResultList = entityService
				.upsertBatch(tenant, expandedEntities, contextList, true).await().indefinitely();

		assertEquals(1, operationResultList.size());
		verify(entityDAO, times(1)).batchUpsertEntity(any());
		verify(batchEmitter, times(1)).send(any(BatchRequest.class));
	}

	@Test
	@Order(13)
	public void upsertBatchFailureTest() throws Exception {

		List<Map<String, Object>> expandedEntities = new ArrayList<>();
		expandedEntities.add(resolved);

		List<Context> contextList = new ArrayList<>();
		contextList.add(context);

		Map<String, Object> entityBatchDaoRes = new HashMap<>();
		List<String> successes = new ArrayList<>();
		List<Map<String, String>> failures = new ArrayList<>();

		Map<String, String> fail = new HashMap<>();
		fail.put(entityId, "Internal custom error2");
		fail.put("urn:test:testentity", "Internal custom error1");
		failures.add(fail);

		entityBatchDaoRes.put("success", successes);
		entityBatchDaoRes.put("failure", failures);

		Uni<Void> emitterResponse = Uni.createFrom().nullItem();
		when(batchEmitter.send(any(BatchRequest.class))).thenReturn(emitterResponse);

		Uni<Map<String, Object>> createEntityRes = Uni.createFrom().item(entityBatchDaoRes);
		when(entityDAO.batchUpsertEntity(any())).thenReturn(createEntityRes);

		List<NGSILDOperationResult> operationResultList = entityService
				.upsertBatch(tenant, expandedEntities, contextList, true).await().indefinitely();

		assertEquals(2, operationResultList.size());
		verify(entityDAO, times(1)).batchUpsertEntity(any());
		verify(batchEmitter, times(1)).send(any(BatchRequest.class));
	}

	@Test
	@Order(14)
	public void deleteBatchTest() throws Exception {

		List<String> entityIds = new ArrayList<>();
		entityIds.add(entityId);

		List<Context> contextList = new ArrayList<>();
		contextList.add(context);

		Map<String, Object> entityBatchDaoRes = new HashMap<>();
		List<String> successes = new ArrayList<>();
		successes.add(entityId);
		List<Map<String, String>> failures = new ArrayList<>();
		entityBatchDaoRes.put("success", successes);
		entityBatchDaoRes.put("failure", failures);

		Uni<Void> emitterResponse = Uni.createFrom().nullItem();
		when(batchEmitter.send(any(BatchRequest.class))).thenReturn(emitterResponse);

		Uni<Map<String, Object>> createEntityRes = Uni.createFrom().item(entityBatchDaoRes);
		when(entityDAO.batchDeleteEntity(any(), any())).thenReturn(createEntityRes);

		List<NGSILDOperationResult> operationResultList = entityService.deleteBatch(tenant, entityIds, true).await()
				.indefinitely();

		assertEquals(1, operationResultList.size());
		verify(entityDAO, times(1)).batchDeleteEntity(any(), any());
		verify(batchEmitter, times(1)).send(any(BatchRequest.class));
	}

	@Test
	@Order(15)
	public void deleteBatchFailureTest() throws Exception {

		List<String> entityIds = new ArrayList<>();
		entityIds.add(entityId);

		List<Context> contextList = new ArrayList<>();
		contextList.add(context);

		Map<String, Object> entityBatchDaoRes = new HashMap<>();
		List<String> successes = new ArrayList<>();
		List<Map<String, String>> failures = new ArrayList<>();

		Map<String, String> fail = new HashMap<>();
		fail.put(entityId, "Internal custom error");
		failures.add(fail);

		entityBatchDaoRes.put("success", successes);
		entityBatchDaoRes.put("failure", failures);

		Uni<Void> emitterResponse = Uni.createFrom().nullItem();
		when(batchEmitter.send(any(BatchRequest.class))).thenReturn(emitterResponse);

		Uni<Map<String, Object>> createEntityRes = Uni.createFrom().item(entityBatchDaoRes);
		when(entityDAO.batchDeleteEntity(any(), any())).thenReturn(createEntityRes);

		List<NGSILDOperationResult> operationResultList = entityService.deleteBatch(tenant, entityIds, true).await()
				.indefinitely();

		assertEquals(1, operationResultList.size());
		verify(entityDAO, times(1)).batchDeleteEntity(any(), any());
		verify(batchEmitter, times(1)).send(any(BatchRequest.class));
	}

}
