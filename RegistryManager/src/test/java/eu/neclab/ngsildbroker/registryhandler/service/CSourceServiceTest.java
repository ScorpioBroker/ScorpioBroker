package eu.neclab.ngsildbroker.registryhandler.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.CompletionException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.jsonldjava.core.Context;

import eu.neclab.ngsildbroker.commons.datatypes.requests.AppendCSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CreateCSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteCSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.results.NGSILDOperationResult;
import eu.neclab.ngsildbroker.commons.datatypes.results.QueryResult;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import eu.neclab.ngsildbroker.registryhandler.controller.CustomProfile;
import eu.neclab.ngsildbroker.registryhandler.repository.CSourceDAO;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowIterator;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.pgclient.PgException;

@QuarkusTest
@TestProfile(CustomProfile.class)
public class CSourceServiceTest {

	@InjectMocks
	CSourceService CSourceService;

	@Mock
	CSourceDAO cSourceInfoDAO;

	@Mock
	MutinyEmitter<BaseRequest> kafkaSenderInterface;

	@Mock
	MicroServiceUtils microServiceUtils;

	@Mock
	Vertx vertx;

	@Mock
	WebClient webClient;

	@Mock
	Context context;

	String jsonLdObject;

	String csorceRegistrationId = "urn:ngsi-ld:ContextSourceRegistration:1";
	Map<String, Object> resolved = null;
	String tenant = "test";

	@BeforeEach
	public void setUp() throws Exception {

		MockitoAnnotations.openMocks(this);

		jsonLdObject = "{\"https://uri.etsi.org/ngsi-ld/endpoint\":[{\"@value\":\"http://my.csource.org:1234\"}],\"@id\""
				+ ":\"urn:ngsi-ld:ContextSourceRegistration:1\",\"https://uri.etsi.org/ngsi-ld/information\""
				+ ":[{\"https://uri.etsi.org/ngsi-ld/entities\":[{\"@type\":[\"https://uri.etsi.org/ngsi-ld/default-context/Room\"]}]}]"
				+ ",\"https://uri.etsi.org/ngsi-ld/location\":[{\"https://purl.org/geojson/vocab#coordinates\":[{\"@list\":"
				+ "[{\"@list\":[{\"@list\":[{\"@value\":8.686752319335938},{\"@value\":49.359122687528746}]},{\"@list\":[{\"@value\":8.742027282714844},{\"@value\""
				+ ":49.3642654834877}]},{\"@list\":[{\"@value\":8.767433166503904},{\"@value\":49.398462568451485}]},{\"@list\":[{\"@value\""
				+ ":8.768119812011719},{\"@value\":49.42750021620163}]},{\"@list\":[{\"@value\":8.74305725097656},{\"@value\":49.44781634951542}]},{\"@list\""
				+ ":[{\"@value\":8.669242858886719},{\"@value\":49.43754770762113}]},{\"@list\":[{\"@value\":8.63525390625},{\"@value\""
				+ ":49.41968407776289}]},{\"@list\":[{\"@value\":8.637657165527344},{\"@value\":49.3995797187007}]},{\"@list\":[{\"@value\""
				+ ":8.663749694824219},{\"@value\":49.36851347448498}]},{\"@list\":[{\"@value\":8.686752319335938},{\"@value\":49.359122687528746}]}]}],\"@type\":"
				+ "[\"https://purl.org/geojson/vocab#Polygon\"]}]}],\"@type\":[\"https://uri.etsi.org/ngsi-ld/ContextSourceRegistration\"]}"
				+ "[\"https://uri.etsi.org/ngsi-ld/Relationship\"]}]}";

		ObjectMapper objectMapper = new ObjectMapper();
		resolved = objectMapper.readValue(jsonLdObject, Map.class);
	}

	@Test
	public void createRegistrationTest() {

		RowSet<Row> rowSetMock = mock(RowSet.class);
		Uni<RowSet<Row>> uniRowsetMock = Uni.createFrom().item(rowSetMock);
		when(cSourceInfoDAO.createRegistration(any())).thenReturn(uniRowsetMock);

		Uni<Void> kafkaResponse = Uni.createFrom().nullItem();
		when(kafkaSenderInterface.send(any(CreateCSourceRequest.class))).thenReturn(kafkaResponse);

		Uni<NGSILDOperationResult> resultUni = CSourceService.createRegistration(tenant, resolved);
		NGSILDOperationResult result = resultUni.await().indefinitely();

		assertEquals(csorceRegistrationId, result.getEntityId());
		assertEquals(1, result.getSuccesses().size());
		assertEquals(0, result.getFailures().size());
		verify(cSourceInfoDAO, times(1)).createRegistration(any());

	}

	@Test
	public void createRegistrationExistTest() {

		Uni<Void> kafkaResponse = Uni.createFrom().nullItem();
		when(kafkaSenderInterface.send(any(CreateCSourceRequest.class))).thenReturn(kafkaResponse);

		PgException sqlException = new PgException("duplicate key value violates unique constraint", "", "23505", "");

		when(cSourceInfoDAO.createRegistration(any())).thenReturn(Uni.createFrom().failure(sqlException));

		Uni<NGSILDOperationResult> resultUni = CSourceService.createRegistration(tenant, resolved);

		Throwable throwable = assertThrows(CompletionException.class, () -> resultUni.await().indefinitely());
		ResponseException responseException = (ResponseException) throwable.getCause();

		assertEquals(409, responseException.getErrorCode());
		assertEquals("Registration already exists", responseException.getDetail());
		verify(cSourceInfoDAO, times(1)).createRegistration(any());

	}

	@Test
	public void createRegistrationErrorTest() {

		Uni<Void> kafkaResponse = Uni.createFrom().nullItem();
		when(kafkaSenderInterface.send(any(CreateCSourceRequest.class))).thenReturn(kafkaResponse);

		when(cSourceInfoDAO.createRegistration(any()))
				.thenReturn(Uni.createFrom().failure(new RuntimeException("Something went wrong")));

		Uni<NGSILDOperationResult> resultUni = CSourceService.createRegistration(tenant, resolved);

		Throwable throwable = assertThrows(CompletionException.class, () -> resultUni.await().indefinitely());
		ResponseException responseException = (ResponseException) throwable.getCause();

		assertEquals(500, responseException.getErrorCode());
		assertEquals("Something went wrong", responseException.getDetail());
		verify(cSourceInfoDAO, times(1)).createRegistration(any());

	}

	@Test
	public void updateRegistrationTest() {

		Uni<Void> kafkaResponse = Uni.createFrom().nullItem();
		when(kafkaSenderInterface.send(any(AppendCSourceRequest.class))).thenReturn(kafkaResponse);

		RowSet<Row> rowSetMock = mock(RowSet.class);
		when(rowSetMock.rowCount()).thenReturn(1);
		Uni<RowSet<Row>> uniRowsetMock = Uni.createFrom().item(rowSetMock);
		when(cSourceInfoDAO.updateRegistration(any())).thenReturn(uniRowsetMock);

		Uni<NGSILDOperationResult> resultUni = CSourceService.updateRegistration(tenant, csorceRegistrationId,
				resolved);
		NGSILDOperationResult result = resultUni.await().indefinitely();

		assertEquals(csorceRegistrationId, result.getEntityId());
		assertEquals(1, result.getSuccesses().size());
		assertEquals(0, result.getFailures().size());
		verify(cSourceInfoDAO, times(1)).updateRegistration(any());

	}

	@Test
	public void updateRegistrationNotFoundTest() {

		Uni<Void> kafkaResponse = Uni.createFrom().nullItem();
		when(kafkaSenderInterface.send(any(AppendCSourceRequest.class))).thenReturn(kafkaResponse);

		RowSet<Row> rowSetMock = mock(RowSet.class);
		when(rowSetMock.rowCount()).thenReturn(0);
		Uni<RowSet<Row>> uniRowsetMock = Uni.createFrom().item(rowSetMock);
		when(cSourceInfoDAO.updateRegistration(any())).thenReturn(uniRowsetMock);

		Uni<NGSILDOperationResult> resultUni = CSourceService.updateRegistration(tenant, csorceRegistrationId,
				resolved);

		Throwable throwable = assertThrows(CompletionException.class, () -> resultUni.await().indefinitely());
		ResponseException responseException = (ResponseException) throwable.getCause();

		assertEquals(404, responseException.getErrorCode());
		assertEquals("Registration not found", responseException.getDetail());
		verify(cSourceInfoDAO, times(1)).updateRegistration(any());

	}

	@Test
	public void updateRegistrationErrorTest() {

		Uni<Void> kafkaResponse = Uni.createFrom().nullItem();
		when(kafkaSenderInterface.send(any(AppendCSourceRequest.class))).thenReturn(kafkaResponse);

		when(cSourceInfoDAO.updateRegistration(any()))
				.thenReturn(Uni.createFrom().failure(new RuntimeException("Something went wrong")));

		Uni<NGSILDOperationResult> resultUni = CSourceService.updateRegistration(tenant, csorceRegistrationId,
				resolved);

		Throwable throwable = assertThrows(CompletionException.class, () -> resultUni.await().indefinitely());
		ResponseException responseException = (ResponseException) throwable.getCause();

		assertEquals(500, responseException.getErrorCode());
		assertEquals("Something went wrong", responseException.getDetail());
		verify(cSourceInfoDAO, times(1)).updateRegistration(any());

	}

	@Test
	public void retrieveRegistrationTest() {

		Row rowMock = mock(Row.class);
		RowSet<Row> rowSetMock = mock(RowSet.class);
		RowIterator<Row> rowIteratorMock = mock(RowIterator.class);
		when(rowSetMock.size()).thenReturn(1);
		when(rowSetMock.iterator()).thenReturn(rowIteratorMock);
		when(rowIteratorMock.next()).thenReturn(rowMock);
		JsonObject jsonObject = new JsonObject();
		jsonObject.put("@id", "urn:ngsi-ld:ContextSourceRegistration:1");
		when(rowMock.getJsonObject(anyInt())).thenReturn(jsonObject);

		Uni<RowSet<Row>> uniRowsetMock = Uni.createFrom().item(rowSetMock);
		when(cSourceInfoDAO.getRegistrationById(any(), any())).thenReturn(uniRowsetMock);

		Uni<Map<String, Object>> resultUni = CSourceService.retrieveRegistration(tenant, csorceRegistrationId);
		Map<String, Object> result = resultUni.await().indefinitely();

		assertEquals("urn:ngsi-ld:ContextSourceRegistration:1", result.get("@id"));
		verify(cSourceInfoDAO, times(1)).getRegistrationById(any(), any());

	}

	@Test
	public void retrieveRegistrationNotFoundTest() {

		RowSet<Row> rowSetMock = mock(RowSet.class);
		when(rowSetMock.size()).thenReturn(0);
		Uni<RowSet<Row>> uniRowsetMock = Uni.createFrom().item(rowSetMock);
		when(cSourceInfoDAO.getRegistrationById(any(), any())).thenReturn(uniRowsetMock);

		Uni<Map<String, Object>> resultUni = CSourceService.retrieveRegistration(tenant, csorceRegistrationId);

		Throwable throwable = assertThrows(CompletionException.class, () -> resultUni.await().indefinitely());
		ResponseException responseException = (ResponseException) throwable.getCause();

		assertEquals(404, responseException.getErrorCode());
		assertEquals(csorceRegistrationId + "was not found", responseException.getDetail());
		verify(cSourceInfoDAO, times(1)).getRegistrationById(any(), any());

	}

	@Test
	public void retrieveRegistrationDataNotFoundTest() {

		Row rowMock = mock(Row.class);
		RowSet<Row> rowSetMock = mock(RowSet.class);
		RowIterator<Row> rowIteratorMock = mock(RowIterator.class);
		when(rowSetMock.size()).thenReturn(1);
		when(rowSetMock.iterator()).thenReturn(rowIteratorMock);
		when(rowIteratorMock.next()).thenReturn(rowMock);
		when(rowMock.getJsonObject(anyInt())).thenReturn(null);

		Uni<RowSet<Row>> uniRowsetMock = Uni.createFrom().item(rowSetMock);
		when(cSourceInfoDAO.getRegistrationById(any(), any())).thenReturn(uniRowsetMock);

		Uni<Map<String, Object>> resultUni = CSourceService.retrieveRegistration(tenant, csorceRegistrationId);

		Throwable throwable = assertThrows(CompletionException.class, () -> resultUni.await().indefinitely());
		ResponseException responseException = (ResponseException) throwable.getCause();

		assertEquals(404, responseException.getErrorCode());
		assertEquals(csorceRegistrationId + "was not found", responseException.getDetail());
		verify(cSourceInfoDAO, times(1)).getRegistrationById(any(), any());
	}

	@Test
	public void deleteRegistrationTest() {

		Uni<Void> kafkaResponse = Uni.createFrom().nullItem();
		when(kafkaSenderInterface.send(any(DeleteCSourceRequest.class))).thenReturn(kafkaResponse);

		Row rowMock = mock(Row.class);
		RowSet<Row> rowSetMock = mock(RowSet.class);
		RowIterator<Row> rowIteratorMock = mock(RowIterator.class);
		when(rowSetMock.rowCount()).thenReturn(1);
		when(rowSetMock.iterator()).thenReturn(rowIteratorMock);
		when(rowIteratorMock.next()).thenReturn(rowMock);
		JsonObject jsonObject = new JsonObject();
		jsonObject.put("@id", "urn:ngsi-ld:ContextSourceRegistration:1");
		when(rowMock.getJsonObject(anyInt())).thenReturn(jsonObject);

		Uni<RowSet<Row>> uniRowsetMock = Uni.createFrom().item(rowSetMock);
		when(cSourceInfoDAO.deleteRegistration(any())).thenReturn(uniRowsetMock);

		Uni<NGSILDOperationResult> resultUni = CSourceService.deleteRegistration(tenant, csorceRegistrationId);
		NGSILDOperationResult result = resultUni.await().indefinitely();

		assertEquals(csorceRegistrationId, result.getEntityId());
		assertEquals(1, result.getSuccesses().size());
		assertEquals(0, result.getFailures().size());
		verify(cSourceInfoDAO, times(1)).deleteRegistration(any());

	}

	@Test
	public void deleteRegistrationNotFoundTest() {

		Uni<Void> kafkaResponse = Uni.createFrom().nullItem();
		when(kafkaSenderInterface.send(any(DeleteCSourceRequest.class))).thenReturn(kafkaResponse);

		RowSet<Row> rowSetMock = mock(RowSet.class);
		when(rowSetMock.rowCount()).thenReturn(0);

		Uni<RowSet<Row>> uniRowsetMock = Uni.createFrom().item(rowSetMock);
		when(cSourceInfoDAO.deleteRegistration(any())).thenReturn(uniRowsetMock);

		Uni<NGSILDOperationResult> resultUni = CSourceService.deleteRegistration(tenant, csorceRegistrationId);

		Throwable throwable = assertThrows(CompletionException.class, () -> resultUni.await().indefinitely());
		ResponseException responseException = (ResponseException) throwable.getCause();

		assertEquals(404, responseException.getErrorCode());
		assertEquals("Registration not found", responseException.getDetail());
		verify(cSourceInfoDAO, times(1)).deleteRegistration(any());

	}

	@Test
	public void deleteRegistrationErrorTest() {

		Uni<Void> kafkaResponse = Uni.createFrom().nullItem();
		when(kafkaSenderInterface.send(any(DeleteCSourceRequest.class))).thenReturn(kafkaResponse);

		when(cSourceInfoDAO.deleteRegistration(any()))
				.thenReturn(Uni.createFrom().failure(new RuntimeException("Something went wrong")));

		Uni<NGSILDOperationResult> resultUni = CSourceService.deleteRegistration(tenant, csorceRegistrationId);

		Throwable throwable = assertThrows(CompletionException.class, () -> resultUni.await().indefinitely());
		ResponseException responseException = (ResponseException) throwable.getCause();

		assertEquals(500, responseException.getErrorCode());
		assertEquals("Something went wrong", responseException.getDetail());
		verify(cSourceInfoDAO, times(1)).deleteRegistration(any());

	}

	@Test
	public void queryRegistrationsTest() {

		Row rowMock = mock(Row.class);
		RowSet<Row> rowSetMock = mock(RowSet.class);
		RowIterator<Row> rowIteratorMock = mock(RowIterator.class);
		when(rowSetMock.iterator()).thenReturn(rowIteratorMock);
		when(rowIteratorMock.next()).thenReturn(rowMock);
		when(rowMock.getLong(0)).thenReturn(1L);

		Uni<RowSet<Row>> uniRowsetMock = Uni.createFrom().item(rowSetMock);

		when(cSourceInfoDAO.query(any(), any(), any(), any(), any(), any(), any(), any(), anyInt(), anyInt(),
				anyBoolean())).thenReturn(uniRowsetMock);

		Uni<QueryResult> resultUni = CSourceService.queryRegistrations(tenant, new HashSet<>(), null, "", null, null,
				null, null, 0, 0, true);

		QueryResult result = resultUni.await().indefinitely();

		assertEquals(1L, result.getCount());
		verify(cSourceInfoDAO, times(1)).query(any(), any(), any(), any(), any(), any(), any(), any(), anyInt(),
				anyInt(), anyBoolean());

	}

	@Test
	public void queryRegistrationsLimitTest() {

		Row rowMock = mock(Row.class);
		RowSet<Row> rowSetMock = mock(RowSet.class);
		RowIterator<Row> rowIteratorMock = mock(RowIterator.class);
		when(rowSetMock.size()).thenReturn(1);
		when(rowSetMock.iterator()).thenReturn(rowIteratorMock);
		when(rowIteratorMock.next()).thenReturn(rowMock);
		when(rowIteratorMock.hasNext()).thenReturn(true).thenReturn(false);
		JsonObject jsonObject = new JsonObject();
		jsonObject.put("@id", "urn:ngsi-ld:ContextSourceRegistration:1");
		when(rowMock.getJsonObject(anyInt())).thenReturn(jsonObject);
		when(rowMock.getLong(0)).thenReturn(0L);

		Uni<RowSet<Row>> uniRowsetMock = Uni.createFrom().item(rowSetMock);

		when(cSourceInfoDAO.query(any(), any(), any(), any(), any(), any(), any(), any(), anyInt(), anyInt(),
				anyBoolean())).thenReturn(uniRowsetMock);

		Uni<QueryResult> resultUni = CSourceService.queryRegistrations(tenant, new HashSet<>(), null, "", null, null,
				null, null, 1, 0, false);

		QueryResult result = resultUni.await().indefinitely();

		assertEquals(0L, result.getCount());
		verify(cSourceInfoDAO, times(1)).query(any(), any(), any(), any(), any(), any(), any(), any(), anyInt(),
				anyInt(), anyBoolean());

	}

	@Test
	public void checkInternalAndSendUpdateIfNeededTest() {

		Uni<Boolean> uniMock = Uni.createFrom().item(false);
		when(cSourceInfoDAO.isTenantPresent(anyString())).thenReturn(uniMock);

		Map<String, String> details1 = new HashMap<>();
		details1.put("url", "https://example.com/federation1");
		details1.put("sourcetenant", "sourceTenant1");
		details1.put("targettenant", "targetTenant1");
		details1.put("regtype", "regType1");

		Map<String, String> details2 = new HashMap<>();
		details2.put("url", "https://example.com/federation2");
		details2.put("sourcetenant", "sourceTenant2");
		details2.put("targettenant", "targetTenant2");
		details2.put("regtype", "regType2");

		Map<String, Map<String, String>> fedMap = new HashMap<>();
		fedMap.put("federation1", details1);
		fedMap.put("federation2", details2);

		CSourceService.fedMap = fedMap;

		Uni<Void> voidResult = CSourceService.checkInternalAndSendUpdateIfNeeded();
		voidResult.await().indefinitely();

		verify(cSourceInfoDAO, times(2)).isTenantPresent(anyString());
	}

}
