package eu.neclab.ngsildbroker.subscriptionmanager.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionException;

import io.vertx.core.http.impl.headers.HeadersMultiMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.jsonldjava.core.Context;

import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.DeleteSubscriptionRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.datatypes.results.NGSILDOperationResult;
import eu.neclab.ngsildbroker.commons.datatypes.results.QueryResult;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.subscriptionmanager.controller.CustomProfile;
import eu.neclab.ngsildbroker.subscriptionmanager.repository.SubscriptionInfoDAO;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowIterator;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.pgclient.PgException;

@QuarkusTest
@TestMethodOrder(OrderAnnotation.class)
@TestProfile(CustomProfile.class)
public class SubscriptionServiceTest {

	@InjectMocks
	SubscriptionService subscriptionService;

	@Mock
	SubscriptionInfoDAO subDAO;

	@Mock
	Context context;

	@Mock
	MutinyEmitter<SubscriptionRequest> internalSubEmitter;

	@Mock
	LocalEntityService localEntityService;

	@Mock
	LocalContextService localContextService;

	String subscriptionId = "urn:ngsi-ld:Subscription:1";
	String notificationId = "urn:ngsi-ld:notify:1";
	String jsonLdObject;
	Map<String, Object> resolved = null;
	String tenant = "test";
	HeadersMultiMap link = new HeadersMultiMap();

	@SuppressWarnings("unchecked")
	@BeforeEach
	public void setUp() throws Exception {
		MockitoAnnotations.openMocks(this);
		jsonLdObject = "{\"https://uri.etsi.org/ngsi-ld/entities\":[{\"@id\":\"urn:ngsi-ld:Vehicle:A135\",\"@type\":"
				+ "[\"https://uri.etsi.org/ngsi-ld/default-context/Vehicle\"]}],\"@id\":\"urn:ngsi-ld:Subscription:"
				+ "1\",\"https://uri.etsi.org/ngsi-ld/notification\":[{\"https://uri.etsi.org/ngsi-ld/endpoint\":"
				+ "[{\"https://uri.etsi.org/ngsi-ld/accept\":[{\"@value\":\"application/json\"}],\"https://uri.etsi.org/ngsi-ld/uri\":"
				+ "[{\"@value\":\"mqtt://localhost:1883/notify\"}]}],\"https://uri.etsi.org/ngsi-ld/format\":"
				+ "[{\"@value\":\"keyValues\"}]}],\"@type\":[\"https://uri.etsi.org/ngsi-ld/Subscription\"]}";
		ObjectMapper objectMapper = new ObjectMapper();
		resolved = objectMapper.readValue(jsonLdObject, Map.class);

	}

	@Test
	public void createSubscriptionTest() {

		

		when(subDAO.createSubscription(any(), any())).thenReturn(Uni.createFrom().voidItem());

		Uni<Void> uniEmiiterResponse = Uni.createFrom().nullItem();
		when(internalSubEmitter.send(any(SubscriptionRequest.class))).thenReturn(uniEmiiterResponse);

		Uni<NGSILDOperationResult> uniResult = subscriptionService.createSubscription(link,tenant, resolved, context, null);
		NGSILDOperationResult result = uniResult.await().indefinitely();

		assertEquals(subscriptionId, result.getEntityId());
		assertEquals(1, result.getSuccesses().size());
		assertEquals(0, result.getFailures().size());
		verify(subDAO, times(1)).createSubscription(any(), any());

	}

	@Test
	public void createSubscriptionExistTest() {

		PgException sqlException = new PgException("duplicate key value violates unique constraint", "", "23505", "");
		when(subDAO.createSubscription(any(), any())).thenReturn(Uni.createFrom().failure(sqlException));

		Uni<NGSILDOperationResult> uniResult = subscriptionService.createSubscription(link,tenant, resolved, context, null);

		Throwable throwable = assertThrows(CompletionException.class, () -> uniResult.await().indefinitely());
		ResponseException responseException = (ResponseException) throwable.getCause();

		assertEquals(409, responseException.getErrorCode());
		assertEquals("Subscription with id " + subscriptionId + " exists", responseException.getDetail());
		verify(subDAO, times(1)).createSubscription(any(), any());

	}

	@SuppressWarnings("unchecked")
	@Test
	public void updateSubscriptionNotFoundTest() {

		// create a mock Tuple2 object with an empty map and null context
		Tuple2<Map<String, Object>, Object> tuple2 = mock(Tuple2.class);
		Mockito.when(tuple2.size()).thenReturn(0);

		Uni<Tuple2<Map<String, Object>, Object>> uniRowsetMock = Uni.createFrom().item(tuple2);
		when(subDAO.updateSubscription(any(), any())).thenReturn(uniRowsetMock);

		Uni<Void> uniEmiiterResponse = Uni.createFrom().nullItem();
		when(internalSubEmitter.send(any(SubscriptionRequest.class))).thenReturn(uniEmiiterResponse);

		Uni<NGSILDOperationResult> uniResult = subscriptionService.updateSubscription(tenant, subscriptionId, resolved,
				context);

		Throwable throwable = assertThrows(CompletionException.class, () -> uniResult.await().indefinitely());
		ResponseException responseException = (ResponseException) throwable.getCause();

		assertEquals(404, responseException.getErrorCode());
		assertEquals("subscription not found", responseException.getDetail());
		verify(subDAO, times(1)).updateSubscription(any(), any());

	}

	@SuppressWarnings("unchecked")
	@Test
	public void deleteSubscriptionTest() {

		RowSet<Row> rowSetMock = mock(RowSet.class);
		Uni<RowSet<Row>> uniRowsetMock = Uni.createFrom().item(rowSetMock);
		when(subDAO.deleteSubscription(any())).thenReturn(uniRowsetMock);

		Uni<Void> uniEmiiterResponse = Uni.createFrom().nullItem();
		when(internalSubEmitter.send(any(SubscriptionRequest.class))).thenReturn(uniEmiiterResponse);

		Uni<NGSILDOperationResult> uniResult = subscriptionService.deleteSubscription(tenant, subscriptionId);
		NGSILDOperationResult result = uniResult.await().indefinitely();

		assertEquals(subscriptionId, result.getEntityId());
		verify(subDAO, times(1)).deleteSubscription(any(DeleteSubscriptionRequest.class));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void getSubscriptionTest() {

		Row rowMock = mock(Row.class);
		RowSet<Row> rowSetMock = mock(RowSet.class);
		RowIterator<Row> rowIteratorMock = mock(RowIterator.class);
		when(rowSetMock.size()).thenReturn(1);
		when(rowSetMock.iterator()).thenReturn(rowIteratorMock);
		when(rowIteratorMock.next()).thenReturn(rowMock);
		JsonObject jsonObject = new JsonObject();
		jsonObject.put("@id", subscriptionId);
		when(rowMock.getJsonObject(anyInt())).thenReturn(jsonObject);
		Uni<RowSet<Row>> uniRowsetMock = Uni.createFrom().item(rowSetMock);
		when(subDAO.getSubscription(any(), any())).thenReturn(uniRowsetMock);

		Uni<Map<String, Object>> uniResult = subscriptionService.getSubscription(tenant, subscriptionId);
		Map<String, Object> result = uniResult.await().indefinitely();

		assertEquals(subscriptionId, result.get("@id"));
		verify(subDAO, times(1)).getSubscription(any(), any());

	}

	@SuppressWarnings("unchecked")
	@Test
	public void getSubscriptionNotFoundTest() {

		RowSet<Row> rowSetMock = mock(RowSet.class);
		when(rowSetMock.size()).thenReturn(0);
		Uni<RowSet<Row>> uniRowsetMock = Uni.createFrom().item(rowSetMock);
		when(subDAO.getSubscription(any(), any())).thenReturn(uniRowsetMock);

		Uni<Map<String, Object>> uniResult = subscriptionService.getSubscription(tenant, subscriptionId);

		Throwable throwable = assertThrows(CompletionException.class, () -> uniResult.await().indefinitely());
		ResponseException responseException = (ResponseException) throwable.getCause();

		assertEquals(404, responseException.getErrorCode());
		assertEquals("subscription not found", responseException.getDetail());
		verify(subDAO, times(1)).getSubscription(any(), any());

	}

	@SuppressWarnings("unchecked")
	@Test
	public void getAllSubscriptionsTest() {

		Row rowMock = mock(Row.class);
		RowSet<Row> rowSetMock = mock(RowSet.class);
		RowIterator<Row> rowIteratorMock = mock(RowIterator.class);
		when(rowSetMock.size()).thenReturn(1);
		when(rowSetMock.iterator()).thenReturn(rowIteratorMock);
		when(rowIteratorMock.next()).thenReturn(rowMock);
		when(rowIteratorMock.hasNext()).thenReturn(true).thenReturn(false);
		JsonObject jsonObject = new JsonObject();
		jsonObject.put("@id", subscriptionId);
		when(rowMock.getJsonObject(anyInt())).thenReturn(jsonObject);
		Uni<RowSet<Row>> uniRowsetMock = Uni.createFrom().item(rowSetMock);
		when(subDAO.getAllSubscriptions(any(), anyInt(), anyInt())).thenReturn(uniRowsetMock);

		Uni<QueryResult> uniResult = subscriptionService.getAllSubscriptions(tenant, 0, 0);
		QueryResult result = uniResult.await().indefinitely();

		assertEquals(1L, result.getCount());
		verify(subDAO, times(1)).getAllSubscriptions(any(), anyInt(), anyInt());

	}

	@SuppressWarnings("unchecked")
	@Test
	public void getAllSubscriptionsEmptyResultTest() {

		Row rowMock = mock(Row.class);
		RowSet<Row> rowSetMock = mock(RowSet.class);
		RowIterator<Row> rowIteratorMock = mock(RowIterator.class);
		when(rowSetMock.size()).thenReturn(0);
		when(rowSetMock.iterator()).thenReturn(rowIteratorMock);
		when(rowIteratorMock.next()).thenReturn(rowMock);
		when(rowIteratorMock.hasNext()).thenReturn(false);
		Uni<RowSet<Row>> uniRowsetMock = Uni.createFrom().item(rowSetMock);
		when(subDAO.getAllSubscriptions(any(), anyInt(), anyInt())).thenReturn(uniRowsetMock);

		Uni<QueryResult> uniResult = subscriptionService.getAllSubscriptions(tenant, 0, 0);
		QueryResult result = uniResult.await().indefinitely();

		assertEquals(0L, result.getCount());
		verify(subDAO, times(1)).getAllSubscriptions(any(), anyInt(), anyInt());

	}

	@Test
	public void remoteNotifyTest() throws Exception {

		Field field = SubscriptionService.class.getDeclaredField("remoteNotifyCallbackId2InternalSub");
		field.setAccessible(true);
		field.set(subscriptionService, new HashMap<String, SubscriptionRequest>());

		Uni<Void> resultUni = subscriptionService.remoteNotify(notificationId, resolved, context);
		resultUni.await().indefinitely();

		verify(localEntityService, times(0)).getEntityById(any(), any(), anyBoolean());
	}

}
