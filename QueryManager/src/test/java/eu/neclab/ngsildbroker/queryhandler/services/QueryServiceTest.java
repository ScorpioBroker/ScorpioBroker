package eu.neclab.ngsildbroker.queryhandler.services;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

import eu.neclab.ngsildbroker.commons.datatypes.RegistrationEntry;
import eu.neclab.ngsildbroker.queryhandler.controller.CustomProfile;
import eu.neclab.ngsildbroker.queryhandler.repository.QueryDAO;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.Uni;
import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;

@QuarkusTest
@TestMethodOrder(OrderAnnotation.class)
@TestProfile(CustomProfile.class)
public class QueryServiceTest {

	@InjectMocks
	QueryService queryService;

	@Mock
	Vertx vertx;

	@Mock
	WebClient webClient;

	@Mock
	Context context;

	@Mock
	QueryDAO queryDAO;

	String entityId = "urn:test:testentity2";
	Map<String, Object> resolved = null;
	String jsonLdObject;

	@BeforeEach
	public void setUp() throws Exception {
		MockitoAnnotations.openMocks(this);
		Table<String, String, List<RegistrationEntry>> registriesMap = HashBasedTable.create();
		Uni<Table<String, String, List<RegistrationEntry>>> uniRegistriesMap = Uni.createFrom().item(registriesMap);
		when(queryDAO.getAllRegistries()).thenReturn(uniRegistriesMap);

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

	@Test
	@Order(1)
	public void retrieveEntityLocallyTest() {

		Uni<Map<String, Object>> getEntityRes = Uni.createFrom().item(resolved);

		when(queryDAO.getEntity(anyString(), anyString(), any())).thenReturn(getEntityRes);

		Uni<Map<String, Object>> originalEntityResUni = queryService.retrieveEntity(context, "", entityId, null, null,
				true,null,null,false,1,null);

		Map<String, Object> originalEntityRes = originalEntityResUni.await().indefinitely();

		assertEquals(true,
				originalEntityRes.containsKey("https://uri.etsi.org/ngsi-ld/default-context/complexproperty"));
		verify(queryDAO, times(1)).getEntity(anyString(), anyString(), any());

	}

	@Test
	@Order(2)
	public void retrieveEntityNotFoundTest() {

		Uni<Map<String, Object>> getEntityRes = Uni.createFrom().item(new HashMap<String, Object>());
		when(queryDAO.getEntity(anyString(), anyString(), any())).thenReturn(getEntityRes);
		Uni<Map<String, Object>> originalEntityResUni = queryService.retrieveEntity(context, "", entityId, null, null,
				true,null,null,false,1,null);
		Map<String, Object> originalEntityRes = originalEntityResUni.await().indefinitely();

		assertTrue(originalEntityRes.isEmpty());
	}
}