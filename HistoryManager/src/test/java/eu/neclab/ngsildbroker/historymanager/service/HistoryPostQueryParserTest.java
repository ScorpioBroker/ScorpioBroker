package eu.neclab.ngsildbroker.historymanager.service;

import io.agroal.api.AgroalDataSource;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.vertx.mutiny.pgclient.PgPool;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import java.lang.Integer;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
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
import eu.neclab.ngsildbroker.commons.controllers.QueryControllerFunctions;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.results.QueryResult;
import eu.neclab.ngsildbroker.commons.storage.StorageDAO;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.ngsiqueries.ParamsResolver;
import eu.neclab.ngsildbroker.historymanager.repository.HistoryDAO;
import groovy.util.logging.Slf4j;

import com.github.jsonldjava.core.Context;

@QuarkusTest
@Slf4j
@TestMethodOrder(OrderAnnotation.class)
public class HistoryPostQueryParserTest {

	@Mock
	HistoryDAO historyDAO;

	@Mock
	private StorageDAO storageDAO;

	@Mock
	private AgroalDataSource writerDataSource;

	@Mock
	private PgPool pgClient;;

	@ConfigProperty(name = "scorpio.directDB", defaultValue = "true")
	boolean directDB;

	@ConfigProperty(name = "scorpio.history.default-limit", defaultValue = "50")
	int defaultLimit;

	@ConfigProperty(name = "scorpio.history.max-limit", defaultValue = "1000")
	int maxLimit;

	@Mock
	MutinyEmitter<BaseRequest> kafkaSenderInterface;

	@Mock
	QueryParams queryParams;

	@Mock
	QueryResult queryResult;

	@Mock
	ParamsResolver paramsResolver;

	@Mock
	QueryControllerFunctions controller;

	@InjectMocks
	@Spy
	private HistoryPostQueryParser historyPostQueryParser;

	@Mock
	HistoryService service;

	URI uri;

	String attrsPayload;
	String entityPayload;
	String temporalQPayload;
	String invalidPayload;
	String invalidTypePayload;
	JsonNode blankNode;
	JsonNode payloadNode;

	ArrayListMultimap<String, String> multimaparr = ArrayListMultimap.create();

	@BeforeEach
	public void setUp() throws Exception {
		MockitoAnnotations.initMocks(this);

		pgClient.preparedQuery("SELECT $1");
		ObjectMapper objectMapper = new ObjectMapper();

		attrsPayload = "{\r\n" + "    \"@type\": [\"https://uri.etsi.org/ngsi-ld/default-context/Query\"],\r\n"
				+ "    \"https://uri.etsi.org/ngsi-ld/attrs\": [\r\n" + "        {\r\n"
				+ "            \"@id\": \"smartcity:houses:house2\",\r\n"
				+ "            \"@type\": [\"https://uri.etsi.org/ngsi-ld/default-context/House\"]\r\n"
				+ "        }\r\n" + "    ]\r\n" + "}";

		entityPayload = "{\r\n" + "    \"@type\": [\"https://uri.etsi.org/ngsi-ld/default-context/Query\"],\r\n"
				+ "    \"https://uri.etsi.org/ngsi-ld/entities\": [\r\n" + "        {\r\n"
				+ "            \"@id\": \"smartcity:houses:house2\",\r\n"
				+ "            \"@type\": [\"https://uri.etsi.org/ngsi-ld/default-context/House\"]\r\n"
				+ "        }\r\n" + "    ]\r\n" + "}";

		temporalQPayload = "{\r\n" + "    \"@type\": [\"https://uri.etsi.org/ngsi-ld/default-context/Query\"],\r\n"
				+ "    \"https://uri.etsi.org/ngsi-ld/temporalQ\": [\r\n" + "        {\r\n"
				+ "            \"@id\": \"smartcity:houses:house2\",\r\n"
				+ "            \"@type\": [\"https://uri.etsi.org/ngsi-ld/default-context/House\"]\r\n"
				+ "        }\r\n" + "    ]\r\n" + "}";

		invalidTypePayload = "{\r\n" + "    \"type\": \"Query\",\r\n" + "    \"entities\": [\r\n" + "        {\r\n"
				+ "            \"@id\": \"smartcity:houses:house2\",\r\n" + "            \"@type\": \"House\"\r\n"
				+ "        }\r\n" + "    ]\r\n" + "}";

		invalidPayload = "{\r\n" + "    \"@type\": \"Query\",\r\n" + "    \"entities\": [\r\n" + "        {\r\n"
				+ "            \"@id\": \"smartcity:houses:house2\",\r\n" + "            \"@type\": \"House\"\r\n"
				+ "        }\r\n" + "    ]\r\n" + "}";

		blankNode = objectMapper.createObjectNode();
		payloadNode = objectMapper.readTree(attrsPayload);
		payloadNode = objectMapper.readTree(entityPayload);
		payloadNode = objectMapper.readTree(temporalQPayload);
		payloadNode = objectMapper.readTree(invalidPayload);
		payloadNode = objectMapper.readTree(invalidTypePayload);
		directDB = true;
	}

	@AfterEach
	public void tearDown() {
		attrsPayload = null;
		entityPayload = null;
		temporalQPayload = null;
		invalidPayload = null;
		invalidTypePayload = null;
	}

	/**
	 * this method is use test "parse" method for querying attrs
	 */
	@Test
	@Order(1)

	public void historyQueryAttrsTest() throws ResponseException {

		ArrayListMultimap<String, String> subIds = ArrayListMultimap.create();
		multimaparr.put("content-type", "application/json");
		Gson gson = new Gson();
		Map<String, Object> queries = gson.fromJson(attrsPayload, Map.class);
		Integer limit = null;
		Integer offset = null;
		boolean count = false;
		List<String> options = new ArrayList<>();
		Context context = new Context();
		QueryParams result = historyPostQueryParser.parse(queries, limit, offset, defaultLimit, maxLimit, count,
				options, context);
		Assertions.assertEquals("null", result.getAttrs());
		Mockito.verify(historyPostQueryParser).parse(queries, limit, offset, defaultLimit, maxLimit, count, options,
				context);
	}

	/**
	 * this method is use test "parse" method for querying temporal entities
	 */
	@Test
	@Order(2)
	public void historyQueryEntitiesTest() throws ResponseException {

		ArrayListMultimap<String, String> subIds = ArrayListMultimap.create();
		multimaparr.put("content-type", "application/json");
		Gson gson = new Gson();
		Map<String, Object> queries = gson.fromJson(entityPayload, Map.class);
		Integer limit = -1;
		Integer offset = 0;
		boolean count = false;
		List<String> options = new ArrayList<>();
		Context context = new Context();
		QueryParams result = historyPostQueryParser.parse(queries, limit, offset, defaultLimit, maxLimit, count,
				options, context);
		Assertions.assertEquals(count, result.getCountResult());
		Mockito.verify(historyPostQueryParser).parse(queries, limit, offset, defaultLimit, maxLimit, count, options,
				context);
	}

	/**
	 * this method is use test "parse" method for querying temporal entities when
	 * time is empty
	 */
	@Test
	@Order(3)
	public void historyQueryTest() throws ResponseException {

		ArrayListMultimap<String, String> subIds = ArrayListMultimap.create();
		multimaparr.put("content-type", "application/json");
		Gson gson = new Gson();
		Map<String, Object> queries = gson.fromJson(temporalQPayload, Map.class);
		Integer limit = -1;
		Integer offset = 0;
		boolean count = false;
		List<String> options = new ArrayList<>();
		Context context = new Context();
		try {
			historyPostQueryParser.parse(queries, limit, offset, defaultLimit, maxLimit, count, options, context);
		} catch (Exception e) {
			Assertions.assertEquals("Time is empty", e.getMessage());
			Mockito.verify(historyPostQueryParser).parse(queries, limit, offset, defaultLimit, maxLimit, count, options,
					context);
		}
	}

	/**
	 * this method is use test "parse" method for querying temporal entities when
	 * query is invalid
	 */
	@Test
	@Order(4)
	public void invalidHistoryQueryTest() throws ResponseException {

		ArrayListMultimap<String, String> subIds = ArrayListMultimap.create();
		multimaparr.put("content-type", "application/json");
		Gson gson = new Gson();
		Map<String, Object> queries = gson.fromJson(invalidPayload, Map.class);
		Integer limit = -1;
		Integer offset = 0;
		boolean count = false;
		List<String> options = new ArrayList<>();
		Context context = new Context();
		try {
			historyPostQueryParser.parse(queries, limit, offset, defaultLimit, maxLimit, count, options, context);
		} catch (Exception e) {
			Assertions.assertEquals("Type has to be Query for this operation", e.getMessage());
			Mockito.verify(historyPostQueryParser).parse(queries, limit, offset, defaultLimit, maxLimit, count, options,
					context);
		}
	}

	/**
	 * this method is use test "parse" method for querying temporal entities when
	 * type is invalid
	 */
	@Test
	@Order(5)
	public void invalidTypeHistoryQueryTest() throws ResponseException {

		ArrayListMultimap<String, String> subIds = ArrayListMultimap.create();
		multimaparr.put("content-type", "application/json");
		Gson gson = new Gson();
		Map<String, Object> queries = gson.fromJson(invalidTypePayload, Map.class);
		Integer limit = -1;
		Integer offset = 0;
		boolean count = false;
		List<String> options = new ArrayList<>();
		Context context = new Context();
		try {
			historyPostQueryParser.parse(queries, limit, offset, defaultLimit, maxLimit, count, options, context);
		} catch (Exception e) {
			Assertions.assertEquals("type is an unknown entry", e.getMessage());
			Mockito.verify(historyPostQueryParser).parse(queries, limit, offset, defaultLimit, maxLimit, count, options,
					context);
		}
	}
}