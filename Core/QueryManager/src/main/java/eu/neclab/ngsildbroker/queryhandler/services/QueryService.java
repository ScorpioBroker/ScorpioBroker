package eu.neclab.ngsildbroker.queryhandler.services;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import com.netflix.discovery.EurekaClient;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.KafkaConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.CSourceRegistration;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.datatypes.QueryResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.ldcontext.ContextResolverBasic;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;
import eu.neclab.ngsildbroker.commons.stream.service.KafkaOps;
import eu.neclab.ngsildbroker.queryhandler.config.QueryConsumerChannel;
import eu.neclab.ngsildbroker.queryhandler.config.QueryProducerChannel;
import eu.neclab.ngsildbroker.queryhandler.repository.QueryDAO;

@Service
public class QueryService {

	private final static Logger logger = LoggerFactory.getLogger(QueryService.class);

//	public static final Gson GSON = DataSerializer.GSON;

	@Value("${entity.topic}")
	String ENTITY_TOPIC;
	@Value("${entity.keyValues.topic}")
	String KVENTITY_TOPIC;
	@Value("${entity.withoutSysAttrs.topic}")
	String ENTITY_WITHOUT_SYSATTRS_TOPIC;
	@Value("${atcontext.url}")
	String atContextServerUrl;

	@Autowired
	KafkaOps operations;
	
	@Autowired
	ObjectMapper objectMapper;

	@Autowired
	ContextResolverBasic contextResolver;

	@Value("${query.topic}")
	String requestTopic;

	@Value("${query.result.topic}")
	String queryResultTopic;

	@Value("${csource.query.topic}")
	String csourceQueryTopic;

	@Value("${kafka.replytimeout}")
	long replyTimeout = 5000;
	
	@Value("${maxLimit}")
	int maxLimit = 500;

	@Autowired(required = false)
	QueryDAO queryDAO;
	
	@Value("${directDbConnection}")
	boolean directDbConnection;

	@SuppressWarnings("unused")

	@Autowired
	private EurekaClient eurekaClient;

	@Autowired
	ReplyingKafkaTemplate<String, byte[], byte[]> kafkaTemplate;
	
	@Autowired
	RestTemplate restTemplate;
	
	@SuppressWarnings("unused")
	// TODO check to remove ... never used
	private final QueryConsumerChannel consumerChannels;

	private QueryProducerChannel producerChannels;

	public QueryService(QueryConsumerChannel consumerChannels, QueryProducerChannel producerChannels) {
		this.consumerChannels = consumerChannels;
		this.producerChannels = producerChannels;
	}

	@PostConstruct
	private void setup() {
		kafkaTemplate.setReplyTimeout(replyTimeout);
	}

	/**
	 * Method is used for get entity based on entity id or attributes
	 * 
	 * @param entityId
	 * @param attrs
	 * @return String
	 * @throws ResponseException
	 * @throws IOException
	 */
	public String retrieveEntity(String entityId, List<String> attrs, boolean keyValues, boolean includeSysAttrs) throws ResponseException, IOException {

		logger.trace("call retriveEntity method in QueryService class");
		// null id check
		/*
		 * if (entityId == null) throw new ResponseException(ErrorType.BadRequestData);
		 */
		boolean checkData = entityId.contains("=");
		if (checkData) {
			throw new ResponseException(ErrorType.BadRequestData);
		}
		// get entity from ENTITY topic.
		byte[] entityJson;
		if (keyValues) {
			entityJson = operations.getMessage(entityId, this.KVENTITY_TOPIC);
		} else {
			if (includeSysAttrs)
				entityJson = operations.getMessage(entityId, this.ENTITY_TOPIC);
			else
				entityJson = operations.getMessage(entityId, this.ENTITY_WITHOUT_SYSATTRS_TOPIC);
		}
		// check whether exists.
		if (entityJson == null)
			throw new ResponseException(ErrorType.ResourceNotFound);

		JsonNode entityJsonBody = objectMapper.createObjectNode();
		if (attrs != null && !attrs.isEmpty()) {
			JsonNode entityChildJsonBody = objectMapper.createObjectNode();
			entityChildJsonBody = objectMapper.readTree(entityJson).get(NGSIConstants.JSON_LD_ID);
			((ObjectNode) entityJsonBody).set(NGSIConstants.JSON_LD_ID, entityChildJsonBody);
			entityChildJsonBody = objectMapper.readTree(entityJson).get(NGSIConstants.JSON_LD_TYPE);
			((ObjectNode) entityJsonBody).set(NGSIConstants.JSON_LD_TYPE, entityChildJsonBody);
			
			if (includeSysAttrs) {
				entityChildJsonBody = objectMapper.readTree(entityJson).get(NGSIConstants.NGSI_LD_CREATED_AT);
				((ObjectNode) entityJsonBody).set(NGSIConstants.NGSI_LD_CREATED_AT, entityChildJsonBody);
				entityChildJsonBody = objectMapper.readTree(entityJson).get(NGSIConstants.NGSI_LD_MODIFIED_AT);
				((ObjectNode) entityJsonBody).set(NGSIConstants.NGSI_LD_MODIFIED_AT, entityChildJsonBody);
			}
			
			for (int i = 0; i < attrs.size(); i++) {
				entityChildJsonBody = objectMapper.readTree(entityJson).get(attrs.get(i));
				((ObjectNode) entityJsonBody).set(attrs.get(i), entityChildJsonBody);
			}
		} else {
			entityJsonBody = objectMapper.readTree(entityJson);
		}
		if (keyValues && !includeSysAttrs) { // manually remove createdAt and modifiedAt at root level
			ObjectNode objectNode = (ObjectNode) entityJsonBody;
			objectNode.remove(NGSIConstants.NGSI_LD_CREATED_AT);
			objectNode.remove(NGSIConstants.NGSI_LD_MODIFIED_AT);	
			logger.debug("sysattrs removed");
		}		

		return entityJsonBody.toString();
	}

	/**
	 * Method is used for get all entity operation
	 * 
	 * @return List<JsonNode>
	 * @throws ResponseException
	 * @throws IOException
	 */
	public ArrayList<String> retriveAllEntity() throws ResponseException, IOException {
		logger.trace("retriveAllEntity() in QueryService class :: started");
		byte[] entity = null;
		Map<String, byte[]> records = operations.pullFromKafka(this.ENTITY_TOPIC);
		ArrayList<String> result = new ArrayList<String>();
		if (records.isEmpty()) {
			result.add("[]");
		} else {

			StringBuilder resultString = new StringBuilder("[");

			for (String recordKey : records.keySet()) {
				entity = records.get(recordKey);
				if (Arrays.equals(entity, AppConstants.NULL_BYTES)) {
					continue;
				}

				resultString.append(new String(entity));
				resultString.append(",");
			}
			logger.trace("retriveAllEntity() in QueryService class :: completed");

			if (resultString.length() == 1) // it has only the first square bracket, no entities
				resultString.append("]");
			else
				resultString.setCharAt(resultString.length() - 1, ']');

			result.add(resultString.toString());
			result.addAll(records.keySet());
		}
		return result;
	}

	/**
	 * Method is used for query request and query response is being implemented as
	 * synchronized flow
	 * 
	 * @param storageManagerQuery
	 * @return String
	 * @throws Exception
	 */
	public List<String> getFromStorageManager(String storageManagerQuery) throws Exception {
		// create producer record
		logger.trace("getFromStorageManager() :: started");
		ProducerRecord<String, byte[]> record = new ProducerRecord<String, byte[]>(requestTopic,
				storageManagerQuery.getBytes());
		// set reply topic in header
		record.headers().add(new RecordHeader(KafkaHeaders.REPLY_TOPIC, queryResultTopic.getBytes()));
		RequestReplyFuture<String, byte[], byte[]> sendAndReceive = kafkaTemplate.sendAndReceive(record);
		// get consumer record
		ConsumerRecord<String,  byte[]> consumerRecord = sendAndReceive.get();
		// read from byte array
		ByteArrayInputStream bais = new ByteArrayInputStream(consumerRecord.value());
		DataInputStream in = new DataInputStream(bais);
		List<String> entityList = new ArrayList<String>();
		while (in.available() > 0) {
			entityList.add(in.readUTF());
		}		
		// return consumer value		
		logger.trace("getFromStorageManager() :: completed");
		return entityList;
	}

	/**
	 * Method is used for query request and query response is being implemented as
	 * synchronized flow
	 * 
	 * @param contextRegistryQuery
	 * @return String
	 * @throws Exception
	 */
	public String getFromContextRegistry(String contextRegistryQuery) throws Exception {
		// create producer record
		String contextRegistryData = null;
		logger.trace("getFromContextRegistry() :: started");
		ProducerRecord<String, byte[]> record = new ProducerRecord<String, byte[]>(csourceQueryTopic,
				contextRegistryQuery.getBytes());
		// set reply topic in header
		record.headers().add(new RecordHeader(KafkaHeaders.REPLY_TOPIC, queryResultTopic.getBytes()))
				.add(KafkaHeaders.MESSAGE_KEY, "dummy".getBytes());// change with some useful key
		RequestReplyFuture<String, byte[], byte[]> sendAndReceive = kafkaTemplate.sendAndReceive(record);
		// get consumer record
		ConsumerRecord<String, byte[]> consumerRecord = sendAndReceive.get();
		// return consumer value
		logger.trace("getFromContextRegistry() :: completed");
		contextRegistryData = new String((byte[]) consumerRecord.value());
		logger.debug("getFromContextRegistry() data broker list::" + contextRegistryData);
		return contextRegistryData;
	}

	/**
	 * To calling storage manager and registry manager (to discover csources)
	 * 
	 * @param queryRequest
	 * @param rawQueryString
	 * @param qToken
	 * @param offset
	 * @param limit
	 * @param expandedAttrs
	 * @return List<Entity>
	 * @throws ExecutionException
	 * @throws InterruptedException
	 * @throws IOException
	 * @throws ResponseException
	 * @throws URISyntaxException
	 * @throws Exception
	 */
	public QueryResult getData(QueryParams qp, String rawQueryString, List<Object> linkHeaders,
			Integer limit, Integer offset, String qToken)
			throws ResponseException, Exception {

		List<String> aggregatedResult = new ArrayList<String>();
		QueryResult result = new QueryResult(null, null, ErrorType.None, -1, true);
		List<String> realResult;		
		int dataLeft = 0;
		if (qToken == null) {
			ExecutorService executorService = Executors.newFixedThreadPool(2);

			Future<List<String>> futureStorageManager = executorService.submit(new Callable<List<String>>() {
				public List<String> call() throws Exception {
					logger.trace("Asynchronous Callable storage manager");
					if (queryDAO != null) {
						return queryDAO.query(qp);
					} else {
						return getFromStorageManager(DataSerializer.toJson(qp));
					}
				}
			});

			Future<String> futureContextRegistry = executorService.submit(new Callable<String>() {
				public String call() throws Exception {
					logger.trace("Asynchronous 1 context registry");
					String brokerList = getFromContextRegistry(DataSerializer.toJson(qp));
					logger.debug("broker list from context registry::" + brokerList);
					return brokerList;
				}
			});

			Type listType;
			List<String> fromCsources = new ArrayList<String>();

			// Csources response
			try {
				String csourceResultString = (String) futureContextRegistry.get();
				listType = new TypeToken<ArrayList<CSourceRegistration>>() {
				}.getType();
				List<CSourceRegistration> discoveredCSources = DataSerializer
						.getCSourceRegistrations(csourceResultString, listType);
				List<URI> csourcesEndpointCollection = discoveredCSources.stream().map(e -> e.getEndpoint())
						.collect(Collectors.toList());
				logger.debug("csourcesEndpointCollection size::" + csourcesEndpointCollection.size());
				if (csourcesEndpointCollection.size() > 0) {
					csourcesEndpointCollection.forEach(e -> logger.debug(e.toString()));
					fromCsources = getDataFromCsources(csourcesEndpointCollection, rawQueryString, linkHeaders);
					logger.debug("csource call response :: ");
					fromCsources.forEach(e->logger.debug(e));
				}
			} catch (Exception e) {
				logger.error("No reply from registry. Looks like you are running without a context source registry.");
				logger.error(e.getMessage());
			}
			executorService.shutdown();

			// storage response
			logger.trace("storage task status completed :: " + futureStorageManager.isDone());
			List<String> fromStorage = (List<String>) futureStorageManager.get();
			
			logger.trace("response from storage :: " );
			fromStorage.forEach(e->logger.debug(e));
			
			aggregatedResult.addAll(fromStorage);
			if (fromCsources != null) {
				aggregatedResult.addAll(fromCsources);
			}
			logger.trace("aggregated");
			aggregatedResult.forEach(e->logger.debug(e));
			if (aggregatedResult.size() > limit) {
				qToken = generateToken();
				writeFullResultToKafka(qToken, aggregatedResult);
				int end = offset + limit;
				if (end > aggregatedResult.size()) {
					end = aggregatedResult.size();
				}
				realResult = aggregatedResult.subList(offset, end);
				dataLeft = aggregatedResult.size() - end;
			} else {
				realResult = aggregatedResult;
			}
		} else {
			// read from byte array
			byte[] data = operations.getMessage(qToken, KafkaConstants.PAGINATION_TOPIC);
			if(data == null) {
				throw new ResponseException(ErrorType.BadRequestData, "The provided qtoken is not valid. Provide a valid qtoken or remove the parameter to start a new query");
			}
			ByteArrayInputStream bais = new ByteArrayInputStream(data);
			DataInputStream in = new DataInputStream(bais);
			while (in.available() > 0) {
				aggregatedResult.add(in.readUTF());
			}
			int end = offset + limit;
			if (end > aggregatedResult.size()) {
				end = aggregatedResult.size();
			}
			realResult = aggregatedResult.subList(offset, end);
			dataLeft = aggregatedResult.size() - end;

		}
		result.setDataString(realResult);
		result.setqToken(qToken);
		result.setLimit(limit);
		result.setOffset(offset);
		result.setResultsLeftAfter(dataLeft);
		result.setResultsLeftBefore(offset);
		return result;
	}

	private void writeFullResultToKafka(String qToken, List<String> aggregatedResult) throws IOException {
		// write to byte array
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(baos);
		for (String element : aggregatedResult) {
			out.writeUTF(element);
		}		
		operations.pushToKafka(producerChannels.paginationWriteChannel(), qToken.getBytes(), baos.toByteArray());
	}

	private String generateToken() {
		return UUID.randomUUID().toString();
	}

	/**
	 * making http call to all discovered csources async.
	 * 
	 * @param endpointsList
	 * @param query
	 * @return List<Entity>
	 * @throws InterruptedException
	 * @throws ExecutionException
	 * @throws URISyntaxException
	 * @throws ResponseException
	 * @throws IOException
	 */
	private List<String> getDataFromCsources(List<URI> endpointsList, String query, List<Object> linkHeaders)
			throws ResponseException, Exception {
		logger.trace("getDataFromCsources() started ::");
		logger.trace("endpoints discovered ::");
		endpointsList.forEach(e -> logger.trace(e.toString()));
		List<String> allDiscoveredEntities = new ArrayList<String>();
		ExecutorService executorService = Executors.newFixedThreadPool(2);
		Set<Callable<String>> callablesCollection = new HashSet<Callable<String>>();
		for (URI uri : endpointsList) {
			logger.debug("url " + uri.toString() + "/ngsi-ld/v1/entities/?" + query);
			Callable<String> callable = () -> {
				HttpHeaders headers = new HttpHeaders();
				headers.addAll("Link", Arrays.asList(linkHeaders.toArray(new String[0])));
				HttpEntity entity = new HttpEntity<>(headers);
				String result = restTemplate.exchange(uri.toString() + "/ngsi-ld/v1/entities/?" + query, HttpMethod.GET,
						entity, String.class).getBody();
			
				logger.debug("http call result :: ::" + result);
				return result;
			};
			callablesCollection.add(callable);
		}
		List<Future<String>> futures = executorService.invokeAll(callablesCollection);
		// TODO: why sleep?
		// Thread.sleep(5000);
		for (Future<String> future : futures) {
			logger.trace("future.isDone = " + future.isDone());
			List<String> entitiesList = new ArrayList<String>();
			try {
				String response = (String) future.get();
				logger.debug("response from invoke all ::" + response);
				if (!("[]").equals(response) && response != null) {
					JsonNode jsonNode = objectMapper.readTree(response);
					for (int i = 0; i <= jsonNode.size(); i++) {
						if (jsonNode.get(i) != null && !jsonNode.isNull()) {
							String payload = contextResolver.expand(jsonNode.get(i).toString(), linkHeaders);
							entitiesList.add(payload);
						}
					}
				}
			} catch (JsonSyntaxException | ExecutionException e) {
				logger.error("Exception  ::", e);
			}
			allDiscoveredEntities.addAll(entitiesList);
		}
		executorService.shutdown();
		logger.trace("getDataFromCsources() completed ::");
		return allDiscoveredEntities;
	}
}
