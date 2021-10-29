package eu.neclab.ngsildbroker.queryhandler.services;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.PostConstruct;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ArrayListMultimap;
import com.google.gson.JsonSyntaxException;
import com.netflix.discovery.EurekaClient;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.KafkaConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.datatypes.QueryResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.ldcontext.ContextResolverBasic;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;
import eu.neclab.ngsildbroker.commons.stream.service.KafkaOps;
import eu.neclab.ngsildbroker.queryhandler.repository.CSourceDAO;
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
	@Qualifier("qmops")
	KafkaOps operations;

	@Autowired
	ObjectMapper objectMapper;

	@Autowired
	@Qualifier("qmconRes")
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

	@Autowired
	QueryDAO queryDAO;

	@Autowired
	@Qualifier("qmcsourcedao")
	CSourceDAO cSourceDAO;

	@Value("${directDbConnection}")
	boolean directDbConnection;

	@SuppressWarnings("unused")

	@Autowired
	private EurekaClient eurekaClient;

	@Autowired
	ReplyingKafkaTemplate<String, byte[], byte[]> kafkaTemplate;

	@Autowired
	@Qualifier("qmrestTemp")
	RestTemplate restTemplate;

	/*
	 * private QueryProducerChannel producerChannels;
	 * 
	 * public QueryService(QueryProducerChannel producerChannels) {
	 * 
	 * this.producerChannels = producerChannels; }
	 */

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
	public String retrieveEntity(String entityId, List<String> attrs, boolean keyValues, boolean includeSysAttrs)
			throws ResponseException, IOException {

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
			throw new ResponseException(ErrorType.NotFound);

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
	public QueryResult getFromStorageManager(String storageManagerQuery) throws Exception {
		// create producer record
		QueryResult queryResult = new QueryResult(null, null, ErrorType.None, -1, true);
		logger.trace("getFromStorageManager() :: started");
		ProducerRecord<String, byte[]> record = new ProducerRecord<String, byte[]>(requestTopic,
				storageManagerQuery.getBytes());
		// set reply topic in header
		record.headers().add(new RecordHeader(KafkaHeaders.REPLY_TOPIC, queryResultTopic.getBytes()));
		RequestReplyFuture<String, byte[], byte[]> sendAndReceive = kafkaTemplate.sendAndReceive(record);
		// get consumer record
		ConsumerRecord<String, byte[]> consumerRecord = sendAndReceive.get();
		// read from byte array
		ByteArrayInputStream bais = new ByteArrayInputStream(consumerRecord.value());
		DataInputStream in = new DataInputStream(bais);
		List<String> entityList = new ArrayList<String>();
		while (in.available() > 0) {
			entityList.add(in.readUTF());
		}
		// return consumer value
		logger.trace("getFromStorageManager() :: completed");
		queryResult.setActualDataString(entityList);
		return queryResult;
	}

	/**
	 * Method is used for query request and query response is being implemented as
	 * synchronized flow
	 * 
	 * @param contextRegistryQuery
	 * @return String
	 * @throws Exception
	 */
	public QueryResult getFromContextRegistry(String contextRegistryQuery) throws Exception {
		// create producer record
		String contextRegistryData = null;
		QueryResult queryResult = new QueryResult(null, null, ErrorType.None, -1, true);
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
		logger.debug("getFromContextRegistry() :: completed");
		contextRegistryData = new String((byte[]) consumerRecord.value());
		logger.debug("getFromContextRegistry() data broker list::" + contextRegistryData);
		queryResult.setActualDataString(DataSerializer.getStringList(contextRegistryData));
		return queryResult;
	}

	/**
	 * To calling storage manager and registry manager (to discover csources)
	 * 
	 * @param queryRequest
	 * @param rawQueryString
	 * @param qToken
	 * @param offset
	 * @param limit
	 * @param headers
	 * @param expandedAttrs
	 * @return List<Entity>
	 * @throws ExecutionException
	 * @throws InterruptedException
	 * @throws IOException
	 * @throws ResponseException
	 * @throws URISyntaxException
	 * @throws Exception
	 */
	public QueryResult getData(QueryParams qp, String rawQueryString, List<Object> linkHeaders, Integer limit,
			Integer offset, String qToken, Boolean showServices, Boolean countResult,
			ArrayListMultimap<String, String> headers, Boolean postQuery) throws ResponseException {

		List<String> aggregatedResult = new ArrayList<String>();
		QueryResult result = new QueryResult(null, null, ErrorType.None, -1, true);
		qp.setLimit(limit);
		qp.setCheck("string");
		qp.setOffSet(offset);
		qp.setCountResult(countResult);
		int dataLeft = 0;
		if (qToken == null) {
			ExecutorService executorService = Executors.newFixedThreadPool(2);

			Future<QueryResult> futureStorageManager = executorService.submit(new Callable<QueryResult>() {
				public QueryResult call() throws Exception {
					logger.trace("Asynchronous Callable storage manager");
					// TAKE CARE OF PAGINATION HERE
					if (queryDAO != null) {
						try {
							return queryDAO.query(qp);
						} catch (Exception e) {
							throw new ResponseException(ErrorType.TenantNotFound);
						}
					} else {
						return getFromStorageManager(DataSerializer.toJson(qp));
					}
				}
			});

			Future<QueryResult> futureContextRegistry = executorService.submit(new Callable<QueryResult>() {
				public QueryResult call() throws Exception {
					try {
						QueryResult fromCsources = new QueryResult(null, null, ErrorType.None, -1, true);
						logger.trace("Asynchronous 1 context registry");
						QueryResult brokerList;
						if (cSourceDAO != null) {
							brokerList = cSourceDAO.queryExternalCsources(qp);
						} else {
							brokerList = getFromContextRegistry(DataSerializer.toJson(qp));
						}
						Pattern p = Pattern.compile(NGSIConstants.NGSI_LD_ENDPOINT_REGEX);
						Pattern ptenant = Pattern.compile(NGSIConstants.NGSI_LD_ENDPOINT_TENANT);
						Matcher m;
						Matcher mtenant;
						QueryResult queryResult = new QueryResult(null, null, ErrorType.None, -1, true);
						Set<Callable<QueryResult>> callablesCollection = new HashSet<Callable<QueryResult>>();
						if (brokerList.getActualDataString() == null) {
							return queryResult;
						}
						for (String brokerInfo : brokerList.getActualDataString()) {
							m = p.matcher(brokerInfo);
							m.find();
							final String uri_tenant;
							String uri = m.group(1);
							mtenant = ptenant.matcher(brokerInfo);
							if (mtenant != null && mtenant.matches()) {
								mtenant.find();
								uri_tenant = mtenant.group(1);

							} else {
								uri_tenant = null;
							}
							logger.debug("url " + uri.toString() + "/ngsi-ld/v1/entities/?" + rawQueryString);
							Callable<QueryResult> callable = () -> {
								HttpHeaders callHeaders = new HttpHeaders();
								for (Entry<String, String> entry : headers.entries()) {
									String key = entry.getKey();
									if (key.equals(NGSIConstants.TENANT_HEADER)) {
										continue;
									}
									callHeaders.add(key, entry.getValue());
								}
								if (uri_tenant != null) {
									callHeaders.add(NGSIConstants.TENANT_HEADER, uri_tenant);
								}
								HttpEntity entity;

								String resultBody;
								ResponseEntity<String> response;
								int count = 0;
								if (postQuery) {
									entity = new HttpEntity<String>(rawQueryString, callHeaders);
									response = restTemplate.exchange(uri + "/ngsi-ld/v1/entityOperations/query",
											HttpMethod.POST, entity, String.class);
									resultBody = response.getBody();
								} else {
									entity = new HttpEntity<String>(callHeaders);
									response = restTemplate.exchange(uri + "/ngsi-ld/v1/entities/?" + rawQueryString,
											HttpMethod.GET, entity, String.class);
									resultBody = response.getBody();
								}
								if (response.getHeaders().containsKey(NGSIConstants.COUNT_HEADER_RESULT)) {
									count = Integer.parseInt(response.getHeaders().get(NGSIConstants.COUNT_HEADER_RESULT).get(0));
								}
								logger.debug("http call result :: ::" + resultBody);
								
								QueryResult result = new QueryResult(getDataListFromResult(resultBody), null, ErrorType.None, -1, true);
								result.setCount(count);
								return result;
							};
							callablesCollection.add(callable);

						}

						fromCsources = getDataFromCsources(callablesCollection);
						logger.debug("csource call response :: ");
						// fromCsources.forEach(e -> logger.debug(e));
						return fromCsources;
					} catch (Exception e) {
						e.printStackTrace();
						logger.error(
								"No reply from registry. Looks like you are running without a context source registry.");
						logger.error(e.getMessage());
						return null;
					}
				}
			});

			// Csources response

			executorService.shutdown();

			// storage response
			logger.trace("storage task status completed :: " + futureStorageManager.isDone());
			QueryResult fromStorage;
			try {
				fromStorage = futureStorageManager.get();
			} catch (Exception e) {
				logger.error("Failed to get data from storage", e);
				throw new ResponseException(ErrorType.InternalError, "Failed to get data from storage");
			}
			QueryResult fromCsources;
			try {
				fromCsources = futureContextRegistry.get();
			} catch (Exception e) {
				logger.error("Failed to get data from registry", e);
				throw new ResponseException(ErrorType.InternalError, "Failed to get data from registry");
			}
			// logger.trace("response from storage :: ");
			// fromStorage.forEach(e -> logger.debug(e));
			List<String> fromStorageDataList = fromStorage.getActualDataString();
			List<String> fromCsourceDataList = new ArrayList<String>();
			if (fromCsources.getActualDataString() != null) {
				fromCsourceDataList = fromCsources.getActualDataString();
			}
			int count = 0;
			if (fromStorage.getCount() != null) {
				count = fromStorage.getCount();
			}

			if (fromStorageDataList != null) {
				aggregatedResult.addAll(fromStorageDataList);
			}
			int countremote = 0;
			if (fromCsources.getCount() != null) {
				countremote = fromCsources.getCount();
			}
			if (fromCsourceDataList.size() > 0) {
				aggregatedResult.addAll(fromCsourceDataList);
			}
			if (count != 0 && countremote != 0) {
				result.setCount(count + countremote);
			}
			if (count != 0 && countremote == 0) {
				result.setCount(count);
			}
			if (count == 0 && countremote != 0) {
				result.setCount(countremote);
			}
			// logger.trace("aggregated");
			// aggregatedResult.forEach(e -> logger.debug(e));
			/*
			 * if (aggregatedResult.size() > limit) { qToken = generateToken(); String
			 * writeToken = qToken; int end = offset + limit; if (end >
			 * aggregatedResult.size()) { end = aggregatedResult.size(); } realResult =
			 * aggregatedResult.subList(offset, end); dataLeft = aggregatedResult.size() -
			 * end; new Thread() { public void run() { try {
			 * writeFullResultToKafka(writeToken, aggregatedResult); } catch (IOException e)
			 * {
			 * 
			 * } catch (ResponseException e) {
			 * 
			 * } }; }.start(); } else {
			 */
			// }
		} else {
			// read from byte array
			byte[] data = operations.getMessage(qToken, KafkaConstants.PAGINATION_TOPIC);
			if (data == null) {
				throw new ResponseException(ErrorType.BadRequestData,
						"The provided qtoken is not valid. Provide a valid qtoken or remove the parameter to start a new query");
			}
			ByteArrayInputStream bais = new ByteArrayInputStream(data);
			DataInputStream in = new DataInputStream(bais);
			try {
				while (in.available() > 0) {
					aggregatedResult.add(in.readUTF());
				}
			} catch (IOException e) {
				logger.error("failed reading in utf of data", e);
				throw new ResponseException(ErrorType.BadRequestData, "failed reading in utf of data"); 
			}
			int end = offset + limit;
			if (end > aggregatedResult.size()) {
				end = aggregatedResult.size();
			}
			aggregatedResult.subList(offset, end);
			dataLeft = aggregatedResult.size() - end;

		}
		result.setDataString(aggregatedResult);
		result.setqToken(qToken);
		result.setLimit(limit);
		result.setOffset(offset);
		result.setResultsLeftAfter(dataLeft);
		result.setResultsLeftBefore(offset);
		return result;
	}
	protected List<String> getDataListFromResult(String resultBody) throws ResponseException{
		List<String> entitiesList = new ArrayList<String>();
		try {
			
			logger.debug("response from invoke all ::" + resultBody);
			if (!("[]").equals(resultBody) && resultBody != null) {
				JsonNode jsonNode = objectMapper.readTree(resultBody);
				for (int i = 0; i <= jsonNode.size(); i++) {
					if (jsonNode.get(i) != null && !jsonNode.isNull()) {
						String payload = contextResolver.expand(jsonNode.get(i).toString(), null, true,
								AppConstants.ENTITIES_URL_ID);// , linkHeaders);
						entitiesList.add(payload);
					}
				}
			}
			return entitiesList;
		} catch (JsonSyntaxException | IOException e) {
			logger.error("Exception  ::", e);
			return new ArrayList<String>();
		}

		
	}

	// TODO decide on removal
	/*
	 * private void writeFullResultToKafka(String qToken, List<String>
	 * aggregatedResult) throws IOException, ResponseException { // write to byte
	 * array ByteArrayOutputStream baos = new ByteArrayOutputStream();
	 * DataOutputStream out = new DataOutputStream(baos); for (String element :
	 * aggregatedResult) { out.writeUTF(element); }
	 * operations.pushToKafka(producerChannels.paginationWriteChannel(),
	 * qToken.getBytes(), baos.toByteArray()); }
	 */

	/*
	 * private String generateToken() { return UUID.randomUUID().toString(); }
	 */

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
	private QueryResult getDataFromCsources(Set<Callable<QueryResult>> callablesCollection)
			throws ResponseException, Exception {
		ExecutorService executorService = Executors.newFixedThreadPool(2);
		List<Future<QueryResult>> futures = executorService.invokeAll(callablesCollection);
		QueryResult queryResult = new QueryResult(null, null, ErrorType.None, -1, true);
		// TODO: why sleep?
		// Thread.sleep(5000);
		List<String> entities = new ArrayList<String>();
		int count = 0;
		for (Future<QueryResult> future : futures) {
			logger.trace("future.isDone = " + future.isDone());
			QueryResult tempResult = future.get();
			entities.addAll(tempResult.getDataString());
			count += tempResult.getCount();
		}
		executorService.shutdown();
		logger.trace("getDataFromCsources() completed ::");
		queryResult.setActualDataString(entities);
		queryResult.setDataString(entities);
		queryResult.setCount(count);
		return queryResult;
	}
}