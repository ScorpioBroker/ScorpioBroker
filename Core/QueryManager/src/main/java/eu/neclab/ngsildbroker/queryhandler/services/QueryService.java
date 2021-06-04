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
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
	public List<String> getFromStorageManager(String storageManagerQuery) throws Exception {
		// create producer record
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
	public List<String> getFromContextRegistry(String contextRegistryQuery) throws Exception {
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
		logger.debug("getFromContextRegistry() :: completed");
		contextRegistryData = new String((byte[]) consumerRecord.value());
		logger.debug("getFromContextRegistry() data broker list::" + contextRegistryData);
		return DataSerializer.getStringList(contextRegistryData);
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
	public QueryResult getData(QueryParams qp, String rawQueryString, List<Object> linkHeaders, Integer limit,
			Integer offset, String qToken, Boolean showServices, Boolean countResult,String check) throws ResponseException, Exception {

		List<String> aggregatedResult = new ArrayList<String>();
		QueryResult result = new QueryResult(null, null, ErrorType.None, -1, true);
		List<String> realResult;
		qp.setLimit(limit);
		qp.setOffSet(offset);
        qp.setCountResult(countResult);		
		int dataLeft = 0;
		if (qToken == null) {
			ExecutorService executorService = Executors.newFixedThreadPool(2);

			Future<List<String>> futureStorageManager = executorService.submit(new Callable<List<String>>() {
				public List<String> call() throws Exception {
					logger.trace("Asynchronous Callable storage manager");
					//TAKE CARE OF PAGINATION HERE
					if (queryDAO != null) {
						return queryDAO.query(qp);
					} else {
						return getFromStorageManager(DataSerializer.toJson(qp));
					}
				}
			});

			Future<List<String>> futureContextRegistry = executorService.submit(new Callable<List<String>>() {
				public List<String> call() throws Exception {
					try {
						List<String> fromCsources = new ArrayList<String>();
						logger.trace("Asynchronous 1 context registry");
						List<String> brokerList;
						if (cSourceDAO != null) {
							brokerList = cSourceDAO.queryExternalCsources(qp);
						} else {
							brokerList = getFromContextRegistry(DataSerializer.toJson(qp));
						}
						Pattern p = Pattern.compile(NGSIConstants.NGSI_LD_ENDPOINT_REGEX);
						Matcher m;
						Set<Callable<String>> callablesCollection = new HashSet<Callable<String>>();
						for (String brokerInfo : brokerList) {
							m = p.matcher(brokerInfo);
							m.find();
							String uri = m.group(1);
							logger.debug("url " + uri.toString() + "/ngsi-ld/v1/entities/?" + rawQueryString);
							Callable<String> callable = () -> {
								HttpHeaders headers = new HttpHeaders();
								for (Object link : linkHeaders) {
									headers.add("Link", "<" + link.toString()
											+ ">; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json\"");
								}

								HttpEntity<Object> entity = new HttpEntity<Object>(headers);

								String result = restTemplate.exchange(uri + "/ngsi-ld/v1/entities/?" + rawQueryString,
										HttpMethod.GET, entity, String.class).getBody();

								logger.debug("http call result :: ::" + result);
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
			List<String> fromStorage = (List<String>) futureStorageManager.get();
			List<String> fromCsources = (List<String>) futureContextRegistry.get();
			// logger.trace("response from storage :: ");
			// fromStorage.forEach(e -> logger.debug(e));

			aggregatedResult.addAll(fromStorage);
			if (fromCsources != null) {
				aggregatedResult.addAll(fromCsources);
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
				realResult = aggregatedResult;
			//}
		} else {
			// read from byte array
			byte[] data = operations.getMessage(qToken, KafkaConstants.PAGINATION_TOPIC);
			if (data == null) {
				throw new ResponseException(ErrorType.BadRequestData,
						"The provided qtoken is not valid. Provide a valid qtoken or remove the parameter to start a new query");
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
	//TODO decide on removal
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
	private List<String> getDataFromCsources(Set<Callable<String>> callablesCollection)
			throws ResponseException, Exception {
		List<String> allDiscoveredEntities = new ArrayList<String>();
		ExecutorService executorService = Executors.newFixedThreadPool(2);
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
							String payload = contextResolver.expand(jsonNode.get(i).toString(), null, true, AppConstants.ENTITIES_URL_ID);// , linkHeaders);
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
