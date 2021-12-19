package eu.neclab.ngsildbroker.queryhandler.services;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.datatypes.QueryResult;
import eu.neclab.ngsildbroker.commons.datatypes.RemoteQueryResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.EntityQueryService;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;
import eu.neclab.ngsildbroker.queryhandler.repository.CSourceDAO;
import eu.neclab.ngsildbroker.queryhandler.repository.QueryDAO;

@Service
public class QueryService implements EntityQueryService{

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
	ObjectMapper objectMapper;

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
	CSourceDAO cSourceDAO;

	@Value("${directDbConnection}")
	boolean directDbConnection;

	@Autowired
	ReplyingKafkaTemplate<String, String, String> kafkaTemplate;

	@Autowired
	RestTemplate restTemplate;

	protected JsonLdOptions opts = new JsonLdOptions(JsonLdOptions.JSON_LD_1_1);

	/*
	 * private QueryProducerChannel producerChannels;
	 * 
	 * public QueryService(QueryProducerChannel producerChannels) {
	 * 
	 * this.producerChannels = producerChannels; }
	 */

	@PostConstruct
	private void setup() {
		// kafkaTemplate.setReplyTimeout(replyTimeout);
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
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(requestTopic,
				storageManagerQuery);
		// set reply topic in header
		record.headers().add(new RecordHeader(KafkaHeaders.REPLY_TOPIC, queryResultTopic.getBytes()));
		RequestReplyFuture<String, String, String> sendAndReceive = kafkaTemplate.sendAndReceive(record);
		// get consumer record
		ConsumerRecord<String, String> consumerRecord = sendAndReceive.get();
		// read from byte array
		List<String> entityList = new ArrayList<String>();
		
		entityList.add(consumerRecord.value());
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
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(csourceQueryTopic,
				contextRegistryQuery);
		// set reply topic in header
		record.headers().add(new RecordHeader(KafkaHeaders.REPLY_TOPIC, queryResultTopic.getBytes()))
				.add(KafkaHeaders.MESSAGE_KEY, "dummy".getBytes());// change with some useful key
		RequestReplyFuture<String, String, String> sendAndReceive = kafkaTemplate.sendAndReceive(record);
		// get consumer record
		ConsumerRecord<String, String> consumerRecord = sendAndReceive.get();
		// return consumer value
		logger.debug("getFromContextRegistry() :: completed");
		contextRegistryData = new String((String) consumerRecord.value());
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

			Future<RemoteQueryResult> futureContextRegistry = executorService.submit(new Callable<RemoteQueryResult>() {
				public RemoteQueryResult call() throws Exception {
					try {

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

						Set<Callable<RemoteQueryResult>> callablesCollection = new HashSet<Callable<RemoteQueryResult>>();
						if (brokerList.getActualDataString() == null) {
							return null;
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
							Callable<RemoteQueryResult> callable = () -> {
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
									count = Integer.parseInt(
											response.getHeaders().get(NGSIConstants.COUNT_HEADER_RESULT).get(0));
								}
								logger.debug("http call result :: ::" + resultBody);

								RemoteQueryResult result = new RemoteQueryResult(null, ErrorType.None, -1, true);
								result.setCount(count);
								result.addData(objectMapper.readTree(JsonUtils
										.toString(JsonLdProcessor.expand(linkHeaders, JsonUtils.fromString(resultBody),
												opts, AppConstants.ENTITY_RETRIEVED_PAYLOAD, true))));
								return result;
							};
							callablesCollection.add(callable);

						}
						logger.debug("csource call response :: ");
						// fromCsources.forEach(e -> logger.debug(e));
						return getDataFromCsources(callablesCollection);
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
			RemoteQueryResult fromCsources;
			try {
				fromCsources = futureContextRegistry.get();
			} catch (Exception e) {
				logger.error("Failed to get data from registry", e);
				throw new ResponseException(ErrorType.InternalError, "Failed to get data from registry");
			}
			if (fromCsources != null) {
				try {
					fromStorage = mergeStorage(fromCsources, fromStorage);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			result = fromStorage;
			// logger.trace("response from storage :: ");
			// fromStorage.forEach(e -> logger.debug(e));
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
			//TODO redo with views in sql and add proper storagemanager solution
			String data = null; //operations.getMessage(qToken, KafkaConstants.PAGINATION_TOPIC);
			if (data == null) {
				throw new ResponseException(ErrorType.BadRequestData,
						"The provided qtoken is not valid. Provide a valid qtoken or remove the parameter to start a new query");
			}
			
			
			/*
			 * try { DataInputStream in = new DataInputStream(new
			 * ByteArrayInputStream(data)); while (in.available() > 0) {
			 * aggregatedResult.add(in.readUTF()); } } catch (IOException e) {
			 * logger.error("failed reading in utf of data", e); throw new
			 * ResponseException(ErrorType.BadRequestData, "failed reading in utf of data");
			 * }
			 */			int end = offset + limit;
			if (end > aggregatedResult.size()) {
				end = aggregatedResult.size();
			}
			aggregatedResult.subList(offset, end);
			dataLeft = aggregatedResult.size() - end;
			result.setDataString(aggregatedResult);
		}

		result.setqToken(qToken);
		result.setLimit(limit);
		result.setOffset(offset);
		result.setResultsLeftAfter(dataLeft);
		result.setResultsLeftBefore(offset);
		return result;
	}

	private QueryResult mergeStorage(RemoteQueryResult fromCsources, QueryResult fromStorage) throws IOException {
		if (fromStorage.getActualDataString() != null) {
			for (String entry : fromStorage.getActualDataString()) {
				JsonNode entity = objectMapper.readTree(entry);
				fromCsources.addData(entity);
			}
			fromCsources.setCount(fromCsources.getCount() + fromStorage.getCount());
		}
		return fromCsources;
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
	private RemoteQueryResult getDataFromCsources(Set<Callable<RemoteQueryResult>> callablesCollection)
			throws ResponseException, Exception {
		if (callablesCollection == null || callablesCollection.isEmpty()) {
			return null;
		}
		ExecutorService executorService = Executors.newFixedThreadPool(2);
		List<Future<RemoteQueryResult>> futures = executorService.invokeAll(callablesCollection);
		RemoteQueryResult queryResult = new RemoteQueryResult(null, ErrorType.None, -1, true);
		int count = 0;
		for (Future<RemoteQueryResult> future : futures) {
			logger.trace("future.isDone = " + future.isDone());
			RemoteQueryResult tempResult = future.get();
			for (ObjectNode entry : tempResult.getId2Data().values()) {
				queryResult.addData(entry);
			}
			count += tempResult.getCount();
		}
		executorService.shutdown();
		logger.trace("getDataFromCsources() completed ::");
		queryResult.setCount(count);
		return queryResult;
	}
}