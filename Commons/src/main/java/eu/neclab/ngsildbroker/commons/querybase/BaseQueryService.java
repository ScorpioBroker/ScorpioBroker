package eu.neclab.ngsildbroker.commons.querybase;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.datatypes.results.QueryResult;
import eu.neclab.ngsildbroker.commons.datatypes.results.RemoteQueryResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.EntryQueryService;
import eu.neclab.ngsildbroker.commons.storage.StorageDAO;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;

public abstract class BaseQueryService implements EntryQueryService {

	private final static Logger logger = LoggerFactory.getLogger(BaseQueryService.class);

//	public static final Gson GSON = DataSerializer.GSON;

	@Value("${atcontext.url}")
	String atContextServerUrl;

	private StorageDAO entryDAO;

	private StorageDAO registryDAO;

	@Value("${scorpio.directDB}")
	boolean directDbConnection;

	RestTemplate restTemplate = HttpUtils.getRestTemplate();

	protected JsonLdOptions opts = new JsonLdOptions(JsonLdOptions.JSON_LD_1_1);

	@PostConstruct
	private void setup() {
		entryDAO = getQueryDAO();
		registryDAO = getCsourceDAO();
	}

	/**
	 * Method is used for query request and query response is being implemented as
	 * synchronized flow
	 * 
	 * @param qp
	 * @return String
	 * @throws Exception
	 */
	public QueryResult getFromStorageManager(QueryParams qp) throws Exception {
		QueryResult queryResult = new QueryResult(null, null, ErrorType.None, -1, true);
		return queryResult;
	}

	protected abstract StorageDAO getQueryDAO();

	protected abstract StorageDAO getCsourceDAO();

	/**
	 * Method is used for query request and query response is being implemented as
	 * synchronized flow
	 * 
	 * @param qp
	 * @return String
	 * @throws Exception
	 */
	public QueryResult getFromContextRegistry(QueryParams qp) throws Exception {
		QueryResult queryResult = new QueryResult(null, null, ErrorType.None, -1, true);
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
	public QueryResult getData(QueryParams qp, String rawQueryString, List<Object> linkHeaders,
			ArrayListMultimap<String, String> headers, Boolean postQuery) throws ResponseException {

		QueryResult result = new QueryResult(null, null, ErrorType.None, -1, true);

		ExecutorService executorService = Executors.newFixedThreadPool(2);

		Future<QueryResult> futureStorageManager = executorService.submit(new Callable<QueryResult>() {
			public QueryResult call() throws Exception {
				logger.trace("Asynchronous Callable storage manager");
				if (directDbConnection) {
					try {
						return entryDAO.query(qp);
					} catch (Exception e) {
						logger.error(e.getMessage());
						throw new ResponseException(ErrorType.TenantNotFound, "Tenant not found");
					}
				} else {
					return getFromStorageManager(qp);
				}
			}
		});

		Future<RemoteQueryResult> futureContextRegistry = executorService.submit(new Callable<RemoteQueryResult>() {
			public RemoteQueryResult call() throws Exception {
				/*
				 * for now we just check if the registryDAO is null to see if we call the
				 * registry at all this is a bit dangerous because it's implicit logic which
				 * might be overseen when thinking about the get results through kafka scenario
				 */
				if (registryDAO == null) {
					return new RemoteQueryResult(null, ErrorType.None, -1, true);
				}
				QueryResult registrations = null;
				if (directDbConnection) {
					registrations = registryDAO.query(qp);
				} else {
					registrations = getFromContextRegistry(qp);
				}
				try {
					logger.trace("Asynchronous 1 context registry");
					if (registryDAO == null) {
						return new RemoteQueryResult(null, ErrorType.None, -1, true);
					}
					if (registrations.getActualDataString() == null) {
						return new RemoteQueryResult(null, ErrorType.None, -1, true);
					}
					Set<Callable<RemoteQueryResult>> callablesCollection = new HashSet<Callable<RemoteQueryResult>>();
					for (String registration : registrations.getActualDataString()) {
						Map<String, Object> reg = (Map<String, Object>) JsonUtils.fromString(registration);
						String tenant = HttpUtils.getTenantFromHeaders(headers);
						String internalRegId = AppConstants.INTERNAL_REGISTRATION_ID;
						if (tenant != null && !tenant.equals(AppConstants.INTERNAL_NULL_KEY)) {
							internalRegId += ":" + tenant;
						}
						if (reg.get(NGSIConstants.JSON_LD_ID).equals(internalRegId)) {
							continue;
						}
						String tmpEndpoint = ((List<Map<String, String>>) reg.get(NGSIConstants.NGSI_LD_ENDPOINT))
								.get(0).get(NGSIConstants.JSON_LD_VALUE);
						HttpHeaders additionalHeaders = HttpUtils.getAdditionalHeaders(reg, linkHeaders,
								headers.get(HttpHeaders.ACCEPT.toLowerCase()));
						if (linkHeaders != null) {
							for (Object entry : linkHeaders) {
								additionalHeaders.add("Link", "<" + entry
										+ ">; rel=http://www.w3.org/ns/json-ld#context; type=\"application/ld+json\"");
							}
						}
						String endpoint;
						if (tmpEndpoint.endsWith("/")) {
							endpoint = tmpEndpoint.substring(0, tmpEndpoint.length() - 1);
						} else {
							endpoint = tmpEndpoint;
						}
						logger.debug("url " + endpoint + "/ngsi-ld/v1/entities/?" + rawQueryString);
						Callable<RemoteQueryResult> callable = () -> {
							HttpEntity<String> entity;
							String resultBody;
							ResponseEntity<String> response;
							long count = 0;
							if (postQuery) {
								entity = new HttpEntity<String>(rawQueryString, additionalHeaders);
								response = restTemplate.exchange(endpoint + "/ngsi-ld/v1/entityOperations/query",
										HttpMethod.POST, entity, String.class);
								resultBody = response.getBody();
							} else {
								additionalHeaders.remove(HttpHeaders.ACCEPT);
								additionalHeaders.add(HttpHeaders.ACCEPT, "application/json");
								entity = new HttpEntity<String>(additionalHeaders);
								response = restTemplate.exchange(
										new URI(endpoint + "/ngsi-ld/v1/entities/" + encodeQuery(rawQueryString)),
										HttpMethod.GET, entity, String.class);
								resultBody = response.getBody();
							}
							if (response.getHeaders().containsKey(NGSIConstants.COUNT_HEADER_RESULT)) {
								count = Long
										.parseLong(response.getHeaders().get(NGSIConstants.COUNT_HEADER_RESULT).get(0));
							}
							logger.debug("http call result :: ::" + resultBody);

							RemoteQueryResult result = new RemoteQueryResult(null, ErrorType.None, -1, true);
							result.setCount(count);
							if (resultBody != null && !resultBody.isBlank()) {
								result.addData(JsonLdProcessor.expand(linkHeaders, JsonUtils.fromString(resultBody),
										opts, 999,
										HttpUtils.parseAcceptHeader(additionalHeaders.get(HttpHeaders.ACCEPT)) == 2));
							}
							return result;
						};
						callablesCollection.add(callable);
					}
					logger.debug("csource call response :: ");
					return getDataFromCsources(callablesCollection);
				} catch (ResourceAccessException | UnknownHostException e) {
					logger.error("Failed to reach an endpoint in the registry", e);
				} catch (Exception e) {
					logger.error(
							"No reply from registry. Looks like you are running without a context source registry.", e);
				}
				return new RemoteQueryResult(null, ErrorType.None, -1, true);
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
		// TODO redo the qtoken approach or remove it...
//		if (data == null) {
//			throw new ResponseException(ErrorType.BadRequestData,
//					"The provided qtoken is not valid. Provide a valid qtoken or remove the parameter to start a new query");
		return result;

	}

	private QueryResult mergeStorage(RemoteQueryResult fromCsources, QueryResult fromStorage) throws IOException {
		if (fromStorage == null || fromCsources == null) {
		}
		if (fromStorage.getActualDataString() != null) {
			for (String entry : fromStorage.getActualDataString()) {
				Object entity = JsonUtils.fromString(entry);
				fromCsources.addData(entity);
			}
		}
		if (fromStorage != null && fromCsources != null) {
			fromCsources.setCount(fromCsources.getCount() + fromStorage.getCount());
		}
		return fromCsources;
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
	private RemoteQueryResult getDataFromCsources(Set<Callable<RemoteQueryResult>> callablesCollection)
			throws ResponseException, Exception {
		if (callablesCollection == null || callablesCollection.isEmpty()) {
			return null;
		}
		ExecutorService executorService = Executors.newFixedThreadPool(2);
		List<Future<RemoteQueryResult>> futures = executorService.invokeAll(callablesCollection);
		RemoteQueryResult queryResult = new RemoteQueryResult(null, ErrorType.None, -1, true);
		long count = 0;
		for (Future<RemoteQueryResult> future : futures) {
			logger.trace("future.isDone = " + future.isDone());
			RemoteQueryResult tempResult;
			try {
				tempResult = future.get();
			} catch (Exception e) {
				logger.warn("Remote query failed", e);
				continue;
			}
			for (Map<String, Object> entry : tempResult.getId2Data().values()) {
				queryResult.addData(entry);
			}
			count += tempResult.getCount();
		}
		executorService.shutdown();
		logger.trace("getDataFromCsources() completed ::");
		queryResult.setCount(count);
		return queryResult;
	}

	private String encodeQuery(String query) {
		String[] params = query.split("&");
		StringBuilder result = new StringBuilder();
		for (String param : params) {
			int index = param.indexOf("=");
			if (index == -1) {
				result.append(param);
			} else {
				result.append(param.substring(0, index + 1));
				result.append(URLEncoder.encode(param.substring(index + 1), Charset.forName("utf-8")));
			}
			result.append('&');

		}
		return result.substring(0, result.length() - 1);
	}
}