package eu.neclab.ngsildbroker.commons.querybase;

import java.io.IOException;
import java.net.URISyntaxException;
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
import javax.inject.Inject;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.mutiny.core.http.HttpHeaders;

public abstract class BaseQueryService implements EntryQueryService {

	private final static Logger logger = LoggerFactory.getLogger(BaseQueryService.class);

//	public static final Gson GSON = DataSerializer.GSON;

	// @Value("${atcontext.url}")
	// public String atContextServerUrl;

	private StorageDAO entryDAO;

	private StorageDAO registryDAO;

	@ConfigProperty(name = "scorpio.directDB", defaultValue = "true")
	public boolean directDbConnection;

	@Inject
	Vertx vertx;

	WebClient webClient;

	public JsonLdOptions opts = new JsonLdOptions(JsonLdOptions.JSON_LD_1_1);

	@PostConstruct
	void setup() {
		entryDAO = getQueryDAO();
		registryDAO = getCsourceDAO();
		webClient = WebClient.create(vertx);
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
				// TODO Error handling retries?
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
						if (reg.get(NGSIConstants.JSON_LD_ID).equals(AppConstants.INTERNAL_REGISTRATION_ID)) {
							continue;
						}
						String endpoint = ((List<Map<String, String>>) reg.get(NGSIConstants.NGSI_LD_ENDPOINT)).get(0)
								.get(NGSIConstants.JSON_LD_VALUE);
						MultiMap additionalHeaders = HttpUtils.getAdditionalHeaders(reg, linkHeaders,
								headers.get(HttpHeaders.ACCEPT.toString()));
						logger.debug("url " + endpoint + "/ngsi-ld/v1/entities/?" + rawQueryString);
						Callable<RemoteQueryResult> callable = () -> {

							String resultBody;
							HttpResponse<Buffer> response;
							int count = 0;
							if (postQuery) {
								response = webClient.postAbs(endpoint + "/ngsi-ld/v1/entityOperations/query")
										.putHeaders(additionalHeaders).sendBuffer(Buffer.buffer(rawQueryString))
										.result();
								resultBody = response.bodyAsString();
							} else {
								response = webClient.getAbs(endpoint + "/ngsi-ld/v1/entities/?" + rawQueryString)
										.putHeaders(additionalHeaders).send().result();
								resultBody = response.bodyAsString();
							}
							if (response.headers().contains(NGSIConstants.COUNT_HEADER_RESULT)) {
								count = Integer.parseInt(response.headers().get(NGSIConstants.COUNT_HEADER_RESULT));
							}
							logger.debug("http call result :: ::" + resultBody);

							RemoteQueryResult result = new RemoteQueryResult(null, ErrorType.None, -1, true);
							result.setCount(count);

							result.addData(JsonLdProcessor.expand(linkHeaders, JsonUtils.fromString(resultBody), opts,
									AppConstants.ENTITY_RETRIEVED_PAYLOAD,
									HttpUtils.parseAcceptHeader(additionalHeaders.getAll(HttpHeaders.ACCEPT)) == 2));
							return result;
						};
						callablesCollection.add(callable);
					}
					logger.debug("csource call response :: ");
					return getDataFromCsources(callablesCollection);
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
			System.out.println();
		}
		if (fromStorage.getActualDataString() != null) {
			for (String entry : fromStorage.getActualDataString()) {
				Object entity = JsonUtils.fromString(entry);
				fromCsources.addData(entity);
			}
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
		int count = 0;
		for (Future<RemoteQueryResult> future : futures) {
			logger.trace("future.isDone = " + future.isDone());
			RemoteQueryResult tempResult = future.get();
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
}