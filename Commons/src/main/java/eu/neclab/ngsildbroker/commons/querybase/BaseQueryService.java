package eu.neclab.ngsildbroker.commons.querybase;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.jsonldjava.core.JsonLdError;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;

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
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.vertx.UniHelper;
import io.vertx.core.Future;
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
	public Uni<QueryResult> getData(QueryParams qp, String rawQueryString, List<Object> linkHeaders,
			ArrayListMultimap<String, String> headers, Boolean postQuery) {
		Uni<QueryResult> entryQuery = entryDAO.query(qp);
		Uni<List<RemoteQueryResult>> csourceQuery = Uni.createFrom().nullItem();
		if (registryDAO != null) {
			csourceQuery = registryDAO.query(qp).onItem().transformToUni(t -> {
				List<Uni<RemoteQueryResult>> remoteResults = Lists.newArrayList();
				for (String registration : t.getActualDataString()) {
					Map<String, Object> reg;
					try {
						reg = (Map<String, Object>) JsonUtils.fromString(registration);
					} catch (IOException e1) {
						logger.error("corruption in registry found" + registration, e1);
						continue;
					}
					if (reg.get(NGSIConstants.JSON_LD_ID).equals(AppConstants.INTERNAL_REGISTRATION_ID)) {
						continue;
					}
					String endpoint = ((List<Map<String, String>>) reg.get(NGSIConstants.NGSI_LD_ENDPOINT)).get(0)
							.get(NGSIConstants.JSON_LD_VALUE);
					MultiMap additionalHeaders = HttpUtils.getAdditionalHeaders(reg, linkHeaders,
							headers.get(HttpHeaders.ACCEPT.toString()));
					logger.debug("url " + endpoint + "/ngsi-ld/v1/entities/?" + rawQueryString);

					Future<HttpResponse<Buffer>> exchange;
					if (postQuery) {
						exchange = webClient.postAbs(endpoint + "/ngsi-ld/v1/entityOperations/query")
								.putHeaders(additionalHeaders).sendBuffer(Buffer.buffer(rawQueryString));
					} else {
						exchange = webClient.getAbs(endpoint + "/ngsi-ld/v1/entities/?" + rawQueryString)
								.putHeaders(additionalHeaders).send();
					}
					remoteResults.add(UniHelper.toUni(exchange).onItem().transform(response -> {
						RemoteQueryResult remoteResult = new RemoteQueryResult(null, ErrorType.None, -1, true);
						int count = 0;
						if (response.headers().contains(NGSIConstants.COUNT_HEADER_RESULT)) {
							count = Integer.parseInt(response.headers().get(NGSIConstants.COUNT_HEADER_RESULT));
						}
						remoteResult.setCount(count);
						try {
							remoteResult.addData(JsonLdProcessor.expand(linkHeaders,
									JsonUtils.fromString(response.bodyAsString()), opts,
									AppConstants.ENTITY_RETRIEVED_PAYLOAD,
									HttpUtils.parseAcceptHeader(additionalHeaders.getAll(HttpHeaders.ACCEPT)) == 2));
						} catch (JsonLdError | ResponseException | IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						return remoteResult;
					}));
				}
				if (remoteResults.isEmpty()) {
					return Uni.createFrom().nullItem();
				}
				return Uni.combine().all().unis(remoteResults).combinedWith(remoteEntries -> {
					List<RemoteQueryResult> temp = Lists.newArrayList();
					remoteEntries.forEach(entry -> temp.add((RemoteQueryResult) entry));
					return temp;
				});
			});
		}
		return Uni.combine().all().unis(entryQuery, csourceQuery).combinedWith((entryResult, csourceResult) -> {
			if (csourceResult != null) {
				RemoteQueryResult queryResult = new RemoteQueryResult(null, ErrorType.None, -1, true);
				csourceResult.forEach(entry -> {
					entry.getId2Data().values().forEach(valueEntry -> {
						queryResult.addData(entry);
					});
					queryResult.setCount(queryResult.getCount() + entry.getCount());
				});
				try {
					entryResult = mergeStorage(queryResult, entryResult);
				} catch (IOException e) {
					logger.error("failed to merger remote storage", e);
				}
			}
			return entryResult;
		});
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
}