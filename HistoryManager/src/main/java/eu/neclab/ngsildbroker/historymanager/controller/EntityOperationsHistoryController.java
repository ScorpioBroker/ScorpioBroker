package eu.neclab.ngsildbroker.historymanager.controller;

import java.util.List;
import java.util.Random;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.resteasy.reactive.RestResponse;
import com.github.jsonldjava.core.JsonLdProcessor;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.controllers.EntryControllerFunctions;
import eu.neclab.ngsildbroker.commons.controllers.QueryControllerFunctions;
import eu.neclab.ngsildbroker.commons.interfaces.PayloadQueryParamParser;
import eu.neclab.ngsildbroker.historymanager.service.HistoryPostQueryParser;
import eu.neclab.ngsildbroker.historymanager.service.HistoryService;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpServerRequest;

@Singleton
@Path("/ngsi-ld/v1/temporal/entityOperations")
public class EntityOperationsHistoryController {

	@Inject
	HistoryService entityService;

	@ConfigProperty(name = "scorpio.history.batch-operations.create.max", defaultValue = "-1")
	int maxCreateBatch;
	@ConfigProperty(name = "scorpio.history.batch-operations.update.max", defaultValue = "-1")
	int maxUpdateBatch;
	@ConfigProperty(name = "scorpio.history.batch-operations.upsert.max", defaultValue = "-1")
	int maxUpsertBatch;
	@ConfigProperty(name = "scorpio.history.batch-operations.delete.max", defaultValue = "-1")
	int maxDeleteBatch;
	@ConfigProperty(name = "scorpio.history.default-limit", defaultValue = "50")
	int defaultLimit;
	@ConfigProperty(name = "scorpio.history.max-limit", defaultValue = "1000")
	int maxLimit;

	@ConfigProperty(name = "ngsild.corecontext", defaultValue = "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld")
	String coreContext;

	private PayloadQueryParamParser paramParser = new HistoryPostQueryParser();

	Random random = new Random();

	@PostConstruct
	public void init() {
		JsonLdProcessor.init(coreContext);
	}

	@Path("/create")
	@POST
	public Uni<RestResponse<Object>> createMultiple(HttpServerRequest request, String payload) {
		return EntryControllerFunctions.createMultiple(entityService, request, payload, maxCreateBatch,
				AppConstants.TEMP_ENTITY_CREATE_PAYLOAD, random);
	}

	@Path("/upsert")
	@POST
	public Uni<RestResponse<Object>> upsertMultiple(HttpServerRequest request, String payload,
			@QueryParam(value = "options") String options) {
		return EntryControllerFunctions.upsertMultiple(entityService, request, payload, options, maxCreateBatch,
				AppConstants.TEMP_ENTITY_CREATE_PAYLOAD, random);
	}

	@Path("/update")
	@POST
	public Uni<RestResponse<Object>> updateMultiple(HttpServerRequest request, String payload,
			@QueryParam(value = "options") String options) {
		return EntryControllerFunctions.updateMultiple(entityService, request, payload, maxUpdateBatch, options,
				AppConstants.TEMP_ENTITY_UPDATE_PAYLOAD, random);
	}

	@Path("/delete")
	@POST
	public Uni<RestResponse<Object>> deleteMultiple(HttpServerRequest request, String payload) {
		return EntryControllerFunctions.deleteMultiple(entityService, request, payload,
				AppConstants.TEMP_ENTITY_CREATE_PAYLOAD, random);
	}

	@Path("/query")
	@POST
	public Uni<RestResponse<Object>> postQuery(HttpServerRequest request, String payload,
			@QueryParam(value = "limit") Integer limit, @QueryParam(value = "offset") Integer offset,
			@QueryParam(value = "qtoken") String qToken, @QueryParam(value = "options") List<String> options,
			@QueryParam(value = "count") boolean count) {

		return QueryControllerFunctions.postQuery(entityService, request, payload, limit, offset, qToken, options,
				count, defaultLimit, maxLimit, AppConstants.QUERY_PAYLOAD, paramParser);
	}

}
