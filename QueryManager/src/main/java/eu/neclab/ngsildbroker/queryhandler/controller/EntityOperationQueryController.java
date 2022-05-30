package eu.neclab.ngsildbroker.queryhandler.controller;

import java.util.List;

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
import eu.neclab.ngsildbroker.commons.controllers.QueryControllerFunctions;
import eu.neclab.ngsildbroker.commons.interfaces.PayloadQueryParamParser;
import eu.neclab.ngsildbroker.queryhandler.services.EntityPostQueryParser;
import eu.neclab.ngsildbroker.queryhandler.services.QueryService;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpServerRequest;

@Singleton
@Path("/ngsi-ld/v1/entityOperations")
public class EntityOperationQueryController {

	@Inject
	QueryService queryService;

	@ConfigProperty(name = "scorpio.entity.default-limit", defaultValue = "50")
	int defaultLimit;

	@ConfigProperty(name = "scorpio.entity.batch-operations.query.max-limit", defaultValue = "1000")
	int maxLimit;

	@ConfigProperty(name = "scorpio.entity.batch-operations.query.max-limit", defaultValue = "1000")
	String coreContext;

	private PayloadQueryParamParser paramParser = new EntityPostQueryParser();

	@PostConstruct
	public void init() {
		JsonLdProcessor.init(coreContext);
	}

	@Path("/query")
	@POST
	public Uni<RestResponse<Object>> postQuery(HttpServerRequest request, String payload,
			@QueryParam(value = "limit") Integer limit, @QueryParam(value = "offset") Integer offset,
			@QueryParam(value = "qtoken") String qToken, @QueryParam(value = "options") List<String> options,
			@QueryParam(value = "count") boolean count) {

		return QueryControllerFunctions.postQuery(queryService, request, payload, limit, offset, qToken, options, count,
				defaultLimit, maxLimit, AppConstants.QUERY_PAYLOAD, paramParser);
	}
}
