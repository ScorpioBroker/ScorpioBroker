package eu.neclab.ngsildbroker.queryhandler.controller;

import java.util.List;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.resteasy.reactive.RestResponse;
import com.github.jsonldjava.core.JsonLdProcessor;
import eu.neclab.ngsildbroker.commons.controllers.QueryControllerFunctions;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.queryhandler.services.QueryService;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpServerRequest;

@Singleton
@Path("/ngsi-ld/v1")
public class QueryController {

	@Inject
	QueryService queryService;

	@ConfigProperty(name = "scorpio.entity.default-limit", defaultValue = "50")
	int defaultLimit;

	@ConfigProperty(name = "scorpio.entity.max-limit", defaultValue = "1000")
	int maxLimit;

	@ConfigProperty(name = "ngsild.corecontext", defaultValue = "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld")
	String coreContext;

	@PostConstruct
	public void init() {
		JsonLdProcessor.init(coreContext);
	}

	/**
	 * Method(GET) for multiple attributes separated by comma list
	 * 
	 * @param request
	 * @param entityId
	 * @param attrs
	 * @return
	 * @throws ResponseException
	 */
	@Path("/entities/{entityId}")
	@GET
	public Uni<RestResponse<Object>> getEntity(HttpServerRequest request,
			@QueryParam(value = "attrs") List<String> attrs, @QueryParam(value = "options") List<String> options,
			@PathParam("entityId") String entityId) {
		return QueryControllerFunctions.getEntity(queryService, request, attrs, options, entityId, false, defaultLimit,
				maxLimit);
	}

	/**
	 * Method(GET) for fetching all entities by kafka and other geo query operation
	 * by database
	 * 
	 * @param request
	 * @param type
	 * @return ResponseEntity object
	 */
	@Path("/entities")
	@GET
	public Uni<RestResponse<Object>> queryForEntities(HttpServerRequest request) {
		return QueryControllerFunctions.queryForEntries(queryService, request, false, defaultLimit, maxLimit, true);
	}

	@Path("/types")
	@GET
	public Uni<RestResponse<Object>> getAllTypes(HttpServerRequest request,
			@QueryParam(value = "details") boolean details) {
		return QueryControllerFunctions.getAllTypes(queryService, request, details, false, defaultLimit, maxLimit);
	}

	@Path("/types/{entityType}")
	@GET
	public Uni<RestResponse<Object>> getType(HttpServerRequest request, @PathParam("entityType") String type,
			@QueryParam(value = "details") boolean details) {
		return QueryControllerFunctions.getType(queryService, request, type, details, false, defaultLimit, maxLimit);
	}

	@Path("/attributes")
	@GET
	public Uni<RestResponse<Object>> getAllAttributes(HttpServerRequest request,
			@QueryParam(value = "details") boolean details) {

		return QueryControllerFunctions.getAllAttributes(queryService, request, details, false, defaultLimit, maxLimit);
	}

	@Path("/attributes/{attribute}")
	@GET
	public Uni<RestResponse<Object>> getAttribute(HttpServerRequest request, @PathParam("attribute") String attribute,
			@QueryParam(value = "details") boolean details) {
		return QueryControllerFunctions.getAttribute(queryService, request, attribute, details, false, defaultLimit,
				maxLimit);
	}

}