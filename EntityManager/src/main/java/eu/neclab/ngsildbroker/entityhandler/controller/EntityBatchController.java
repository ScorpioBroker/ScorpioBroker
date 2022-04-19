package eu.neclab.ngsildbroker.entityhandler.controller;

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
import eu.neclab.ngsildbroker.entityhandler.services.EntityService;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpServerRequest;

@Singleton
@Path("/ngsi-ld/v1/entityOperations")
public class EntityBatchController {

	@Inject
	EntityService entityService;

	@ConfigProperty( name = "",defaultValue = "${batchoperations.maxnumber.create:-1}")
	int maxCreateBatch;
	@ConfigProperty( name = "",defaultValue = "${batchoperations.maxnumber.update:-1}")
	int maxUpdateBatch;
	@ConfigProperty( name = "",defaultValue = "${batchoperations.maxnumber.upsert:-1}")
	int maxUpsertBatch;
	@ConfigProperty( name = "",defaultValue = "${batchoperations.maxnumber.delete:-1}")
	int maxDeleteBatch;

	@ConfigProperty(name = "ngsild.corecontext", defaultValue = "ngsild.corecontext:https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld")
	String coreContext;

	@PostConstruct
	public void init() {
		JsonLdProcessor.init(coreContext);
	}

	
	@POST
	@Path("/create")
	public Uni<RestResponse<Object>> createMultiple(HttpServerRequest request, String payload) {
		return EntryControllerFunctions.createMultiple(entityService, request, payload, maxCreateBatch,
				AppConstants.ENTITY_CREATE_PAYLOAD);
	}

	@POST
	@Path("/upsert")
	public Uni<RestResponse<Object>> upsertMultiple(HttpServerRequest request, String payload,
			@QueryParam("options") String options) {
		return EntryControllerFunctions.upsertMultiple(entityService, request, payload, options, maxCreateBatch,
				AppConstants.ENTITY_CREATE_PAYLOAD);
	}

	@POST
	@Path("/update")
	public Uni<RestResponse<Object>> updateMultiple(HttpServerRequest request, String payload,
			@QueryParam("options") String options) {
		return EntryControllerFunctions.updateMultiple(entityService, request, payload, maxUpdateBatch, options,
				AppConstants.ENTITY_UPDATE_PAYLOAD);
	}

	@POST
	@Path("/delete")
	public Uni<RestResponse<Object>> deleteMultiple(HttpServerRequest request, String payload) {
		return EntryControllerFunctions.deleteMultiple(entityService, request, payload, AppConstants.ENTITY_CREATE_PAYLOAD);
	}

}
