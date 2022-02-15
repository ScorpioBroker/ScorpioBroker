package eu.neclab.ngsildbroker.entityhandler.controller;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.resteasy.reactive.RestResponse;

import com.github.jsonldjava.core.JsonLdProcessor;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.controllers.EntryControllerFunctions;
import eu.neclab.ngsildbroker.entityhandler.services.EntityService;
import io.vertx.core.http.HttpServerRequest;

@Path("/ngsi-ld/v1/entityOperations")
public class EntityBatchController {

	@Inject
	EntityService entityService;

	@ConfigProperty(name = "batchoperations.maxnumber.create", defaultValue = "-1")
	int maxCreateBatch;

	@ConfigProperty(name = "batchoperations.maxnumber.update", defaultValue = "-1")
	int maxUpdateBatch;

	@ConfigProperty(name = "batchoperations.maxnumber.upsert", defaultValue = "-1")
	int maxUpsertBatch;

	@ConfigProperty(name = "batchoperations.maxnumber.delete", defaultValue = "-1")
	int maxDeleteBatch;

	@ConfigProperty(name = "ngsild.corecontext", defaultValue = "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld")
	String coreContext;

	@PostConstruct
	void init() {
		JsonLdProcessor.init(coreContext);
	}

	@Path("/create")
	@POST
	public RestResponse<Object> createMultiple(HttpServerRequest request, String payload) {
		return EntryControllerFunctions.createMultiple(entityService, request, payload, maxCreateBatch,
				AppConstants.ENTITY_CREATE_PAYLOAD);
	}

	@Path("/upsert")
	@POST
	public RestResponse<Object> upsertMultiple(HttpServerRequest request, String payload,
			@QueryParam(value = "options") String options) {
		return EntryControllerFunctions.upsertMultiple(entityService, request, payload, options, maxCreateBatch,
				AppConstants.ENTITY_CREATE_PAYLOAD);
	}

	@Path("/update")
	@POST
	public RestResponse<Object> updateMultiple(HttpServerRequest request, String payload,
			@QueryParam(value = "options") String options) {
		return EntryControllerFunctions.updateMultiple(entityService, request, payload, maxUpdateBatch, options,
				AppConstants.ENTITY_UPDATE_PAYLOAD);
	}

	@Path("/delete")
	@POST
	public RestResponse<Object> deleteMultiple(HttpServerRequest request, String payload) {
		return EntryControllerFunctions.deleteMultiple(entityService, request, payload,
				AppConstants.ENTITY_CREATE_PAYLOAD);
	}

}
