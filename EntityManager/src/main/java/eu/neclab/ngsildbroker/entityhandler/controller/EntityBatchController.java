package eu.neclab.ngsildbroker.entityhandler.controller;

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

import eu.neclab.ngsildbroker.entityhandler.services.EntityService;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpServerRequest;

@Singleton
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

	@ConfigProperty(name = "ngsild.corecontext", defaultValue = "ngsild.corecontext:https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld")
	String coreContext;

	Random random = new Random();

	@PostConstruct
	public void init() {
		JsonLdProcessor.init(coreContext);
	}

	@POST
	@Path("/create")
	public Uni<RestResponse<Object>> createMultiple(HttpServerRequest request, String payload) {
		return null;
	}

	@POST
	@Path("/upsert")
	public Uni<RestResponse<Object>> upsertMultiple(HttpServerRequest request, String payload,
			@QueryParam(value = "options") String options) {
		return null;
	}

	@POST
	@Path("/update")
	public Uni<RestResponse<Object>> updateMultiple(HttpServerRequest request, String payload,
			@QueryParam(value = "options") String options) {
		return null;
	}

	@POST
	@Path("/delete")
	public Uni<RestResponse<Object>> deleteMultiple(HttpServerRequest request, String payload) {
		return null;
	}

}
