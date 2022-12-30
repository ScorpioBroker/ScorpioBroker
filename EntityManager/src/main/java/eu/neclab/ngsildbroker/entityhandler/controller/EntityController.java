package eu.neclab.ngsildbroker.entityhandler.controller;

import java.util.Map;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.DELETE;
import javax.ws.rs.PATCH;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.resteasy.reactive.RestResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLdProcessor;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.entityhandler.services.EntityService;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.vertx.core.http.HttpServerRequest;

/**
 * 
 * @version 1.0
 * @date 10-Jul-2018
 */
@Singleton
@Path("/ngsi-ld/v1")
public class EntityController {// implements EntityHandlerInterface {

	private final static Logger logger = LoggerFactory.getLogger(EntityController.class);

	@Inject
	EntityService entityService;

	@ConfigProperty(name = "ngsild.corecontext", defaultValue = "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld")
	String coreContext;

	@PostConstruct
	public void init() {
		JsonLdProcessor.init(coreContext);
	}

	/**
	 * Method(POST) for "/ngsi-ld/v1/entities/" rest endpoint.
	 * 
	 * @param payload jsonld message
	 * @return ResponseEntity object
	 */
	@Path("/entities")
	@POST
	public Uni<RestResponse<Object>> createEntity(HttpServerRequest req, String payload) {
		Tuple2<Context, Map<String, Object>> tuple;
		try {
			tuple = HttpUtils.expandBody(req, payload, AppConstants.ENTITY_CREATE_PAYLOAD);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
		}
		return entityService.createEntry(HttpUtils.getTenant(req), tuple.getItem2(), tuple.getItem1()).onItem()
				.transform(opResult -> {
					return HttpUtils.generateCreateResult(opResult, AppConstants.ENTITES_URL);
				}).onFailure().recoverWithItem(error -> {
					return HttpUtils.handleControllerExceptions(error);
				});
	}

	/**
	 * Method(PATCH) for "/ngsi-ld/v1/entities/{entityId}/attrs" rest endpoint.
	 * 
	 * @param entityId
	 * @param payload  json ld message
	 * @return ResponseEntity object
	 */

	@PATCH
	@Path("/entities/{entityId}/attrs")
	public Uni<RestResponse<Object>> updateEntity(HttpServerRequest request, @PathParam("entityId") String entityId,
			String payload) {
		Tuple2<Context, Map<String, Object>> tuple;
		try {
			tuple = HttpUtils.expandBody(request, payload, AppConstants.ENTITY_UPDATE_PAYLOAD);
			HttpUtils.validateUri(entityId);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
		}
		return entityService.updateEntry(HttpUtils.getTenant(request), entityId, tuple.getItem2(), tuple.getItem1())
				.onItem().transform(opResult -> {
					return HttpUtils.generateUpdateResultResponse(opResult);
				}).onFailure().recoverWithItem(error -> {
					return HttpUtils.handleControllerExceptions(error);
				});
	}

	/**
	 * Method(POST) for "/ngsi-ld/v1/entities/{entityId}/attrs" rest endpoint.
	 * 
	 * @param entityId
	 * @param payload  jsonld message
	 * @return ResponseEntity object
	 */

	@POST
	@Path("/entities/{entityId}/attrs")
	public Uni<RestResponse<Object>> appendEntity(HttpServerRequest request, @PathParam("entityId") String entityId,
			String payload, @QueryParam("options") String options) {
		Tuple2<Context, Map<String, Object>> tuple;
		try {
			tuple = HttpUtils.expandBody(request, payload, AppConstants.ENTITY_UPDATE_PAYLOAD);
			HttpUtils.validateUri(entityId);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
		}
		boolean noOverwrite = false;
		if (options != null && options.contains(NGSIConstants.NO_OVERWRITE_OPTION)) {
			noOverwrite = true;
		}
		return entityService
				.appendToEntry(HttpUtils.getTenant(request), entityId, tuple.getItem2(), noOverwrite, tuple.getItem1())
				.onItem().transform(opResult -> {
					return HttpUtils.generateUpdateResultResponse(opResult);
				}).onFailure().recoverWithItem(error -> {
					return HttpUtils.handleControllerExceptions(error);
				});
	}

	/**
	 * Method(PATCH) for "/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}" rest
	 * endpoint.
	 * 
	 * @param entityId
	 * @param attrId
	 * @param payload
	 * @return
	 */
	@SuppressWarnings({ "unchecked", "static-access" })
	@PATCH
	@Path("/entities/{entityId}/attrs/{attrId}")
	public Uni<RestResponse<Object>> partialUpdateEntity(HttpServerRequest request,
			@PathParam("entityId") String entityId, @PathParam("attrId") String attrib, String payload) {
		Tuple2<Context, Map<String, Object>> tuple;
		try {
			tuple = HttpUtils.expandBody(request, payload, AppConstants.ENTITY_ATTRS_UPDATE_PAYLOAD);
			HttpUtils.validateUri(entityId);
			attrib = tuple.getItem1().expandIri(attrib, false, false, null, null);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
		}
		logger.trace("update entry :: started");
		return entityService
				.partialUpdateEntity(HttpUtils.getTenant(request), entityId, attrib, tuple.getItem2(), tuple.getItem1())
				.onItem().transform(updateResult -> {
					logger.trace("update entry :: completed");
					return HttpUtils.generateUpdateResultResponse(updateResult);
				}).onFailure().recoverWithItem(error -> {
					return HttpUtils.handleControllerExceptions(error);
				});
	}

	/**
	 * Method(DELETE) for "/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}" rest
	 * endpoint.
	 * 
	 * @param entityId
	 * @param attrId
	 * @return
	 */

	@DELETE
	@Path("/entities/{entityId}/attrs/{attrId}")
	public Uni<RestResponse<Object>> deleteAttribute(HttpServerRequest request, @PathParam("entityId") String entityId,
			@PathParam("attrId") String attrId, @QueryParam("datasetId") String datasetId,
			@QueryParam("deleteAll") boolean deleteAll) {
		Context context;
		try {
			HttpUtils.validateUri(entityId);
			context = JsonLdProcessor.getCoreContextClone().parse(HttpUtils.getAtContextNoUni(request), false);
			attrId = context.expandIri(attrId, false, false, null, null);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
		}
		logger.trace("delete attribute :: started");
		return entityService
				.deleteAttribute(HttpUtils.getTenant(request), entityId, attrId, datasetId, deleteAll, context).onItem()
				.transform(opResult -> {
					logger.trace("delete attribute :: completed");
					return HttpUtils.generateDeleteResult(opResult);

				}).onFailure().recoverWithItem(error -> {
					return HttpUtils.handleControllerExceptions(error);
				});

	}

	/**
	 * Method(DELETE) for "/ngsi-ld/v1/entities/{entityId}" rest endpoint.
	 * 
	 * @param entityId
	 * @return
	 */
	@DELETE
	@Path("/entities/{entityId}")
	public Uni<RestResponse<Object>> deleteEntity(HttpServerRequest request, @PathParam("entityId") String entityId) {
		Context context;
		try {
			HttpUtils.validateUri(entityId);
			context = JsonLdProcessor.getCoreContextClone().parse(HttpUtils.getAtContextNoUni(request), true);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
		}
		return entityService.deleteEntry(HttpUtils.getTenant(request), entityId, context).onItem().transform(result -> {
			return HttpUtils.generateDeleteResult(result);
		}).onFailure().recoverWithItem(error -> {
			return HttpUtils.handleControllerExceptions(error);
		});
	}
}
