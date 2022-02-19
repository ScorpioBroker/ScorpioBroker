package eu.neclab.ngsildbroker.entityhandler.controller;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.DELETE;
import javax.ws.rs.PATCH;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.resteasy.reactive.RestResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLdConsts;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.controllers.EntryControllerFunctions;
import eu.neclab.ngsildbroker.commons.datatypes.results.UpdateResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.ngsiqueries.ParamsResolver;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.entityhandler.services.EntityService;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpServerRequest;

/**
 * 
 * @version 1.0
 * @date 10-Jul-2018
 */

@Path("/ngsi-ld/v1")
public class EntityController {// implements EntityHandlerInterface {

	private final static Logger logger = LoggerFactory.getLogger(EntityController.class);

	@Inject
	EntityService entityService;

	LocalDateTime startAt;
	LocalDateTime endAt;

	private JsonLdOptions opts = new JsonLdOptions(JsonLdOptions.JSON_LD_1_1);
	@ConfigProperty(name = "ngsild.corecontext", defaultValue = "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld")
	String coreContext;

	@PostConstruct
	void init() {
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
		return EntryControllerFunctions.createEntry(entityService, req, payload, AppConstants.ENTITY_CREATE_PAYLOAD,
				AppConstants.ENTITES_URL, logger);
	}

	/**
	 * Method(PATCH) for "/ngsi-ld/v1/entities/{entityId}/attrs" rest endpoint.
	 * 
	 * @param entityId
	 * @param payload  json ld message
	 * @return ResponseEntity object
	 */

	@Path("/entities/{entityId}/attrs")
	@PATCH
	public RestResponse<Object> updateEntity(HttpServerRequest request, String entityId, String payload) {
		return EntryControllerFunctions.updateEntry(entityService, request, entityId, payload,
				AppConstants.ENTITY_UPDATE_PAYLOAD, logger);
	}

	/**
	 * Method(POST) for "/ngsi-ld/v1/entities/{entityId}/attrs" rest endpoint.
	 * 
	 * @param entityId
	 * @param payload  jsonld message
	 * @return ResponseEntity object
	 */

	@Path("/entities/{entityId}/attrs")
	@POST
	public RestResponse<Object> appendEntity(HttpServerRequest request, String entityId, String payload,
			@QueryParam(value = "options") String options) {
		return EntryControllerFunctions.appendToEntry(entityService, request, entityId, payload, options,
				AppConstants.ENTITY_UPDATE_PAYLOAD, logger);
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
	@SuppressWarnings("unchecked")
	@Path("/entities/{entityId}/attrs/{attrId}")
	@PATCH
	public RestResponse<Object> partialUpdateEntity(HttpServerRequest request, String entityId, String attrId,
			String payload) {
		try {
			Object jsonPayload = JsonUtils.fromString(payload);
			HttpUtils.validateUri(entityId);
			List<Object> atContext = HttpUtils.getAtContext(request);
			boolean atContextAllowed = HttpUtils.doPreflightCheck(request, atContext);
			logger.trace("partial-update entity :: started");
			Map<String, Object> expandedPayload = (Map<String, Object>) JsonLdProcessor
					.expand(atContext, jsonPayload, opts, AppConstants.ENTITY_ATTRS_UPDATE_PAYLOAD, atContextAllowed)
					.get(0);
			Context context = JsonLdProcessor.getCoreContextClone();
			context = context.parse(atContext, true);
			if (jsonPayload instanceof Map) {
				Object payloadContext = ((Map<String, Object>) jsonPayload).get(JsonLdConsts.CONTEXT);
				if (payloadContext != null) {
					context = context.parse(payloadContext, true);
				}
			}
			String expandedAttrib = ParamsResolver.expandAttribute(attrId, context);

			UpdateResult update = entityService.partialUpdateEntity(HttpUtils.getHeaders(request), entityId,
					expandedAttrib, expandedPayload);
			logger.trace("partial-update entity :: completed");
			if (update.getNotUpdated().isEmpty()) {
				return RestResponse.noContent();
			} else {
				return HttpUtils.handleControllerExceptions(new ResponseException(ErrorType.BadRequestData, JsonUtils
						.toPrettyString(JsonLdProcessor.compact(update.getNotUpdated().get(0), context, opts))));
			}
			/*
			 * There is no 207 multi status response in the Partial Attribute Update
			 * operation. Section 6.7.3.1 else { return
			 * ResponseEntity.status(HttpStatus.MULTI_STATUS).body(update.
			 * getAppendedJsonFields()); }
			 */
		} catch (Exception exception) {
			return HttpUtils.handleControllerExceptions(exception);
		}
	}

	/**
	 * Method(DELETE) for "/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}" rest
	 * endpoint.
	 * 
	 * @param entityId
	 * @param attrId
	 * @return
	 */

	@Path("/entities/{entityId}/attrs/{attrId}")
	@DELETE
	public RestResponse<Object> deleteAttribute(HttpServerRequest request, String entityId, String attrId,
			@QueryParam(value = "datasetId") String datasetId, @QueryParam(value = "deleteAll") String deleteAll) {
		try {
			HttpUtils.validateUri(entityId);
			logger.trace("delete attribute :: started");
			Context context = JsonLdProcessor.getCoreContextClone();
			context = context.parse(HttpUtils.getAtContext(request), true);
			String expandedAttrib = ParamsResolver.expandAttribute(attrId, context);
			entityService.deleteAttribute(HttpUtils.getHeaders(request), entityId, expandedAttrib, datasetId,
					deleteAll);
			logger.trace("delete attribute :: completed");
			return RestResponse.noContent();
		} catch (Exception exception) {
			return HttpUtils.handleControllerExceptions(exception);
		}
	}

	/**
	 * Method(DELETE) for "/ngsi-ld/v1/entities/{entityId}" rest endpoint.
	 * 
	 * @param entityId
	 * @return
	 */
	@Path("/entities/{entityId}")
	@DELETE
	public RestResponse<Object> deleteEntity(HttpServerRequest request, String entityId) {
		return EntryControllerFunctions.deleteEntry(entityService, request, entityId, logger);
	}
}
