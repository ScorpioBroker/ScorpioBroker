package eu.neclab.ngsildbroker.entityhandler.controller;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.PATCH;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.ResourceContext;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.resteasy.reactive.RestResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import io.quarkus.vertx.web.Body;
import io.quarkus.vertx.web.Route;
import io.quarkus.vertx.web.Route.HttpMethod;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;

/**
 * 
 * @version 1.0
 * @date 10-Jul-2018
 */

@Path("/ngsi-ld/v1/entities")
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
	public void init() {
		JsonLdProcessor.init(coreContext);
	}

	/**
	 * Method(POST) for "/ngsi-ld/v1/entities/" rest endpoint.
	 * 
	 * @param payload jsonld message
	 * @return ResponseEntity object
	 */
	@Path("")
	@POST
	public RestResponse<Object> createEntity(HttpServerRequest req, String payload) {
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

	@Path("/{entityId}/attrs")
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

	@Path("/{entityId}/attrs")
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
	@Path("/{entityId}/attrs/{attrId}")
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
				return ResponseEntity.noContent().build();
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

	@DeleteMapping("/{entityId}/attrs/{attrId}")
	public ResponseEntity<String> deleteAttribute(HttpServletRequest request, @PathVariable("entityId") String entityId,
			@PathVariable("attrId") String attrId,
			@RequestParam(value = "datasetId", required = false) String datasetId,
			@RequestParam(value = "deleteAll", required = false) String deleteAll) {
		try {
			HttpUtils.validateUri(entityId);
			logger.trace("delete attribute :: started");
			Context context = JsonLdProcessor.getCoreContextClone();
			context = context.parse(HttpUtils.getAtContext(request), true);
			String expandedAttrib = ParamsResolver.expandAttribute(attrId, context);
			entityService.deleteAttribute(HttpUtils.getHeaders(request), entityId, expandedAttrib, datasetId,
					deleteAll);
			logger.trace("delete attribute :: completed");
			return ResponseEntity.noContent().build();
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
	@DeleteMapping("/{entityId}")
	public ResponseEntity<String> deleteEntity(HttpServletRequest request, @PathVariable("entityId") String entityId) {
		return EntryControllerFunctions.deleteEntry(entityService, request, entityId, logger);
	}
}
