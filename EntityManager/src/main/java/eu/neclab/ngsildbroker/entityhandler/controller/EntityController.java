package eu.neclab.ngsildbroker.entityhandler.controller;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
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
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple;
import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.mutiny.tuples.Tuple3;
import io.smallrye.mutiny.unchecked.Unchecked;
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

	LocalDateTime startAt;
	LocalDateTime endAt;

	private JsonLdOptions opts = new JsonLdOptions(JsonLdOptions.JSON_LD_1_1);
	@ConfigProperty(name = "ngsild.corecontext", defaultValue = "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld")
	String coreContext;

	@PostConstruct
	public void init() {
		JsonLdProcessor.init(coreContext);
	}

	public EntityController() {
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

	@PATCH
	@Path("/{entityId}/attrs")
	public Uni<RestResponse<Object>> updateEntity(HttpServerRequest request, @PathParam("entityId") String entityId,
			String payload) {
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

	@POST
	@Path("/{entityId}/attrs")
	public Uni<RestResponse<Object>> appendEntity(HttpServerRequest request, @PathParam("entityId") String entityId,
			String payload, @QueryParam("options") String options) {
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
	@SuppressWarnings({ "unchecked", "static-access" })
	@PATCH
	@Path("/{entityId}/attrs/{attrId}")
	public Uni<RestResponse<Object>> partialUpdateEntity(HttpServerRequest request,
			@PathParam("entityId") String entityId, @PathParam("attrId") String attrId, String payload) {

		return Uni.combine().all().unis(HttpUtils.validateUri(entityId), HttpUtils.getAtContext(request))
				.combinedWith(Unchecked.function((entityIdUri, atContext) -> {
					boolean atContextAllowed = HttpUtils.doPreflightCheck(request, atContext);
					Object jsonPayload = JsonUtils.fromString(payload);
					logger.trace("partial-update entity :: started");
					Map<String, Object> expandedPayload = (Map<String, Object>) JsonLdProcessor.expand(atContext,
							jsonPayload, opts, AppConstants.ENTITY_ATTRS_UPDATE_PAYLOAD, atContextAllowed).get(0);
					Context context = JsonLdProcessor.getCoreContextClone();
					context = context.parse(atContext, true);
					if (jsonPayload instanceof Map) {
						Object payloadContext = ((Map<String, Object>) jsonPayload).get(JsonLdConsts.CONTEXT);
						if (payloadContext != null) {
							context = context.parse(payloadContext, true);
						}
					}
					String expandedAttrib = ParamsResolver.expandAttribute(attrId, context);
					return Tuple3.of(expandedPayload, expandedAttrib, context);
				})).onItem().transformToUni(t -> entityService.partialUpdateEntity(HttpUtils.getHeaders(request),
						entityId, t.getItem2(), t.getItem1()).onItem().transform(u -> Tuple2.of(u, t.getItem3())))
				.onItem().transform(Unchecked.function(t -> {
					if (t.getItem1().getNotUpdated().isEmpty()) {
						return RestResponse.noContent().ok();

					} else {
						throw new ResponseException(ErrorType.BadRequestData, JsonUtils
								.toPrettyString(JsonLdProcessor.compact(t.getItem1().getNotUpdated().get(0), t.getItem2(), opts)));

					}
				})).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);

	}

	/**
	 * Method(DELETE) for "/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}" rest
	 * endpoint.
	 * 
	 * @param entityId
	 * @param attrId
	 * @return
	 */

	@SuppressWarnings("static-access")
	@DELETE
	@Path("/{entityId}/attrs/{attrId}")
	public Uni<RestResponse<Object>> deleteAttribute(HttpServerRequest request, @PathParam("entityId") String entityId,
			@PathParam("attrId") String attrId, @QueryParam("datasetId") String datasetId,
			@QueryParam("deleteAll") String deleteAll) {
		try {
			HttpUtils.validateUri(entityId);
			logger.trace("delete attribute :: started");
			Context context = JsonLdProcessor.getCoreContextClone();
			context = context.parse(HttpUtils.getAtContext(request), true);
			String expandedAttrib = ParamsResolver.expandAttribute(attrId, context);
			entityService.deleteAttribute(HttpUtils.getHeaders(request), entityId, expandedAttrib, datasetId,
					deleteAll);
			logger.trace("delete attribute :: completed");
			return Uni.createFrom().item(RestResponse.noContent().ok());
		} catch (Exception exception) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(exception));
		}
	}

	/**
	 * Method(DELETE) for "/ngsi-ld/v1/entities/{entityId}" rest endpoint.
	 * 
	 * @param entityId
	 * @return
	 */
	@DELETE
	@Path("/{entityId}")
	public Uni<RestResponse<Object>> deleteEntity(HttpServerRequest request, @PathParam("entityId") String entityId) {
		return EntryControllerFunctions.deleteEntry(entityService, request, entityId, logger);
	}
}