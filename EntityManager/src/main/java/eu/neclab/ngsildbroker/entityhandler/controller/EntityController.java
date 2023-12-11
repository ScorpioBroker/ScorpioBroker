package eu.neclab.ngsildbroker.entityhandler.controller;

import static eu.neclab.ngsildbroker.commons.tools.EntityTools.noConcise;

import java.util.Map;

import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonObject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.resteasy.reactive.RestResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.jsonldjava.core.JsonLDService;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.entityhandler.services.EntityService;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpServerRequest;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.PATCH;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.QueryParam;
/**
 * 
 * @version 1.0
 * @date 10-Jul-2018
 */
@ApplicationScoped
@Path("/ngsi-ld/v1")
public class EntityController {// implements EntityHandlerInterface {

	private final static Logger logger = LoggerFactory.getLogger(EntityController.class);

	@Inject
	EntityService entityService;

	@ConfigProperty(name = "ngsild.corecontext", defaultValue = "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld")
	String coreContext;

	@Inject
	JsonLDService ldService;

	/**
	 * Method(POST) for "/ngsi-ld/v1/entities/" rest endpoint.
	 * 
	 * @param body jsonld message
	 * @return ResponseEntity object
	 */
	@Path("/entities")
	@POST
	public Uni<RestResponse<Object>> createEntity(HttpServerRequest req, String bodyStr) {
		Map<String,Object> body;
		try {
			body = new JsonObject(bodyStr).getMap();
		}catch (DecodeException e){
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
		}
		noConcise(body);
		return HttpUtils.expandBody(req, body, AppConstants.ENTITY_CREATE_PAYLOAD, ldService).onItem()
				.transformToUni(tuple -> {
					logger.debug("creating entity");
					return entityService.createEntity(HttpUtils.getTenant(req), tuple.getItem2(), tuple.getItem1())
							.onItem().transform(opResult -> {
								logger.debug("Done creating entity");
								return HttpUtils.generateCreateResult(opResult, AppConstants.ENTITES_URL);
							});
				}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);

	}

	/**
	 * Method(PATCH) for "/ngsi-ld/v1/entities/{entityId}/attrs" rest endpoint.
	 * 
	 * @param entityId
	 * @param body     json ld message
	 * @return ResponseEntity object
	 */

	@PATCH
	@Path("/entities/{entityId}/attrs")
	public Uni<RestResponse<Object>> updateEntity(HttpServerRequest req, @PathParam("entityId") String entityId,
			String bodyStr) {
		Map<String,Object> body;
		try {
			HttpUtils.validateUri(entityId);
			body = new JsonObject(bodyStr).getMap();
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
		}
		noConcise(body);
		return HttpUtils.expandBody(req, body, AppConstants.ENTITY_UPDATE_PAYLOAD, ldService).onItem()
				.transformToUni(tuple -> {
					logger.debug("patch attrs");
					return entityService
							.updateEntity(HttpUtils.getTenant(req), entityId, tuple.getItem2(), tuple.getItem1())
							.onItem().transform(HttpUtils::generateUpdateResultResponse);
				}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);
	}

	/**
	 * Method(POST) for "/ngsi-ld/v1/entities/{entityId}/attrs" rest endpoint.
	 * 
	 * @param entityId
	 * @param body     jsonld message
	 * @return ResponseEntity object
	 */

	@POST
	@Path("/entities/{entityId}/attrs")
	public Uni<RestResponse<Object>> appendEntity(HttpServerRequest req, @PathParam("entityId") String entityId,
			String bodyStr, @QueryParam("options") String options) {
		Map<String,Object> body;
		try {
			HttpUtils.validateUri(entityId);
			body = new JsonObject(bodyStr).getMap();
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
		}
		noConcise(body);
		boolean noOverwrite = options != null && options.contains(NGSIConstants.NO_OVERWRITE_OPTION);
		return HttpUtils.expandBody(req, body, AppConstants.ENTITY_UPDATE_PAYLOAD, ldService).onItem()
				.transformToUni(tuple -> {
					logger.debug("post attrs");
					return entityService.appendToEntity(HttpUtils.getTenant(req), entityId, tuple.getItem2(),
							noOverwrite, tuple.getItem1()).onItem().transform(HttpUtils::generateUpdateResultResponse);
				}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);

	}

	/**
	 * Method(PATCH) for "/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}" rest
	 * endpoint.
	 * 
	 * @param entityId
	 * @param body
	 * @return
	 */
	@PATCH
	@Path("/entities/{entityId}/attrs/{attrId}")
	public Uni<RestResponse<Object>> partialUpdateAttribute(HttpServerRequest req,
			@PathParam("entityId") String entityId, @PathParam("attrId") String attrib, String bodyStr) {
		Map<String,Object> body;
		try {
			HttpUtils.validateUri(entityId);
			body = new JsonObject(bodyStr).getMap();
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
		}
		noConcise(body);

		return HttpUtils.expandBody(req, body, AppConstants.ENTITY_UPDATE_PAYLOAD, ldService).onItem()
				.transformToUni(tuple -> {
					String expAttrib = tuple.getItem1().expandIri(attrib, false, true, null, null);
					logger.debug("update entry :: started");
					return entityService.partialUpdateAttribute(HttpUtils.getTenant(req), entityId, expAttrib,
							tuple.getItem2(), tuple.getItem1()).onItem().transform(updateResult -> {
								logger.trace("update entry :: completed");
								return HttpUtils.generateUpdateResultResponse(updateResult);
							});
				}).onFailure().recoverWithUni(t -> entityService.patchToEndPoint(entityId, req, body, attrib).onItem()
						.transform(isEndPointExist -> {
							if (isEndPointExist)
								return RestResponse.noContent();
							else {
								return HttpUtils.handleControllerExceptions(t);
							}
						}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions));
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
		try {
			HttpUtils.validateUri(entityId);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
		}
		return ldService.parse(HttpUtils.getAtContext(request)).onItem().transformToUni(context -> {
			String finalAttrId = context.expandIri(attrId, false, true, null, null);
			logger.trace("delete attribute :: started");
			return entityService
					.deleteAttribute(HttpUtils.getTenant(request), entityId, finalAttrId, datasetId, deleteAll, context)
					.onItem().transform(opResult -> {
						logger.trace("delete attribute :: completed");
						return HttpUtils.generateDeleteResult(opResult);

					});
		}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);

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
		try {
			HttpUtils.validateUri(entityId);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
		}
		return ldService.parse(HttpUtils.getAtContext(request)).onItem().transformToUni(context -> {
			return entityService.deleteEntity(HttpUtils.getTenant(request), entityId, context).onItem()
					.transform(HttpUtils::generateDeleteResult);
		}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);

	}

	
	@PATCH
	@Path("/entities/{entityId}")
	public Uni<RestResponse<Object>> mergePatch(HttpServerRequest request, @PathParam("entityId") String entityId,
			String bodyStr) {
		Map<String,Object> body;
		try {
			HttpUtils.validateUri(entityId);
			body = new JsonObject(bodyStr).getMap();
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
		}
		if (!entityId.equals(body.get(NGSIConstants.ID)) && body.get(NGSIConstants.ID) != null) {
			return 	Uni.createFrom().item(HttpUtils.handleControllerExceptions(
					new ResponseException(ErrorType.BadRequestData, "Id can not be updated")));
		}
		noConcise(body);
		return HttpUtils.expandBody(request, body, AppConstants.MERGE_PATCH_REQUEST, ldService).onItem()
				.transformToUni(tuple -> {
					return entityService
							.mergePatch(HttpUtils.getTenant(request), entityId, tuple.getItem2(), tuple.getItem1())
							.onItem().transform(HttpUtils::generateUpdateResultResponse);
				}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);

	}

	@Path("/entities/{entityId}")
	@PUT
	public Uni<RestResponse<Object>> replaceEntity(@PathParam("entityId") String entityId, HttpServerRequest request,
			String bodyStr) {
		logger.debug("replacing entity");
		Map<String,Object> body;
		try {
			HttpUtils.validateUri(entityId);
			body = new JsonObject(bodyStr).getMap();
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
		}
		noConcise(body);
		body.put(NGSIConstants.ID, entityId);
		if (!body.containsKey(NGSIConstants.TYPE)) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(
					new ResponseException(ErrorType.BadRequestData, "Type can not be null")));
		}
		return HttpUtils.expandBody(request, body, AppConstants.REPLACE_ENTITY_PAYLOAD, ldService).onItem()
				.transformToUni(tuple -> {

					return entityService.replaceEntity(HttpUtils.getTenant(request), tuple.getItem2(), tuple.getItem1())
							.onItem().transform(opResult -> {

								logger.debug("Done replacing entity");
								return HttpUtils.generateUpdateResultResponse(opResult);
							}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);
				});
	}

	@Path("/entities/{entityId}/attrs/{attrId}")
	@PUT
	public Uni<RestResponse<Object>> replaceAttribute(@PathParam("attrId") String attrId,
			@PathParam("entityId") String entityId, HttpServerRequest request, String bodyStr) {
		logger.debug("replacing Attrs");
		Map<String,Object> body;
		try {
			HttpUtils.validateUri(entityId);
			body = new JsonObject(bodyStr).getMap();
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
		}
		noConcise(body);
		return HttpUtils.expandBody(request, body, AppConstants.PARTIAL_UPDATE_REQUEST, ldService).onItem()
				.transformToUni(tuple -> {
					String finalAttrId = tuple.getItem1().expandIri(attrId, false, true, null, null);
					return entityService.replaceAttribute(HttpUtils.getTenant(request), tuple.getItem2(),
							tuple.getItem1(), entityId, finalAttrId).onItem().transform(opResult -> {
								logger.debug("Done replacing attribute");
								return HttpUtils.generateUpdateResultResponse(opResult);
							}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);
				});
	}
}
