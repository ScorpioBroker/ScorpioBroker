package eu.neclab.ngsildbroker.entityhandler.controller;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.PATCH;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import com.github.jsonldjava.utils.JsonUtils;
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
@ApplicationScoped
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
			Map<String, Object> body = (Map<String, Object>) JsonUtils.fromString(payload);
			noConcise(body);
			tuple = HttpUtils.expandBody(req, body, AppConstants.ENTITY_CREATE_PAYLOAD);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
		}
		logger.info("creating entity");
		return entityService.createEntity(HttpUtils.getTenant(req), tuple.getItem2(), tuple.getItem1()).onItem()
				.transform(opResult -> {
					logger.info("Done creating entity");
					return HttpUtils.generateCreateResult(opResult, AppConstants.ENTITES_URL);
				}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);
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
			Map<String, Object> body = (Map<String, Object>) JsonUtils.fromString(payload);
			noConcise(body);
			tuple = HttpUtils.expandBody(request, body, AppConstants.ENTITY_UPDATE_PAYLOAD);
			HttpUtils.validateUri(entityId);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
		}
		return entityService.updateEntity(HttpUtils.getTenant(request), entityId, tuple.getItem2(), tuple.getItem1())
				.onItem().transform(HttpUtils::generateUpdateResultResponse).onFailure()
				.recoverWithItem(HttpUtils::handleControllerExceptions);
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
			Map<String, Object> body = (Map<String, Object>) JsonUtils.fromString(payload);
			noConcise(body);
			tuple = HttpUtils.expandBody(request, body, AppConstants.ENTITY_UPDATE_PAYLOAD);
			HttpUtils.validateUri(entityId);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
		}
		boolean noOverwrite = options != null && options.contains(NGSIConstants.NO_OVERWRITE_OPTION);
		return entityService
				.appendToEntity(HttpUtils.getTenant(request), entityId, tuple.getItem2(), noOverwrite, tuple.getItem1())
				.onItem().transform(HttpUtils::generateUpdateResultResponse).onFailure()
				.recoverWithItem(HttpUtils::handleControllerExceptions);
	}

	/**
	 * Method(PATCH) for "/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}" rest
	 * endpoint.
	 * 
	 * @param entityId
	 * @param payload
	 * @return
	 */
	@PATCH
	@Path("/entities/{entityId}/attrs/{attrId}")
	public Uni<RestResponse<Object>> partialUpdateAttribute(HttpServerRequest request,
			@PathParam("entityId") String entityId, @PathParam("attrId") String attrib, String payload) {
		Tuple2<Context, Map<String, Object>> tuple;
		String expAttrib;
		try {
			Map<String, Object> body = (Map<String, Object>) JsonUtils.fromString(payload);
			noConcise(body);
			tuple = HttpUtils.expandBody(request, body, AppConstants.ENTITY_ATTRS_UPDATE_PAYLOAD);
			HttpUtils.validateUri(entityId);
			expAttrib = tuple.getItem1().expandIri(attrib, false, true, null, null);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
		}
		logger.trace("update entry :: started");
		return entityService.partialUpdateAttribute(HttpUtils.getTenant(request), entityId, expAttrib, tuple.getItem2(),
				tuple.getItem1()).onItem().transform(updateResult -> {
					logger.trace("update entry :: completed");
					return HttpUtils.generateUpdateResultResponse(updateResult);
				}).onFailure().recoverWithItem(e -> {
					return HttpUtils.handleControllerExceptions(e);
				});

//				.onFailure().recoverWithUni(t -> entityService.patchToEndPoint(entityId, request, payload, attrib)
//						.onItem().transform(isEndPointExist -> {
//							if (isEndPointExist)
//								return RestResponse.noContent();
//							else {
//								return HttpUtils.handleControllerExceptions(t);
//							}
//						}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions));
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
			context = JsonLdProcessor.getCoreContextClone().parse(HttpUtils.getAtContext(request), false);
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
		Context context;
		try {
			HttpUtils.validateUri(entityId);
			context = JsonLdProcessor.getCoreContextClone().parse(HttpUtils.getAtContext(request), true);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
		}
		return entityService.deleteEntity(HttpUtils.getTenant(request), entityId, context).onItem()
				.transform(HttpUtils::generateDeleteResult).onFailure()
				.recoverWithItem(HttpUtils::handleControllerExceptions);
	}

	public void noConcise(Object object) {
		noConcise(object, null, null);
	}

	private void noConcise(Object object, Map<String, Object> parentMap, String keyOfObject) {
		// Object is Map
		if (object instanceof Map<?, ?> map) {
			// Map have object but not type
			if (map.containsKey(NGSIConstants.OBJECT)) {
				((Map<String, Object>) map).put(NGSIConstants.TYPE, NGSIConstants.RELATIONSHIP);

			}
			// Map have value but not type
			if (map.containsKey(NGSIConstants.VALUE) && !map.containsKey(NGSIConstants.TYPE)) {
				// for GeoProperty
				if (map.get(NGSIConstants.VALUE) instanceof Map<?, ?> nestedMap
						&& (NGSIConstants.GEO_KEYWORDS.contains(nestedMap.get(NGSIConstants.TYPE))))
					((Map<String, Object>) map).put(NGSIConstants.TYPE, NGSIConstants.NGSI_LD_GEOPROPERTY_SHORT);
				else
					((Map<String, Object>) map).put(NGSIConstants.TYPE, NGSIConstants.PROPERTY);

			}
			// for GeoProperty
			if (map.containsKey(NGSIConstants.TYPE)
					&& (NGSIConstants.GEO_KEYWORDS.contains(map.get(NGSIConstants.TYPE)))
					&& !keyOfObject.equals(NGSIConstants.VALUE)) {
				Map<String, Object> newMap = new HashMap<>();
				newMap.put(NGSIConstants.TYPE, NGSIConstants.NGSI_LD_GEOPROPERTY_SHORT);
				newMap.put(NGSIConstants.VALUE, map);
				parentMap.put(keyOfObject, newMap);

			}

			// Iterate through every element of Map
			Object[] mapKeys = map.keySet().toArray();
			for (Object key : mapKeys) {
				if (!key.equals(NGSIConstants.ID) && !key.equals(NGSIConstants.TYPE)
						&& !key.equals(NGSIConstants.JSON_LD_CONTEXT)
						&& !key.equals(NGSIConstants.QUERY_PARAMETER_COORDINATES)
						&& !key.equals(NGSIConstants.QUERY_PARAMETER_OBSERVED_AT)
						&& !key.equals(NGSIConstants.INSTANCE_ID)
						&& !key.equals(NGSIConstants.QUERY_PARAMETER_DATA_SET_ID) && !key.equals(NGSIConstants.OBJECT)
						&& !key.equals(NGSIConstants.VALUE) && !key.equals(NGSIConstants.SCOPE)
						&& !key.equals(NGSIConstants.QUERY_PARAMETER_UNIT_CODE)) {
					noConcise(map.get(key), (Map<String, Object>) map, key.toString());
				}
			}
		}
		// Object is List
		else if (object instanceof List<?> list) {
			for (int i = 0; i < list.size(); i++) {
				noConcise(list.get(i), null, null);
			}
		}
		// Object is String or Number value
		else if ((object instanceof String || object instanceof Number) && parentMap != null) {
			// if keyofobject is value then just need to convert double to int if possible
			if (keyOfObject != null && keyOfObject.equals(NGSIConstants.VALUE)) {
				parentMap.put(keyOfObject, HttpUtils.doubleToInt(object));
			} else {
				Map<String, Object> newMap = new HashMap<>();
				newMap.put(NGSIConstants.VALUE, HttpUtils.doubleToInt(object));
				newMap.put(NGSIConstants.TYPE, NGSIConstants.PROPERTY);
				parentMap.put(keyOfObject, newMap);
			}

		}
	}
}
