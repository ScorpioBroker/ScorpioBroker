package eu.neclab.ngsildbroker.entityhandler.controller;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonObject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.resteasy.reactive.RestResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.jsonldjava.core.JsonLDService;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.net.HttpHeaders;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.ViaHeaders;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
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

	@ConfigProperty(name = "scorpio.ngsild.corecontext")
	String coreContext;

	@Inject
	JsonLDService ldService;

	@Inject
	MicroServiceUtils microServiceUtils;

	/**
	 * Method(POST) for "/ngsi-ld/v1/entities/" rest endpoint.
	 * 
	 * @param body jsonld message
	 * @return ResponseEntity object
	 */
	@Path("/entities")
	@POST
	public Uni<RestResponse<Object>> createEntity(HttpServerRequest req, String bodyStr) {

		Map<String, Object> body;
		String tenant = HttpUtils.getTenant(req);
		try {
			body = new JsonObject(bodyStr).getMap();
		} catch (DecodeException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(req)));
		}
		ViaHeaders viaHeaders;
		try {
			viaHeaders = new ViaHeaders(req.headers().getAll(HttpHeaders.VIA),
					microServiceUtils.getSourceAlias(tenant));
		} catch (ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, tenant));
		}
		// try {
		// noConcise(body);
		// } catch (ResponseException e) {
		// return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e,
		// HttpUtils.getTenant(req)));
		// }
		return HttpUtils.expandBody(req, body, AppConstants.ENTITY_CREATE_PAYLOAD, ldService).onItem()
				.transformToUni(tuple -> {
					logger.debug("creating entity");

					return entityService
							.createEntity(tenant, tuple.getItem2(), tuple.getItem1(), req.headers(), viaHeaders)
							.onItem().transform(opResult -> {
								logger.debug("Done creating entity");
								return HttpUtils.generateCreateResult(opResult, AppConstants.ENTITES_URL);
							});
				}).onFailure().recoverWithItem(e -> {
					return HttpUtils.handleControllerExceptions(e, tenant);
				});

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
		Map<String, Object> body;
		String tenant = HttpUtils.getTenant(req);
		try {
			HttpUtils.validateUri(entityId);
			body = new JsonObject(bodyStr).getMap();
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(req)));
		}
		ViaHeaders viaHeaders;
		try {
			viaHeaders = new ViaHeaders(req.headers().getAll(HttpHeaders.VIA),
					microServiceUtils.getSourceAlias(tenant));
		} catch (ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, tenant));
		}
		// try {
		// noConcise(body);
		// } catch (ResponseException e) {
		// return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e,
		// HttpUtils.getTenant(req)));
		// }

		return HttpUtils.expandBody(req, body, AppConstants.ENTITY_UPDATE_PAYLOAD, ldService).onItem()
				.transformToUni(tuple -> {

					logger.debug("patch attrs");
					return entityService.updateEntity(HttpUtils.getTenant(req), entityId, tuple.getItem2(),
							tuple.getItem1(), req.headers(), viaHeaders).onItem()
							.transform(HttpUtils::generateUpdateResultResponse);
				}).onFailure().recoverWithItem(e -> {
					return HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(req));
				});
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
		Map<String, Object> body;
		try {
			HttpUtils.validateUri(entityId);
			body = new JsonObject(bodyStr).getMap();
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(req)));
		}
		String tenant = HttpUtils.getTenant(req);
		ViaHeaders viaHeaders;
		try {
			viaHeaders = new ViaHeaders(req.headers().getAll(HttpHeaders.VIA),
					microServiceUtils.getSourceAlias(tenant));
		} catch (ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, tenant));
		}
		// try {
		// noConcise(body);
		// } catch (ResponseException e) {
		// return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e,
		// HttpUtils.getTenant(req)));
		// }
		boolean noOverwrite = options != null && options.contains(NGSIConstants.NO_OVERWRITE_OPTION);
		return HttpUtils.expandBody(req, body, AppConstants.ENTITY_UPDATE_PAYLOAD, ldService).onItem()
				.transformToUni(tuple -> {
					logger.debug("post attrs");
					return entityService
							.appendToEntity(HttpUtils.getTenant(req), entityId, tuple.getItem2(), noOverwrite,
									tuple.getItem1(), req.headers(), viaHeaders)
							.onItem().transform(HttpUtils::generateUpdateResultResponse);
				}).onFailure().recoverWithItem(e -> {
					return HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(req));
				});

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

		Map<String, Object> body;
		try {
			HttpUtils.validateUri(entityId);
			Map<String, Object> tmp = new JsonObject(bodyStr).getMap();
			if (!tmp.containsKey(attrib)) {
				Map<String, Object> tmp2;
				if (tmp.containsKey(NGSIConstants.JSON_LD_CONTEXT)) {
					tmp2 = new HashMap<String, Object>(2);
					tmp2.put(NGSIConstants.JSON_LD_CONTEXT, tmp.remove(NGSIConstants.JSON_LD_CONTEXT));
				} else {
					tmp2 = new HashMap<String, Object>(1);
				}
				tmp2.put(attrib, tmp);
				body = tmp2;
			} else {
				body = tmp;
			}
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(req)));
		}

		String tenant = HttpUtils.getTenant(req);
		ViaHeaders viaHeaders;
		try {
			viaHeaders = new ViaHeaders(req.headers().getAll(HttpHeaders.VIA),
					microServiceUtils.getSourceAlias(tenant));
		} catch (ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, tenant));
		}
		// try {
		// noConcise(body);
		// } catch (ResponseException e) {
		// return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e,
		// HttpUtils.getTenant(req)));
		// }

		return HttpUtils.expandBody(req, body, AppConstants.ENTITY_UPDATE_PAYLOAD, ldService).onItem()
				.transformToUni(tuple -> {
					String expAttrib = tuple.getItem1().expandIri(attrib, false, true, null, null);
					logger.debug("update entry :: started");

					return entityService.partialUpdateAttribute(HttpUtils.getTenant(req), entityId, expAttrib,
							tuple.getItem2(), tuple.getItem1(), req.headers(), viaHeaders).onItem()
							.transform(updateResult -> {
								logger.trace("update entry :: completed");
								return HttpUtils.generateUpdateResultResponse(updateResult);
							});
				}).onFailure().recoverWithItem(e -> {
					return HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(req));
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
			@QueryParam("deleteAll") String deleteAllS) {
		boolean deleteAll;
		try {
			deleteAll = HttpUtils.parseBoolean(deleteAllS);
			HttpUtils.validateUri(entityId);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
		}
		String tenant = HttpUtils.getTenant(request);
		ViaHeaders viaHeaders;
		try {
			viaHeaders = new ViaHeaders(request.headers().getAll(HttpHeaders.VIA),
					microServiceUtils.getSourceAlias(tenant));
		} catch (ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, tenant));
		}
		return ldService.parse(HttpUtils.getAtContext(request)).onItem().transformToUni(context -> {
			String finalAttrId = context.expandIri(attrId, false, true, null, null);
			logger.trace("delete attribute :: started");
			return entityService.deleteAttribute(HttpUtils.getTenant(request), entityId, finalAttrId, datasetId,
					deleteAll, context, request.headers(), viaHeaders).onItem().transform(opResult -> {
						logger.trace("delete attribute :: completed");
						return HttpUtils.generateDeleteResult(opResult);

					});
		}).onFailure().recoverWithItem(e -> {
			return HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request));
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
		try {
			HttpUtils.validateUri(entityId);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
		}
		String tenant = HttpUtils.getTenant(request);
		ViaHeaders viaHeaders;
		try {
			viaHeaders = new ViaHeaders(request.headers().getAll(HttpHeaders.VIA),
					microServiceUtils.getSourceAlias(tenant));
		} catch (ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, tenant));
		}
		return ldService.parse(HttpUtils.getAtContext(request)).onItem().transformToUni(context -> {
			return entityService
					.deleteEntity(HttpUtils.getTenant(request), entityId, context, request.headers(), viaHeaders)
					.onItem().transform(HttpUtils::generateDeleteResult);
		}).onFailure().recoverWithItem(e -> {
			return HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request));
		});

	}

	@PATCH
	@Path("/entities/{entityId}")
	public Uni<RestResponse<Object>> mergePatch(HttpServerRequest request, @PathParam("entityId") String entityId,
			String bodyStr) {
		Map<String, Object> body;
		try {
			HttpUtils.validateUri(entityId);
			body = new JsonObject(bodyStr).getMap();
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
		}
		String tenant = HttpUtils.getTenant(request);
		ViaHeaders viaHeaders;
		try {
			viaHeaders = new ViaHeaders(request.headers().getAll(HttpHeaders.VIA),
					microServiceUtils.getSourceAlias(tenant));
		} catch (ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, tenant));
		}
		if (!entityId.equals(body.get(NGSIConstants.ID)) && body.get(NGSIConstants.ID) != null) {
			return Uni.createFrom()
					.item(HttpUtils.handleControllerExceptions(
							new ResponseException(ErrorType.BadRequestData, "Id can not be updated"),
							HttpUtils.getTenant(request)));
		}
		// try {
		// noConcise(body);
		// } catch (ResponseException e) {
		// return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e,
		// HttpUtils.getTenant(request)));
		// }
		return HttpUtils.expandBody(request, body, AppConstants.MERGE_PATCH_REQUEST, ldService).onItem()
				.transformToUni(tuple -> {
					return entityService.mergePatch(HttpUtils.getTenant(request), entityId, tuple.getItem2(),
							tuple.getItem1(), request.headers(), viaHeaders).onItem()
							.transform(HttpUtils::generateUpdateResultResponse);
				}).onFailure().recoverWithItem(e -> {
					return HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request));
				});

	}

	@Path("/entities/{entityId}")
	@PUT
	public Uni<RestResponse<Object>> replaceEntity(@PathParam("entityId") String entityId, HttpServerRequest request,
			String bodyStr) {
		logger.debug("replacing entity");
		Map<String, Object> body;
		try {
			HttpUtils.validateUri(entityId);
			body = new JsonObject(bodyStr).getMap();
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
		}
		String tenant = HttpUtils.getTenant(request);
		ViaHeaders viaHeaders;
		try {
			viaHeaders = new ViaHeaders(request.headers().getAll(HttpHeaders.VIA),
					microServiceUtils.getSourceAlias(tenant));
		} catch (ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, tenant));
		}
		// try {
		// noConcise(body);
		// } catch (ResponseException e) {
		// return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e,
		// HttpUtils.getTenant(request)));
		// }
		body.put(NGSIConstants.ID, entityId);
		if (!body.containsKey(NGSIConstants.TYPE)) {
			return Uni.createFrom()
					.item(HttpUtils.handleControllerExceptions(
							new ResponseException(ErrorType.BadRequestData, "Type can not be null"),
							HttpUtils.getTenant(request)));
		}
		return HttpUtils.expandBody(request, body, AppConstants.REPLACE_ENTITY_PAYLOAD, ldService).onItem()
				.transformToUni(tuple -> {

					return entityService.replaceEntity(HttpUtils.getTenant(request), entityId, tuple.getItem2(),
							tuple.getItem1(), request.headers(), viaHeaders).onItem().transform(opResult -> {

								logger.debug("Done replacing entity");
								return HttpUtils.generateUpdateResultResponse(opResult);
							}).onFailure().recoverWithItem(e -> {
								return HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request));
							});
				});
	}

	@Path("/entities/{entityId}/attrs/{attrId}")
	@PUT
	public Uni<RestResponse<Object>> replaceAttribute(@PathParam("attrId") String attrId,
			@PathParam("entityId") String entityId, HttpServerRequest request, String bodyStr) {
		logger.debug("replacing Attrs");

		try {

			HttpUtils.validateUri(entityId);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
		}

		String tenant = HttpUtils.getTenant(request);
		ViaHeaders viaHeaders;
		try {
			viaHeaders = new ViaHeaders(request.headers().getAll(HttpHeaders.VIA),
					microServiceUtils.getSourceAlias(tenant));
		} catch (ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, tenant));
		}
		return JsonUtils.fromString(bodyStr).onItem().transformToUni(body -> {
			Map<String, Object> finalBody;
			if (body instanceof Map m) {

				if (!m.containsKey(attrId)) {
					if (m.containsKey(NGSIConstants.JSON_LD_CONTEXT)) {
						finalBody = new HashMap<>(2);
						finalBody.put(NGSIConstants.JSON_LD_CONTEXT, m.remove(NGSIConstants.JSON_LD_CONTEXT));
					} else {
						finalBody = new HashMap<>(1);
					}
					finalBody.put(bodyStr, m);
				} else {
					finalBody = m;
				}

			} else if (body instanceof List l) {
				finalBody = new HashMap<>(1);
				finalBody.put(attrId, l);
			} else {
				return Uni.createFrom().item(
						HttpUtils.handleControllerExceptions(new ResponseException(ErrorType.BadRequestData), tenant));
			}
			return HttpUtils.expandBody(request, finalBody, AppConstants.ENTITY_ATTRS_UPDATE_PAYLOAD, ldService)
					.onItem()
					.transformToUni(tuple -> {
						String finalAttrId = tuple.getItem1().expandIri(attrId, false, true, null, null);
						return entityService.replaceAttribute(tenant, tuple.getItem2(),
								tuple.getItem1(), entityId, finalAttrId, request.headers(), viaHeaders).onItem()
								.transform(opResult -> {
									logger.debug("Done replacing attribute");
									return HttpUtils.generateUpdateResultResponse(opResult);
								}).onFailure().recoverWithItem(e -> {
									return HttpUtils.handleControllerExceptions(e, tenant);
								});
					});
		});
	}
}
