package eu.neclab.ngsildbroker.entityhandler.controller;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonObject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.metrics.MetricUnits;
import org.eclipse.microprofile.metrics.annotation.ConcurrentGauge;
import org.eclipse.microprofile.metrics.annotation.Counted;
import org.eclipse.microprofile.metrics.annotation.Timed;
import org.jboss.resteasy.reactive.RestResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLDService;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.net.HttpHeaders;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.ParsedQueryParams;
import eu.neclab.ngsildbroker.commons.datatypes.ViaHeaders;
import eu.neclab.ngsildbroker.commons.datatypes.terms.AttrsQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.CSFQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.GeoQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.QQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.ScopeQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.TypeQueryTerm;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.enums.NgsiLdOperation;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import eu.neclab.ngsildbroker.commons.tools.QueryParamParser;
import eu.neclab.ngsildbroker.commons.tools.QueryParser;
import eu.neclab.ngsildbroker.entityhandler.services.EntityService;
import io.quarkus.runtime.Startup;
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

/**
 * 
 * @version 1.0
 * @date 10-Jul-2018
 */
@ApplicationScoped
@Startup
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
	@Counted(name = "entity_create_total", description = "Total number of entity create requests", absolute = true)
	@Timed(name = "entity_create_duration", description = "Duration of entity create requests", unit = MetricUnits.MILLISECONDS, absolute = true)
	@ConcurrentGauge(name = "entity_create_concurrent", description = "Number of concurrent entity create requests", absolute = true)
	public Uni<RestResponse<Object>> createEntity(HttpServerRequest req, String bodyStr) {

		Map<String, Object> body;
		String tenant = HttpUtils.getTenant(req);
		try {
			QueryParamParser.parse(req, NgsiLdOperation.CREATE_ENTITY);
			body = new JsonObject(bodyStr).getMap();
		} catch (DecodeException | ResponseException e) {
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
	@Counted(name = "entity_patch_total", description = "Total number of entity patch requests", absolute = true)
	@Timed(name = "entity_patch_duration", description = "Duration of entity patch requests", unit = MetricUnits.MILLISECONDS, absolute = true)
	@ConcurrentGauge(name = "entity_patch_concurrent", description = "Number of concurrent entity patch requests", absolute = true)
	public Uni<RestResponse<Object>> updateEntity(HttpServerRequest req, @PathParam("entityId") String entityId,
			String bodyStr) {
		Map<String, Object> body;
		String tenant = HttpUtils.getTenant(req);
		try {
			QueryParamParser.parse(req, NgsiLdOperation.UPDATE_ENTITY_ATTRS);
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
	@Counted(name = "entity_update_total", description = "Total number of entity update requests", absolute = true)
	@Timed(name = "entity_update_duration", description = "Duration of entity update requests", unit = MetricUnits.MILLISECONDS, absolute = true)
	@ConcurrentGauge(name = "entity_update_concurrent", description = "Number of concurrent entity update requests", absolute = true)
	public Uni<RestResponse<Object>> appendEntity(HttpServerRequest req, @PathParam("entityId") String entityId,
			String bodyStr) {
		String options;
		Map<String, Object> body;
		try {
			ParsedQueryParams queryParams = QueryParamParser.parse(req, NgsiLdOperation.APPEND_ENTITY_ATTRS);
			options = queryParams.getString(NGSIConstants.QUERY_PARAMETER_OPTIONS);
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
	@Counted(name = "attrs_patch_total", description = "Total number of attrs patch requests", absolute = true)
	@Timed(name = "attrs_patch_duration", description = "Duration of attrs patch requests", unit = MetricUnits.MILLISECONDS, absolute = true)
	@ConcurrentGauge(name = "attrs_patch_concurrent", description = "Number of concurrent attrs patch requests", absolute = true)
	public Uni<RestResponse<Object>> partialUpdateAttribute(HttpServerRequest req,
			@PathParam("entityId") String entityId, @PathParam("attrId") String attrib, String bodyStr) {

		Map<String, Object> body;
		try {
			QueryParamParser.parse(req, NgsiLdOperation.PARTIAL_UPDATE_ATTR);
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
	@Counted(name = "attrs_delete_total", description = "Total number of attrs delete requests", absolute = true)
	@Timed(name = "attrs_delete_duration", description = "Duration of attrs delete requests", unit = MetricUnits.MILLISECONDS, absolute = true)
	@ConcurrentGauge(name = "attrs_delete_concurrent", description = "Number of concurrent attrs delete requests", absolute = true)
	public Uni<RestResponse<Object>> deleteAttribute(HttpServerRequest request, @PathParam("entityId") String entityId,
			@PathParam("attrId") String attrId) {
		String datasetId;
		boolean deleteAll;
		try {
			ParsedQueryParams queryParams = QueryParamParser.parse(request, NgsiLdOperation.DELETE_ATTR);
			datasetId = queryParams.getString(NGSIConstants.QUERY_PARAMETER_DATA_SET_ID);
			deleteAll = queryParams.getBoolean(NGSIConstants.QUERY_PARAMETER_DELETE_ALL);
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
	@Counted(name = "entity_delete_total", description = "Total number of entity delete requests", absolute = true)
	@Timed(name = "entity_delete_duration", description = "Duration of entity delete requests", unit = MetricUnits.MILLISECONDS, absolute = true)
	@ConcurrentGauge(name = "entity_delete_concurrent", description = "Number of concurrent entity delete requests", absolute = true)
	public Uni<RestResponse<Object>> deleteEntity(HttpServerRequest request, @PathParam("entityId") String entityId) {
		try {
			QueryParamParser.parse(request, NgsiLdOperation.DELETE_ENTITY);
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

	/**
	 * Method(DELETE) for "/ngsi-ld/v1/entities" rest endpoint. Purge Entities as
	 * defined by NGSI-LD spec 5.6.21 / 6.4.3.3.
	 *
	 * @return ResponseEntity object
	 */
	@DELETE
	@Path("/entities")
	@Counted(name = "entity_purge_total", description = "Total number of entity purge requests", absolute = true)
	@Timed(name = "entity_purge_duration", description = "Duration of entity purge requests", unit = MetricUnits.MILLISECONDS, absolute = true)
	@ConcurrentGauge(name = "entity_purge_concurrent", description = "Number of concurrent entity purge requests", absolute = true)
	public Uni<RestResponse<Object>> purgeEntities(HttpServerRequest request) {
		ParsedQueryParams queryParams;
		String id;
		String type;
		String idPattern;
		String attrs;
		String q;
		String csf;
		String geometry;
		String georel;
		String coordinates;
		String geoproperty;
		String scopeQ;
		String drop;
		String keep;
		boolean localOnly;
		try {
			queryParams = QueryParamParser.parse(request, NgsiLdOperation.PURGE_ENTITIES);
			id = queryParams.getString(NGSIConstants.QUERY_PARAMETER_ID);
			type = queryParams.getString(NGSIConstants.QUERY_PARAMETER_TYPE);
			idPattern = queryParams.getString(NGSIConstants.QUERY_PARAMETER_IDPATTERN);
			attrs = queryParams.getString(NGSIConstants.QUERY_PARAMETER_ATTRS);
			q = queryParams.getQ();
			csf = queryParams.getString(NGSIConstants.QUERY_PARAMETER_CSF);
			geometry = queryParams.getString(NGSIConstants.QUERY_PARAMETER_GEOMETRY);
			georel = queryParams.getGeorel();
			coordinates = queryParams.getString(NGSIConstants.QUERY_PARAMETER_COORDINATES);
			geoproperty = queryParams.getString(NGSIConstants.QUERY_PARAMETER_GEOPROPERTY);
			scopeQ = queryParams.getScopeQ();
			drop = queryParams.getString(NGSIConstants.QUERY_PARAMETER_DROP);
			keep = queryParams.getString(NGSIConstants.QUERY_PARAMETER_KEEP);
			localOnly = queryParams.getLocal();
			if (drop != null && keep != null) {
				throw new ResponseException(ErrorType.BadRequestData, "drop and keep are mutually exclusive");
			}
			if (!localOnly && type == null && attrs == null && q == null && georel == null && geometry == null
					&& coordinates == null && drop == null && keep == null) {
				throw new ResponseException(ErrorType.BadRequestData,
						"Too wide purge query. Minimum required input is type, attrs, q, a geo query, drop or keep unless the purge is limited to local scope");
			}
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
			TypeQueryTerm typeQueryTerm;
			AttrsQueryTerm attrsQuery;
			QQueryTerm qQueryTerm;
			CSFQueryTerm csfQueryTerm;
			GeoQueryTerm geoQueryTerm;
			ScopeQueryTerm scopeQueryTerm;
			String[] ids;
			List<String> dropAttrs;
			List<String> keepAttrs;
			boolean local = localOnly;
			try {
				typeQueryTerm = QueryParser.parseTypeQuery(type, context);
				if (typeQueryTerm != null && typeQueryTerm.getAllTypes().contains(NGSIConstants.NGSI_LD_STAR)) {
					local = true;
					typeQueryTerm = null;
				}
				attrsQuery = QueryParser.parseAttrs(attrs, context);
				qQueryTerm = QueryParser.parseQuery(q, context);
				csfQueryTerm = QueryParser.parseCSFQuery(csf, context);
				geoQueryTerm = QueryParser.parseGeoQuery(georel, coordinates, geometry, geoproperty, context);
				scopeQueryTerm = QueryParser.parseScopeQuery(scopeQ);
				if (id != null) {
					ids = id.split(",");
					for (String tmpId : ids) {
						HttpUtils.validateUri(tmpId);
					}
				} else {
					ids = null;
				}
				dropAttrs = expandAttrList(drop, context);
				keepAttrs = expandAttrList(keep, context);
			} catch (Exception e) {
				return Uni.createFrom().failure(e);
			}
			return entityService.purgeEntities(tenant, ids, typeQueryTerm, idPattern, attrsQuery, qQueryTerm,
					geoQueryTerm, scopeQueryTerm, csfQueryTerm, dropAttrs, keepAttrs, local, queryParams, context,
					request.headers(), viaHeaders).onItem().transform(results -> {
						if (results.isEmpty()) {
							return RestResponse.status(RestResponse.Status.NO_CONTENT);
						}
						return HttpUtils.generateBatchResult(results);
					});
		}).onFailure().recoverWithItem(e -> {
			return HttpUtils.handleControllerExceptions(e, tenant);
		});

	}

	private static List<String> expandAttrList(String commaList, Context context) {
		if (commaList == null) {
			return null;
		}
		List<String> result = new ArrayList<>();
		for (String entry : commaList.split(",")) {
			result.add(context.expandIri(entry, false, true, null, null));
		}
		return result;
	}

	@PATCH
	@Path("/entities")
	@Counted(name = "entity_merge_patch_total", description = "Total number of entity merge patch requests", absolute = true)
	@Timed(name = "entity_merge_patch_duration", description = "Duration of entity merge patch requests", unit = MetricUnits.MILLISECONDS, absolute = true)
	@ConcurrentGauge(name = "entity_merge_patch_concurrent", description = "Number of concurrent entity merge patch requests", absolute = true)
	public Uni<RestResponse<Object>> mergePatchPure(HttpServerRequest request,
			String bodyStr) {
		String id;
		try {
			QueryParamParser.parse(request, NgsiLdOperation.MERGE_PATCH);
			Map<String, Object> body = new JsonObject(bodyStr).getMap();
			id = (String) body.get(NGSIConstants.ID);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
		}
		return mergePatch(request, id, bodyStr);
	}

	@PATCH
	@Path("/entities/{entityId}")
	@Counted(name = "entity_merge_patch_total", description = "Total number of entity merge patch requests", absolute = true)
	@Timed(name = "entity_merge_patch_duration", description = "Duration of entity merge patch requests", unit = MetricUnits.MILLISECONDS, absolute = true)
	@ConcurrentGauge(name = "entity_merge_patch_concurrent", description = "Number of concurrent entity merge patch requests", absolute = true)
	public Uni<RestResponse<Object>> mergePatch(HttpServerRequest request, @PathParam("entityId") String entityId,
			String bodyStr) {
		Map<String, Object> body;
		try {
			QueryParamParser.parse(request, NgsiLdOperation.MERGE_PATCH);
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
	@Counted(name = "entity_replace_total", description = "Total number of entity replace requests", absolute = true)
	@Timed(name = "entity_replace_duration", description = "Duration of entity replace requests", unit = MetricUnits.MILLISECONDS, absolute = true)
	@ConcurrentGauge(name = "entity_replace_concurrent", description = "Number of concurrent entity replace requests", absolute = true)
	public Uni<RestResponse<Object>> replaceEntity(@PathParam("entityId") String entityId, HttpServerRequest request,
			String bodyStr) {
		logger.debug("replacing entity");
		Map<String, Object> body;
		try {
			QueryParamParser.parse(request, NgsiLdOperation.REPLACE_ENTITY);
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
	@Counted(name = "attrs_replace_total", description = "Total number of attrs replace requests", absolute = true)
	@Timed(name = "attrs_replace_duration", description = "Duration of attrs replace requests", unit = MetricUnits.MILLISECONDS, absolute = true)
	@ConcurrentGauge(name = "attrs_replace_concurrent", description = "Number of concurrent attrs replace requests", absolute = true)
	public Uni<RestResponse<Object>> replaceAttribute(@PathParam("attrId") String attrId,
			@PathParam("entityId") String entityId, HttpServerRequest request, String bodyStr) {
		logger.debug("replacing Attrs");

		try {
			QueryParamParser.parse(request, NgsiLdOperation.REPLACE_ATTR);
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
