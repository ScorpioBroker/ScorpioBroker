package eu.neclab.ngsildbroker.queryhandler.controller;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.Lists;
import com.google.common.net.HttpHeaders;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.results.QueryResult;
import eu.neclab.ngsildbroker.commons.datatypes.terms.AttrsQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.CSFQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.GeoQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.LanguageQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.QQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.ScopeQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.TypeQueryTerm;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.QueryParser;
import eu.neclab.ngsildbroker.queryhandler.services.QueryService;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpServerRequest;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.resteasy.reactive.RestResponse;
import org.jboss.resteasy.reactive.server.jaxrs.RestResponseBuilderImpl;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@Singleton
@Path("/ngsi-ld/v1")
public class QueryController {

	@Inject
	QueryService queryService;

	@ConfigProperty(name = "scorpio.entity.default-limit", defaultValue = "50")
	int defaultLimit;

	@ConfigProperty(name = "scorpio.entity.max-limit", defaultValue = "1000")
	int maxLimit;

	@ConfigProperty(name = "ngsild.corecontext", defaultValue = "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld")
	String coreContext;

	@PostConstruct
	public void init() {
		JsonLdProcessor.init(coreContext);
	}

	/**
	 * Method(GET) for multiple attributes separated by comma list
	 *
	 * @param request
	 * @param entityId
	 * @param attrs
	 * @return
	 * @throws ResponseException
	 */
	@Path("/entities/{entityId}")
	@GET
	public Uni<RestResponse<Object>> getEntity(HttpServerRequest request, @QueryParam(value = "attrs") String attrs,
			@QueryParam(value = "options") String options, @QueryParam(value = "lang") String lang,
			@QueryParam(value = "geometryProperty") String geometryProperty,
			@QueryParam(value = "localOnly") boolean localOnly, @PathParam("entityId") String entityId,
			@QueryParam(value = "doNotCompact") boolean doNotCompact) {
		int acceptHeader = HttpUtils.parseAcceptHeader(request.headers().getAll("Accept"));
		if (acceptHeader == -1) {
			return HttpUtils.getInvalidHeader();
		}

		Context context;
		List<Object> headerContext;
		AttrsQueryTerm attrsQuery;
		LanguageQueryTerm langQuery;
		try {
			HttpUtils.validateUri(entityId);
			headerContext = HttpUtils.getAtContext(request);
			context = JsonLdProcessor.getCoreContextClone().parse(headerContext, false);
			attrsQuery = QueryParser.parseAttrs(attrs, context);
			langQuery = QueryParser.parseLangQuery(lang);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
		}
		return queryService
				.retrieveEntity(context, HttpUtils.getTenant(request), entityId, attrsQuery, langQuery, localOnly)
				.onItem().transform(entity -> {
					if (doNotCompact)
						return RestResponse.ok((Object) entity);
					return HttpUtils.generateEntityResult(headerContext, context, acceptHeader, entity,
							geometryProperty, options, langQuery);
				}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);
	}

	/**
	 * Method(GET) for fetching all entities by kafka and other geo query operation
	 * by database
	 *
	 * @param request
	 * @param idPattern
	 * @return ResponseEntity object
	 */
	@Path("/entities")
	@GET
	public Uni<RestResponse<Object>> query(HttpServerRequest request, @QueryParam("id") String id,
			@QueryParam("type") String typeQuery, @QueryParam("idPattern") String idPattern,
			@QueryParam("attrs") String attrs, @QueryParam("q") String q, @QueryParam("csf") String csf,
			@QueryParam("geometry") String geometry, @QueryParam("georel") String georel,
			@QueryParam("coordinates") String coordinates, @QueryParam("geoproperty") String geoproperty,
			@QueryParam("geometryProperty") String geometryProperty, @QueryParam("lang") String lang,
			@QueryParam("scopeQ") String scopeQ, @QueryParam("localOnly") boolean localOnly,
			@QueryParam("options") String options, @QueryParam("limit") Integer limit, @QueryParam("offset") int offset,
			@QueryParam("count") boolean count, @QueryParam("entityMap") boolean entityMap,
			@QueryParam("zipEntityMap") boolean zipEntityMap) {
		int acceptHeader = HttpUtils.parseAcceptHeader(request.headers().getAll("Accept"));
		if (acceptHeader == -1) {
			return HttpUtils.getInvalidHeader();
		}
		int actualLimit;
		if (limit == null) {
			actualLimit = defaultLimit;
		} else {
			actualLimit = limit;
		}
		if (actualLimit > maxLimit) {
			return Uni.createFrom()
					.item(HttpUtils.handleControllerExceptions(new ResponseException(ErrorType.TooManyResults)));
		}
		if (id != null || typeQuery == null && attrs == null && geometry == null && q == null) {
			return Uni.createFrom()
					.item(HttpUtils.handleControllerExceptions(new ResponseException(ErrorType.InvalidRequest)));
		}

		Context context;
		List<Object> headerContext;
		AttrsQueryTerm attrsQuery;
		TypeQueryTerm typeQueryTerm;
		QQueryTerm qQueryTerm;
		CSFQueryTerm csfQueryTerm;
		GeoQueryTerm geoQueryTerm;
		ScopeQueryTerm scopeQueryTerm;
		LanguageQueryTerm langQuery;
		try {
			headerContext = HttpUtils.getAtContext(request);
			context = JsonLdProcessor.getCoreContextClone().parse(headerContext, false);
			attrsQuery = QueryParser.parseAttrs(attrs, context);
			typeQueryTerm = QueryParser.parseTypeQuery(typeQuery, context);
			qQueryTerm = QueryParser.parseQuery(q, context);
			csfQueryTerm = QueryParser.parseCSFQuery(csf, context);
			geoQueryTerm = QueryParser.parseGeoQuery(georel, coordinates, geometry, geoproperty, context);
			scopeQueryTerm = QueryParser.parseScopeQuery(scopeQ);
			langQuery = QueryParser.parseLangQuery(lang);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
		}
		String[] ids;
		if (id != null) {
			ids = id.split(",");
		} else {
			ids = null;
		}
		String token;
		boolean tokenProvided;

		String md5 = "" + request.params().remove("limit").remove("offset").remove("entityMap").hashCode();
		if (request.headers().contains("qToken")) {
			token = request.headers().get("qToken");
			if (!token.equals(md5)) {
				return Uni.createFrom()
						.item(HttpUtils.handleControllerExceptions(new ResponseException(ErrorType.InvalidRequest)));
			}
			tokenProvided = true;
		} else {
			token = md5;
			tokenProvided = false;
		}
		if (entityMap) {
			return queryService.queryForEntityIds(HttpUtils.getTenant(request), ids, typeQueryTerm, idPattern,
					attrsQuery, qQueryTerm, geoQueryTerm, scopeQueryTerm, langQuery, context).onItem()
					.transform(list -> {
						String body;
						Object result = "[]";
						try {
							body = JsonUtils.toPrettyString(list);
							if (zipEntityMap) {
								result = HttpUtils.zipResult(body);
							} else {
								result = body.getBytes();
							}
						} catch (JsonGenerationException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}

						return RestResponseBuilderImpl.ok(result).header("qToken", md5).build();
					});
		}

		return queryService.query(HttpUtils.getTenant(request), token, tokenProvided, ids, typeQueryTerm, idPattern,
				attrsQuery, qQueryTerm, csfQueryTerm, geoQueryTerm, scopeQueryTerm, langQuery, actualLimit, offset,
				count, localOnly, context).onItem().transform(queryResult -> {
					return HttpUtils.generateQueryResult(request, queryResult, options, geometryProperty, acceptHeader,
							count, actualLimit, langQuery, context);
				}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);
	}

	@Path("/types")
	@GET
	public Uni<RestResponse<Object>> getAllTypes(HttpServerRequest request,
			@QueryParam(value = "details") boolean details, @QueryParam(value = "localOnly") boolean localOnly) {

		HttpUtils.getAtContext(request);
		int acceptHeader = HttpUtils.parseAcceptHeader(request.headers().getAll(HttpHeaders.ACCEPT));
		if (acceptHeader == -1) {
			return HttpUtils.getInvalidHeader();
		}
		List<Object> contextHeader = HttpUtils.getAtContext(request);
		Context context;
		if (contextHeader.isEmpty()) {
			context = JsonLdProcessor.getCoreContextClone();
		} else {
			context = JsonLdProcessor.getCoreContextClone().parse(contextHeader, true);
		}
		if (details) {
			return queryService.getTypesWithDetail(HttpUtils.getTenant(request), localOnly).onItem()
					.transform(types -> {
						return HttpUtils.generateEntityResult(contextHeader, context, acceptHeader, types, null, null,
								null);
					});
		} else
			return queryService.getTypes(HttpUtils.getTenant(request), localOnly).onItem().transform(types -> {
				return HttpUtils.generateEntityResult(contextHeader, context, acceptHeader, types, null, null, null);
			});

	}

	@Path("/types/{entityType}")
	@GET
	public Uni<RestResponse<Object>> getType(HttpServerRequest request, @PathParam("entityType") String type,
			@QueryParam(value = "localOnly") boolean localOnly) {
		int acceptHeader = HttpUtils.parseAcceptHeader(request.headers().getAll(HttpHeaders.ACCEPT));
		if (acceptHeader == -1) {
			return HttpUtils.getInvalidHeader();
		}
		List<Object> contextHeader = HttpUtils.getAtContext(request);
		Context context;
		if (contextHeader.isEmpty()) {
			context = JsonLdProcessor.getCoreContextClone();
		} else {
			context = JsonLdProcessor.getCoreContextClone().parse(contextHeader, true);
		}
		return queryService
				.getType(HttpUtils.getTenant(request), context.expandIri(type, false, true, null, null), localOnly)
				.onItem().transform(map -> {
					if (map.isEmpty()) {
						return RestResponse.notFound();
					} else {
						return HttpUtils.generateEntityResult(contextHeader, context, acceptHeader, map, null, null,
								null);
					}
				});

	}

	@Path("/attributes")
	@GET
	public Uni<RestResponse<Object>> getAllAttributes(HttpServerRequest request,
			@QueryParam(value = "details") boolean details, @QueryParam(value = "localOnly") boolean localOnly) {

		int acceptHeader = HttpUtils.parseAcceptHeader(request.headers().getAll(HttpHeaders.ACCEPT));
		if (acceptHeader == -1) {
			return HttpUtils.getInvalidHeader();
		}
		if (!details) {
			return queryService.getAttribs(HttpUtils.getTenant(request), localOnly).onItem().transform(map -> {
				List<Object> contextHeader = HttpUtils.getAtContext(request);
				return HttpUtils.generateEntityResult(contextHeader,
						JsonLdProcessor.getCoreContextClone().parse(contextHeader, true), acceptHeader, map, null, null,
						null);
			});
		} else {
			return queryService.getAttribsWithDetails(HttpUtils.getTenant(request), localOnly).onItem()
					.transform(list -> {
						List<Object> contextHeader = HttpUtils.getAtContext(request);
						return HttpUtils.generateEntityResult(contextHeader,
								JsonLdProcessor.getCoreContextClone().parse(contextHeader, true), acceptHeader, list,
								null, null, null);
					});
		}

	}

	@Path("/attributes/{attribute}")
	@GET
	public Uni<RestResponse<Object>> getAttribute(HttpServerRequest request, @PathParam("attribute") String attribute,
			@QueryParam(value = "details") boolean details, @QueryParam(value = "localOnly") boolean localOnly) {
		int acceptHeader = HttpUtils.parseAcceptHeader(request.headers().getAll(HttpHeaders.ACCEPT));
		if (acceptHeader == -1) {
			return HttpUtils.getInvalidHeader();
		}
		Context context;
		List<Object> contextHeader = HttpUtils.getAtContext(request);
		if (!contextHeader.isEmpty()) {
			context = JsonLdProcessor.getCoreContextClone().parse(contextHeader, null, true);
		} else {
			context = JsonLdProcessor.getCoreContextClone();
		}
		attribute = context.expandIri(attribute, false, true, null, null);
		return queryService.getAttrib(HttpUtils.getTenant(request), attribute, localOnly).onItem().transform(map -> {
			if (map.isEmpty()) {
				return RestResponse.notFound();
			} else {
				return HttpUtils.generateEntityResult(contextHeader, context, acceptHeader, map, null, null, null);
			}
		});

	}

	@Path("/entityoperations/query")
	@POST
	public Uni<RestResponse<Object>> postQuery(HttpServerRequest request, String payload,
			@QueryParam(value = "limit") Integer limit, @QueryParam(value = "offset") int offset,
			@QueryParam(value = "options") String options, @QueryParam(value = "count") boolean count,
			@QueryParam(value = "localOnly") boolean localOnly,
			@QueryParam(value = "geometryProperty") String geometryProperty,
			@QueryParam(value = "doNotCompact") boolean doNotCompact) {

		int acceptHeader = HttpUtils.parseAcceptHeader(request.headers().getAll("Accept"));
		if (acceptHeader == -1) {
			return HttpUtils.getInvalidHeader();
		}
		int actualLimit;
		if (limit == null) {
			actualLimit = defaultLimit;
		} else {
			actualLimit = limit;
		}
		if (actualLimit > maxLimit) {
			return Uni.createFrom()
					.item(HttpUtils.handleControllerExceptions(new ResponseException(ErrorType.TooManyResults)));
		}

		// we are not expanding the complete payload here because there is some
		// weirdness in postquery payload. expanding item by item through the parsers is
		// fine

		Context context;

		AttrsQueryTerm attrsQuery = null;
		TypeQueryTerm typeQueryTerm = null;
		QQueryTerm qQueryTerm = null;
		CSFQueryTerm csfQueryTerm = null;
		GeoQueryTerm geoQueryTerm = null;
		ScopeQueryTerm scopeQueryTerm = null;
		LanguageQueryTerm langQuery;

		try {
			Map<String, Object> body = (Map<String, Object>) JsonUtils.fromString(payload);
			switch (request.getHeader(io.vertx.core.http.HttpHeaders.CONTENT_TYPE)) {
			case AppConstants.NGB_APPLICATION_JSON:
				if (body.containsKey(NGSIConstants.JSON_LD_CONTEXT)) {
					return Uni.createFrom().item(HttpUtils.handleControllerExceptions(
							new ResponseException(ErrorType.BadRequestData, "@context entry missing")));

				} else {
					context = JsonLdProcessor.getCoreContextClone().parse(HttpUtils.getAtContext(request), true);
					break;
				}
			case AppConstants.NGB_APPLICATION_JSONLD:
				if (body.containsKey(NGSIConstants.JSON_LD_CONTEXT)) {
					context = JsonLdProcessor.getCoreContextClone().parse(body.get(NGSIConstants.JSON_LD_CONTEXT),
							true);
					break;
				} else {
					return Uni.createFrom().item(HttpUtils.handleControllerExceptions(
							new ResponseException(ErrorType.BadRequestData, "@context entry missing")));
				}
			default:
				return Uni.createFrom()
						.item(HttpUtils.handleControllerExceptions(new ResponseException(ErrorType.InvalidRequest,
								"Only Content-Type " + AppConstants.NGB_APPLICATION_JSON + " and "
										+ AppConstants.NGB_APPLICATION_JSONLD + " are allowed")));
			}

			Object entities = body.get(NGSIConstants.NGSI_LD_ENTITIES_SHORT);
			Object attrs = body.get(NGSIConstants.QUERY_PARAMETER_ATTRS);
			Object q = body.get(NGSIConstants.QUERY_PARAMETER_QUERY);
			Object geoQ = body.get(NGSIConstants.NGSI_LD_GEO_QUERY_SHORT);
			if (entities == null && attrs == null && q == null && geoQ == null) {
				return Uni.createFrom()
						.item(HttpUtils.handleControllerExceptions(new ResponseException(ErrorType.BadRequestData,
								"At least one of these entries is required: entities, attrs, q, geoQ")));
			}

			Object lang = body.get(NGSIConstants.QUERY_PARAMETER_LANG);
			Object scopeQ = body.get(NGSIConstants.QUERY_PARAMETER_SCOPE_QUERY);
			Object csf = body.get(NGSIConstants.QUERY_PARAMETER_CSF);
			if (attrs != null) {
				attrsQuery = QueryParser.parseAttrs((String) attrs, context);
			}
			if (q != null) {
				qQueryTerm = QueryParser.parseQuery((String) q, context);
			}
			if (geoQ != null) {
				Map<String, Object> tmp = (Map<String, Object>) geoQ;
				Object georel = tmp.get(NGSIConstants.QUERY_PARAMETER_GEOREL);
				String coordinates = JsonUtils.toString(tmp.get(NGSIConstants.QUERY_PARAMETER_COORDINATES));
				Object geoproperty = tmp.get(NGSIConstants.QUERY_PARAMETER_GEOPROPERTY);
				Object geometry = tmp.get(NGSIConstants.QUERY_PARAMETER_GEOMETRY);
				geoQueryTerm = QueryParser.parseGeoQuery(georel == null ? null : (String) georel, coordinates,
						geometry == null ? null : (String) geometry, geoproperty == null ? null : (String) geoproperty,
						context);
			}
			if (scopeQ != null) {
				scopeQueryTerm = QueryParser.parseScopeQuery((String) scopeQ);
			}
			if (csf != null) {
				csfQueryTerm = QueryParser.parseCSFQuery((String) csf, context);
			}
			if (lang != null) {
				langQuery = QueryParser.parseLangQuery((String) lang);
			} else {
				langQuery = null;
			}
			String tenant = HttpUtils.getTenant(request);
			if (entities != null) {
				if (!(entities instanceof List)) {
					return Uni.createFrom().item(HttpUtils.handleControllerExceptions(
							new ResponseException(ErrorType.BadRequestData, "entities needs to be an array")));
				}
				List<Uni<QueryResult>> unis = Lists.newArrayList();
				for (Map<String, String> entityEntry : (List<Map<String, String>>) entities) {
					String id = entityEntry.get(NGSIConstants.QUERY_PARAMETER_ID);
					String idPattern = entityEntry.get(NGSIConstants.QUERY_PARAMETER_IDPATTERN);
					String typeQuery = entityEntry.get(NGSIConstants.QUERY_PARAMETER_TYPE);
					typeQueryTerm = QueryParser.parseTypeQuery(typeQuery, context);
					unis.add(queryService.query(HttpUtils.getTenant(request), request.headers().get("qToken"), false,
							id == null ? null : new String[] { id }, typeQueryTerm, idPattern, attrsQuery, qQueryTerm,
							csfQueryTerm, geoQueryTerm, scopeQueryTerm, langQuery, actualLimit, offset, count,
							localOnly, context));
				}
				return Uni.combine().all().unis(unis).combinedWith(list -> {
					Iterator<?> it = list.iterator();
					QueryResult first = (QueryResult) it.next();

					while (it.hasNext()) {
						first.getData().addAll(((QueryResult) it.next()).getData());
					}
					if (doNotCompact)
						return RestResponse.ok(first.getData());
					return HttpUtils.generateQueryResult(request, first, options, geometryProperty, acceptHeader, count,
							actualLimit, langQuery, context);
				});
			} else {
				return queryService.query(tenant, request.headers().get("qToken"), false, null, null, null, attrsQuery,
						qQueryTerm, csfQueryTerm, geoQueryTerm, scopeQueryTerm, langQuery, actualLimit, offset, count,
						localOnly, context).onItem().transform(queryResult -> {
							if (doNotCompact)
								return RestResponse.ok((Object) queryResult.getData());
							return HttpUtils.generateQueryResult(request, queryResult, options, geometryProperty,
									acceptHeader, count, actualLimit, langQuery, context);
						}).onFailure().recoverWithItem(e -> HttpUtils.handleControllerExceptions(e));
			}

		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
		}

	}

}