package eu.neclab.ngsildbroker.queryhandler.controller;

import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Sets;
import com.google.common.net.HttpHeaders;
import eu.neclab.ngsildbroker.commons.datatypes.terms.*;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.QueryParser;
import eu.neclab.ngsildbroker.queryhandler.services.QueryService;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpServerRequest;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.resteasy.reactive.RestResponse;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static eu.neclab.ngsildbroker.commons.tools.HttpUtils.INVALID_HEADER;

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
			@QueryParam(value = "options") Set<String> options, @QueryParam(value = "lang") String lang,
			@QueryParam(value = "geometryProperty") String geometryProperty,
			@QueryParam(value = "localOnly") boolean localOnly, @PathParam("entityId") String entityId) {
		int acceptHeader = HttpUtils.parseAcceptHeader(request.headers().getAll("Accept"));
		if (acceptHeader == -1) {
			return HttpUtils.INVALID_HEADER;
		}

		Context context;
		List<Object> headerContext;
		AttrsQueryTerm attrsQuery;

		try {
			HttpUtils.validateUri(entityId);
			headerContext = HttpUtils.getAtContextNoUni(request);
			context = JsonLdProcessor.getCoreContextClone().parse(headerContext, false);
			attrsQuery = QueryParser.parseAttrs(attrs, context);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
		}
		return queryService.retrieveEntity(context, HttpUtils.getTenant(request), entityId, attrsQuery, lang, localOnly)
				.onItem().transform(entity -> {

					return HttpUtils.generateEntityResult(headerContext, context, acceptHeader, entity,
							geometryProperty, options);
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
	public Uni<RestResponse<Object>> query(HttpServerRequest request, @QueryParam("id") Set<String> id,
			@QueryParam("type") String typeQuery, @QueryParam("idPattern") String idPattern,
			@QueryParam("attrs") String attrs, @QueryParam("q") String q, @QueryParam("csf") String csf,
			@QueryParam("geometry") String geometry, @QueryParam("georel") String georel,
			@QueryParam("coordinates") String coordinates, @QueryParam("geoproperty") String geoproperty,
			@QueryParam("geometryProperty") String geometryProperty, @QueryParam("lang") String lang,
			@QueryParam("scopeQ") String scopeQ, @QueryParam("localOnly") Boolean localOnly,
			@QueryParam("options") String options, @QueryParam("limit") AtomicInteger limit,
			@QueryParam("offset") Integer offset, @QueryParam("count") Boolean count) {
		ArrayListMultimap<String, String> headers = HttpUtils.getHeaders(request);
		int acceptHeader = HttpUtils.parseAcceptHeader(headers.get("Accept"));
		if (acceptHeader == -1) {
			return HttpUtils.INVALID_HEADER;
		}
		if (limit.get() == 0) {
			limit.set(defaultLimit);
		}
		if (limit.get() > maxLimit) {
			return Uni.createFrom()
					.item(HttpUtils.handleControllerExceptions(new ResponseException(ErrorType.TooManyResults)));
		}
		if (typeQuery == null && attrs == null && geometry == null && q == null) {
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
		try {
			headerContext = HttpUtils.getAtContextNoUni(request);
			context = JsonLdProcessor.getCoreContextClone().parse(headerContext, false);
			attrsQuery = QueryParser.parseAttrs(attrs, context);
			typeQueryTerm = QueryParser.parseTypeQuery(typeQuery, context);
			qQueryTerm = QueryParser.parseQuery(q, context);
			csfQueryTerm = QueryParser.parseCSFQuery(csf, context);
			geoQueryTerm = QueryParser.parseGeoQuery(georel, coordinates, geometryProperty, geoproperty, context);
			scopeQueryTerm = QueryParser.parseScopeQuery(scopeQ);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
		}
		return queryService
				.query(HttpUtils.getTenant(request), id, typeQueryTerm, idPattern, attrsQuery, qQueryTerm, csfQueryTerm,
						geoQueryTerm, scopeQueryTerm, lang, limit.get(), offset, count, localOnly, context)
				.onItem().transform(queryResult -> {
					return HttpUtils.generateQueryResult(headerContext, context, acceptHeader, context,
							geometryProperty, id, options);
				}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);
	}

	@Path("/types")
	@GET
	public Uni<RestResponse<Object>> getAllTypes(HttpServerRequest request,
			@QueryParam(value = "details") boolean details, @QueryParam(value = "localOnly") boolean localOnly) {
		return HttpUtils.getAtContext(request).onItem().transformToUni(headerContext -> {
			ArrayListMultimap<String, String> headers = HttpUtils.getHeaders(request);
			int acceptHeader = HttpUtils.parseAcceptHeader(headers.get(HttpHeaders.ACCEPT));
			if (acceptHeader == -1) {
				return HttpUtils.INVALID_HEADER;
			}
			if (details) {
				return queryService.getTypesWithDetail(HttpUtils.getTenant(request), localOnly).onItem()
						.transform(types -> {
							if (types.isEmpty()) {
								return RestResponse.notFound();
							} else
								return RestResponse.ok(types);
						});
			} else
				return queryService.getTypes(HttpUtils.getTenant(request), localOnly).onItem().transform(types -> {
					if (types.isEmpty()) {
						return RestResponse.notFound();
					} else
						return RestResponse.ok(types);
				});
		});
	}

	@Path("/types/{entityType}")
	@GET
	public Uni<RestResponse<Object>> getType(HttpServerRequest request, @PathParam("entityType") String type,
			@QueryParam(value = "details") boolean details, @QueryParam(value = "localOnly") boolean localOnly) {
		return HttpUtils.getAtContext(request).onItem().transformToUni(headerContext -> {
			ArrayListMultimap<String, String> headers = HttpUtils.getHeaders(request);
			int acceptHeader = HttpUtils.parseAcceptHeader(headers.get(HttpHeaders.ACCEPT));
			if (acceptHeader == -1) {
				return HttpUtils.INVALID_HEADER;
			}
			return queryService.getType(HttpUtils.getTenant(request), type, localOnly).onItem().transform(map -> {
				if (map.isEmpty()) {
					return RestResponse.notFound();
				} else
					return RestResponse.ok(map);
			});
		});
	}

	@Path("/attributes")
	@GET
	public Uni<RestResponse<Object>> getAllAttributes(HttpServerRequest request,
			@QueryParam(value = "details") boolean details, @QueryParam(value = "localOnly") boolean localOnly) {
		return HttpUtils.getAtContext(request).onItem().transformToUni(headerContext -> {
			ArrayListMultimap<String, String> headers = HttpUtils.getHeaders(request);
			int acceptHeader = HttpUtils.parseAcceptHeader(headers.get(HttpHeaders.ACCEPT));
			if (acceptHeader == -1) {
				return HttpUtils.INVALID_HEADER;
			}
			return queryService.getAttribs(HttpUtils.getTenant(request), localOnly).onItem().transform(map -> {
				if (map.isEmpty()) {
					return RestResponse.notFound();
				} else
					return RestResponse.ok(map);
			});
		});
	}

	@Path("/attributes/{attribute}")
	@GET
	public Uni<RestResponse<Object>> getAttribute(HttpServerRequest request, @PathParam("attribute") String attribute,
			@QueryParam(value = "details") boolean details, @QueryParam(value = "localOnly") boolean localOnly) {
		return HttpUtils.getAtContext(request).onItem().transformToUni(headerContext -> {
			ArrayListMultimap<String, String> headers = HttpUtils.getHeaders(request);
			int acceptHeader = HttpUtils.parseAcceptHeader(headers.get(HttpHeaders.ACCEPT));
			if (acceptHeader == -1) {
				return HttpUtils.INVALID_HEADER;
			}
			return queryService.getAttrib(HttpUtils.getTenant(request), attribute, localOnly).onItem()
					.transform(map -> {
						if (map.isEmpty()) {
							return RestResponse.notFound();
						} else
							return RestResponse.ok(map);
					});
		});
	}

	@Path("/entityOperations/query")
	@POST
	public Uni<RestResponse<Object>> postQuery(HttpServerRequest request, String payload,
			@QueryParam(value = "limit") AtomicInteger limit, @QueryParam(value = "offset") Integer offset,
			@QueryParam(value = "options") List<String> options, @QueryParam(value = "count") Boolean count,
			@QueryParam(value = "lang") String lang, @QueryParam(value = "localOnly") Boolean localOnly) {
		return HttpUtils.getAtContext(request).onItem().transformToUni(headerContext -> {
			ArrayListMultimap<String, String> headers = HttpUtils.getHeaders(request);
			int acceptHeader = HttpUtils.parseAcceptHeader(headers.get(HttpHeaders.ACCEPT));
			if (acceptHeader == -1) {
				return INVALID_HEADER;
			}
			if (limit.get() == 0) {
				limit.set(defaultLimit);
			}
			if (limit.get() > maxLimit) {
				return Uni.createFrom()
						.item(HttpUtils.handleControllerExceptions(new ResponseException(ErrorType.TooManyResults)));
			}
			Map<String, Object> originalPayload;
			try {
				originalPayload = (Map<String, Object>) JsonUtils.fromString(payload);
			} catch (IOException e) {
				return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
			}
			List<Map<String, Object>> entities = (List<Map<String, Object>>) originalPayload.get("entities");
			Context context = JsonLdProcessor.getCoreContextClone().parse(headerContext, true);
			return queryService.postQuery(HttpUtils.getTenant(request), entities, lang, limit.get(), offset, count,
					localOnly, context).onItem().transform(RestResponse::ok);

		});
	}

}