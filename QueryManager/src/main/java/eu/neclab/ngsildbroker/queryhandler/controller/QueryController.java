package eu.neclab.ngsildbroker.queryhandler.controller;

import java.util.List;
import java.util.Set;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.resteasy.reactive.RestResponse;

import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Sets;
import com.google.common.net.HttpHeaders;

import eu.neclab.ngsildbroker.commons.controllers.QueryControllerFunctions;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.QueryParser;
import eu.neclab.ngsildbroker.queryhandler.services.QueryService;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpServerRequest;

@Singleton
@Path("/ngsi-ld/v1")
public class QueryController {

	private static final Uni<RestResponse<Object>> INVALID_HEADER = Uni.createFrom()
			.item(HttpUtils.handleControllerExceptions(
					new ResponseException(ErrorType.NotAcceptable, "Provided accept types are not supported")));;
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
	public Uni<RestResponse<Object>> getEntity(HttpServerRequest request,
			@QueryParam(value = "attrs") Set<String> attrs, @QueryParam(value = "options") Set<String> options,
			@QueryParam(value = "lang") String lang, @QueryParam(value = "geometryProperty") String geometryProperty,
			@QueryParam(value = "localOnly") boolean localOnly, @PathParam("entityId") String entityId) {
		ArrayListMultimap<String, String> headers = HttpUtils.getHeaders(request);
		int acceptHeader = HttpUtils.parseAcceptHeader(headers.get("Accept"));
		if (acceptHeader == -1) {
			return INVALID_HEADER;
		}
		return HttpUtils.getAtContext(request).onItem().transformToUni(headerContext -> {
			Context context = JsonLdProcessor.getCoreContextClone().parse(headerContext, true);
			Set<String> expandedAttrs = Sets.newHashSet();
			for (String attr : attrs) {
				expandedAttrs.add(context.expandIri(attr, false, true, null, null));
			}

			return queryService
					.retrieveEntity(headers, entityId, attrs, expandedAttrs, geometryProperty, lang, localOnly).onItem()
					.transform(entity -> {
						return HttpUtils.generateEntityResult(headerContext, context, acceptHeader, entity,
								geometryProperty, options);
					}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);
		});
	}

	/**
	 * Method(GET) for fetching all entities by kafka and other geo query operation
	 * by database
	 * 
	 * @param request
	 * @param type
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
			@QueryParam("scopeQ") String scopeQ, @QueryParam("localOnly") String localOnly,
			@QueryParam("options") Set<String> options, @QueryParam("limit") Integer limit,
			@QueryParam("offset") Integer offset, @QueryParam("count") Boolean count) {
		
		return HttpUtils.getAtContext(request).onItem().transformToUni(headerContext -> {
			ArrayListMultimap<String, String> headers = HttpUtils.getHeaders(request);
			int acceptHeader = HttpUtils.parseAcceptHeader(headers.get(HttpHeaders.ACCEPT));
			if(acceptHeader == -1) {
				return INVALID_HEADER;
			}
			if(limit == null) {
				limit = defaultLimit;
			}
			if(limit > maxLimit) {
				return Uni.createFrom().item(HttpUtils.handleControllerExceptions(new ResponseException(ErrorType.TooManyResults)));
			}
			Context context = JsonLdProcessor.getCoreContextClone().parse(headerContext, true);
			
			
		});
		
		return QueryControllerFunctions.queryForEntries(queryService, request, false, defaultLimit, maxLimit, true);
	}

	@Path("/types")
	@GET
	public Uni<RestResponse<Object>> getAllTypes(HttpServerRequest request,
			@QueryParam(value = "details") boolean details, @QueryParam(value = "localOnly") boolean localOnly) {
		if(details) {
			return queryService.getTypes(HttpUtils.getHeaders(request), localOnly).onItem().transform(types -> {
				return HttpUtils.generateFollowUpLinkHeader(request, maxLimit, defaultLimit, coreContext, coreContext)
			});
		}
		return QueryControllerFunctions.getAllTypes(queryService, request, details, false, defaultLimit, maxLimit);
	}

	@Path("/types/{entityType}")
	@GET
	public Uni<RestResponse<Object>> getType(HttpServerRequest request, @PathParam("entityType") String type,
			@QueryParam(value = "details") boolean details) {
		return QueryControllerFunctions.getType(queryService, request, type, details, false, defaultLimit, maxLimit);
	}

	@Path("/attributes")
	@GET
	public Uni<RestResponse<Object>> getAllAttributes(HttpServerRequest request,
			@QueryParam(value = "details") boolean details) {

		return QueryControllerFunctions.getAllAttributes(queryService, request, details, false, defaultLimit, maxLimit);
	}

	@Path("/attributes/{attribute}")
	@GET
	public Uni<RestResponse<Object>> getAttribute(HttpServerRequest request, @PathParam("attribute") String attribute,
			@QueryParam(value = "details") boolean details) {
		return QueryControllerFunctions.getAttribute(queryService, request, attribute, details, false, defaultLimit,
				maxLimit);
	}

}