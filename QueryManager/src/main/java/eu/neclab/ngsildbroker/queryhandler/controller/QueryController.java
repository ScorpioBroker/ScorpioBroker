package eu.neclab.ngsildbroker.queryhandler.controller;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import eu.neclab.ngsildbroker.commons.datatypes.terms.DataSetIdTerm;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.UriInfo;
import org.apache.commons.lang3.RandomStringUtils;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.resteasy.reactive.RestResponse;
import org.jboss.resteasy.reactive.server.jaxrs.RestResponseBuilderImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.github.jsonldjava.core.JsonLDService;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.base.Objects;
import com.google.common.net.HttpHeaders;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
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

@Singleton
@Path("/ngsi-ld/v1")
public class QueryController {

	private static Logger logger = LoggerFactory.getLogger(QueryController.class);

	@Inject
	JsonLDService ldService;

	@Inject
	QueryService queryService;

	@ConfigProperty(name = "scorpio.entity.default-limit", defaultValue = "50")
	int defaultLimit;

	@ConfigProperty(name = "scorpio.entity.max-limit", defaultValue = "1000")
	int maxLimit;

	@ConfigProperty(name = "ngsild.corecontext", defaultValue = "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld")
	String coreContext;

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
			@QueryParam(value = "doNotCompact") boolean doNotCompact,
			@QueryParam("containedBy") @DefaultValue("") String containedBy, @QueryParam("join") String join,
			@QueryParam("idsOnly") boolean idsOnly, @QueryParam("joinLevel") @DefaultValue("1") int joinLevel,
			@QueryParam("pick") String pick,@QueryParam("omit") String omit,
			@QueryParam("format") String format,@QueryParam("jsonKeys") String jsonKeysQP) {
		int acceptHeader = HttpUtils.parseAcceptHeader(request.headers().getAll("Accept"));
		if(format!=null && !format.isEmpty()){
			options+=","+format;
		}
		if (acceptHeader == -1) {
			return HttpUtils.getInvalidHeader();
		}
		if (omit!= null && pick != null) {
			return Uni.createFrom()
					.item(HttpUtils.handleControllerExceptions(new ResponseException(ErrorType.BadRequestData)));
		}
		List<Object> headerContext;
		headerContext = HttpUtils.getAtContext(request);
		logger.debug("retrieve called: " + request.path());
		String finalOptions = options;
		return HttpUtils.getContext(headerContext, ldService).onItem().transformToUni(context -> {
			LanguageQueryTerm langQuery;
			List<String> omitList = omit == null ? new ArrayList<>() : Arrays.asList(omit.replaceAll("[\"\\n\\s]", "").split(","));
            try {
				HttpUtils.validateUri(entityId);
				langQuery = QueryParser.parseLangQuery(lang);
			} catch (Exception e) {
				return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
			}
			return queryService.retrieveEntity(context, HttpUtils.getTenant(request), entityId, attrs, langQuery,
					localOnly, containedBy, join, idsOnly, joinLevel).onItem().transformToUni(entity -> {
						if (doNotCompact) {
							return Uni.createFrom().item(RestResponse.ok((Object) entity));
						}
						String finalPick = "";
						if(jsonKeysQP != null){
							finalPick += jsonKeysQP+",id,type,";
						}
						if (pick != null){
							finalPick += pick+",";
						}
						Map<String, Object> pickMap = QueryParser.parseInput(finalPick);
						return HttpUtils.generateEntityResult(headerContext, context, acceptHeader, entity,
								geometryProperty, finalOptions, langQuery, ldService,omitList,pickMap);
					});
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
			@QueryParam("attrs") String attrs, @QueryParam("q") String qInput, @QueryParam("csf") String csf,
			@QueryParam("geometry") String geometry, @QueryParam("georel") String georelInput,
			@QueryParam("coordinates") String coordinates, @QueryParam("geoproperty") String geoproperty,
			@QueryParam("geometryProperty") String geometryProperty, @QueryParam("lang") String lang,
			@QueryParam("scopeQ") String scopeQ, @QueryParam("localOnly") boolean localOnly,
			@QueryParam("options") String options, @QueryParam("limit") Integer limit, @QueryParam("offset") int offset,
			@QueryParam("count") boolean count, @QueryParam("containedBy") @DefaultValue("") String containedBy,
			@QueryParam("join") String join, @QueryParam("idsOnly") boolean idsOnly,
			@QueryParam("joinLevel") @DefaultValue("1") int joinLevel, @QueryParam("doNotCompact") boolean doNotCompact,
			@QueryParam("entityMap") String entityMapToken, @QueryParam("maxDistance") String maxDistance,
			@QueryParam("minDistance") String minDistance, @Context UriInfo uriInfo,
			@QueryParam("pick") String pick,@QueryParam("omit") String omit,@QueryParam("format") String format,
			@QueryParam("jsonKeys") String jsonKeysQP,@QueryParam("datasetId") String datasetId) {
		int acceptHeader = HttpUtils.parseAcceptHeader(request.headers().getAll("Accept"));
		String q;
		String georel;
		if(format!=null && !format.isEmpty()){
			options+=","+format;
		}
		if(id!=null) {
			try {
				HttpUtils.validateUri(id);
			} catch (Exception e) {
				return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
			}
		}
		if (qInput != null) {
            String uri = URLDecoder.decode(request.absoluteURI(), StandardCharsets.UTF_8);
            uri = uri.substring(uri.indexOf("q=") + 2);
            int index = uri.indexOf('&');
            if (index != -1) {
                uri = uri.substring(0, index);
            }
			q=uri;
        } else {
			q = null;
		}
		if(maxDistance != null) {
			georel = georelInput + ";maxDistance="+maxDistance;
		}else if(minDistance != null) {
			georel = georelInput + ";minDistance="+minDistance;
		}else {
			georel = georelInput;
		}
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
		if (!localOnly && id == null && typeQuery == null && attrs == null && geometry == null && q == null && pick == null) {
			return Uni.createFrom()
					.item(HttpUtils.handleControllerExceptions(new ResponseException(ErrorType.BadRequestData)));
		}
		if (omit!= null && pick != null) {
			return Uni.createFrom()
					.item(HttpUtils.handleControllerExceptions(new ResponseException(ErrorType.BadRequestData)));
		}
		if (actualLimit == 0 && !count) {
			return Uni.createFrom()
					.item(HttpUtils.handleControllerExceptions(new ResponseException(ErrorType.BadLimitQuery)));
		}
		logger.debug("Query called: " + request.path());
		List<Object> headerContext;
		headerContext = HttpUtils.getAtContext(request);
		String finalOptions = options;
		Set<String> jsonKeys = new HashSet<>();
		if(jsonKeysQP!=null){
			jsonKeys.addAll(Arrays.asList(jsonKeysQP.split(",")));
		}
		return HttpUtils.getContext(headerContext, ldService).onItem().transformToUni(context -> {
			AttrsQueryTerm attrsQuery;
			TypeQueryTerm typeQueryTerm;
			QQueryTerm qQueryTerm;
			CSFQueryTerm csfQueryTerm;
			GeoQueryTerm geoQueryTerm;
			ScopeQueryTerm scopeQueryTerm;
			LanguageQueryTerm langQuery;
			DataSetIdTerm dataSetIdTerm;
			List<String> omitList = (omit == null) ? new ArrayList<>() : Arrays.asList(omit.replaceAll("[\"\\n\\s]", "").split(","));
			try {
				attrsQuery = QueryParser.parseAttrs(attrs == null ? null : attrs.replaceAll("[\"\\n\\s]", ""), context);
				if(!jsonKeys.isEmpty()){
					attrsQuery=QueryParser.parseAttrs(String.join(",",jsonKeys),context);
				}
				dataSetIdTerm = QueryParser.parseDataSetId(datasetId);
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
			String md5 = String.valueOf(Objects.hashCode(typeQuery, attrs, q, csf, geometry, georel, coordinates,
					geoproperty, geometryProperty, lang, scopeQ, localOnly, finalOptions));
			String headerToken = request.headers().get(NGSIConstants.ENTITY_MAP_TOKEN_HEADER);
			if (headerToken != null && entityMapToken != null) {
				if (!headerToken.equals(entityMapToken)) {
					return Uni.createFrom().item(
							HttpUtils.handleControllerExceptions(new ResponseException(ErrorType.InvalidRequest)));
				}
			}
			String tokenToTest = null;
			if (entityMapToken != null) {
				tokenToTest = entityMapToken;
			} else if (headerToken != null) {
				tokenToTest = headerToken;
			}
			if (tokenToTest != null) {
				if (!tokenToTest.substring(6).equals(md5)) {
					return Uni.createFrom().item(
							HttpUtils.handleControllerExceptions(new ResponseException(ErrorType.InvalidRequest)));
				}
				token = tokenToTest;
				tokenProvided = true;
			} else {
				token = RandomStringUtils.randomAlphabetic(6) + md5;
				tokenProvided = false;
			}
			if (idsOnly) {
				return queryService.queryForEntityIds(HttpUtils.getTenant(request), ids, typeQueryTerm, idPattern,
						attrsQuery, qQueryTerm, geoQueryTerm, scopeQueryTerm, langQuery, context, request.headers(),
						true, join, containedBy, joinLevel, doNotCompact,dataSetIdTerm).onItem().transform(list -> {
							String body;
							Object result = "[]";
							try {
								body = JsonUtils.toPrettyString(list);
//							if (zipEntityMap) {
//								result = HttpUtils.zipResult(body);
//							} else {
								result = body.getBytes();
//							}
							} catch (JsonGenerationException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}

							return RestResponseBuilderImpl.ok(result)
									.header(NGSIConstants.ENTITY_MAP_TOKEN_HEADER, token).build();
						});
			}

			return queryService
					.query(HttpUtils.getTenant(request), token, tokenProvided, ids, typeQueryTerm, idPattern,
							attrsQuery, qQueryTerm, csfQueryTerm, geoQueryTerm, scopeQueryTerm, langQuery, actualLimit,
							offset, count, localOnly, context, request.headers(), doNotCompact,jsonKeys,dataSetIdTerm)
					.onItem().transformToUni(queryResult -> {
						if (doNotCompact) {
							return Uni.createFrom().item(RestResponse.ok((Object) queryResult.getData()));
						}
						String finalPick = "";
						if(jsonKeysQP != null){
							finalPick += jsonKeysQP+",id,type,";
						}
						if (pick != null){
							finalPick += pick+",";
						}
						Map<String, Object> pickMap = QueryParser.parseInput(finalPick);
						return HttpUtils.generateQueryResult(request, queryResult, finalOptions, geometryProperty,
								acceptHeader, count, actualLimit, langQuery, context, ldService,omitList,pickMap);
					});
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
		return HttpUtils.getContext(contextHeader, ldService).onItem().transformToUni(context -> {
			if (details) {
				return queryService.getTypesWithDetail(HttpUtils.getTenant(request), localOnly).onItem()
						.transformToUni(types -> {
							return HttpUtils.generateEntityResult(contextHeader, context, acceptHeader, types, null,
									null, null, ldService,null,null);
						});
			} else {
				return queryService.getTypes(HttpUtils.getTenant(request), localOnly).onItem().transformToUni(types -> {
					return HttpUtils.generateEntityResult(contextHeader, context, acceptHeader, types, null, null, null,
							ldService,null,null);
				});
			}
		}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);

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
		return HttpUtils.getContext(contextHeader, ldService).onItem().transformToUni(context -> {
			return queryService
					.getType(HttpUtils.getTenant(request), context.expandIri(type, false, true, null, null), localOnly)
					.onItem().transformToUni(map -> {
						if (map.isEmpty()) {
							return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound));
						} else {
							return HttpUtils.generateEntityResult(contextHeader, context, acceptHeader, map, null, null,
									null, ldService,null,null);
						}
					});
		}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);

	}

	@Path("/attributes")
	@GET
	public Uni<RestResponse<Object>> getAllAttributes(HttpServerRequest request,
			@QueryParam(value = "details") boolean details, @QueryParam(value = "localOnly") boolean localOnly) {

		int acceptHeader = HttpUtils.parseAcceptHeader(request.headers().getAll(HttpHeaders.ACCEPT));
		if (acceptHeader == -1) {
			return HttpUtils.getInvalidHeader();
		}
		List<Object> contextHeader = HttpUtils.getAtContext(request);
		return HttpUtils.getContext(contextHeader, ldService).onItem().transformToUni(context -> {
			if (!details) {
				return queryService.getAttribs(HttpUtils.getTenant(request), localOnly).onItem().transformToUni(map -> {
					return HttpUtils.generateEntityResult(contextHeader, context, acceptHeader, map, null, null, null,
							ldService,null,null);
				});
			} else {
				return queryService.getAttribsWithDetails(HttpUtils.getTenant(request), localOnly).onItem()
						.transformToUni(list -> {
							return HttpUtils.generateEntityResult(contextHeader, context, acceptHeader, list, null,
									null, null, ldService,null,null);
						});
			}
		}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);

	}

	@Path("/attributes/{attribute}")
	@GET
	public Uni<RestResponse<Object>> getAttribute(HttpServerRequest request, @PathParam("attribute") String attribute,
			@QueryParam(value = "details") boolean details, @QueryParam(value = "localOnly") boolean localOnly) {
		int acceptHeader = HttpUtils.parseAcceptHeader(request.headers().getAll(HttpHeaders.ACCEPT));
		if (acceptHeader == -1) {
			return HttpUtils.getInvalidHeader();
		}

		List<Object> headerContext = HttpUtils.getAtContext(request);
		return HttpUtils.getContext(headerContext, ldService).onItem().transformToUni(context -> {
			return queryService.getAttrib(HttpUtils.getTenant(request),
					context.expandIri(attribute, false, true, null, null), localOnly).onItem().transformToUni(map -> {
						if (map.isEmpty()) {
							return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound) {
							});
						} else {
							return HttpUtils.generateEntityResult(headerContext, context, acceptHeader, map, null, null,
									null, ldService,null,null);
						}
					});
		}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);

	}

}