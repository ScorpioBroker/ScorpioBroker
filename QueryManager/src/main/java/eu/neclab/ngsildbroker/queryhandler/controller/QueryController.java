package eu.neclab.ngsildbroker.queryhandler.controller;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import eu.neclab.ngsildbroker.commons.datatypes.terms.DataSetIdTerm;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.PATCH;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.QueryParam;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.resteasy.reactive.RestResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLDService;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.net.HttpHeaders;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.ViaHeaders;
import eu.neclab.ngsildbroker.commons.datatypes.results.QueryResult;
import eu.neclab.ngsildbroker.commons.datatypes.terms.AttrsQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.CSFQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.GeoQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.LanguageQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.OmitTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.PickTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.QQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.Query;
import eu.neclab.ngsildbroker.commons.datatypes.terms.ScopeQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.TypeQueryTerm;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import eu.neclab.ngsildbroker.commons.tools.QueryParser;
import eu.neclab.ngsildbroker.queryhandler.services.QueryService;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple3;
import io.smallrye.mutiny.tuples.Tuple5;
import io.vertx.core.http.HttpServerRequest;

@Singleton
@Path("/ngsi-ld/v1")
public class QueryController {

	private static Logger logger = LoggerFactory.getLogger(QueryController.class);

	@Inject
	JsonLDService ldService;

	@Inject
	QueryService queryService;

	@Inject
	MicroServiceUtils microServiceUtils;

	@ConfigProperty(name = "scorpio.entity.default-limit")
	int defaultLimit;

	@ConfigProperty(name = "scorpio.entity.max-limit")
	int maxLimit;

	@ConfigProperty(name = "scorpio.ngsild.corecontext")
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
			@QueryParam(value = "local") String localOnlyS, @PathParam("entityId") String entityId,
			@QueryParam(value = "doNotCompact") String doNotCompactS,
			@QueryParam("containedBy") @DefaultValue(AppConstants.EMPTY) String containedBy,
			@QueryParam("join") String join,
			@QueryParam("joinLevel") Integer joinLevel, @QueryParam("pick") String pick,
			@QueryParam("omit") String omit, @QueryParam("format") String format,
			@QueryParam("entityMap") String entityMapS, @QueryParam("datasetId") String datasetId,
			@QueryParam("splitEntities") @DefaultValue("true") String distEntitiesS,
			@HeaderParam("NGSILD-EntityMap") String entityMapToken) {
		boolean localOnly;
		boolean doNotCompact;
		boolean entityMap;
		boolean distEntities;
		String tenant = HttpUtils.getTenant(request);
		try {
			localOnly = HttpUtils.parseBoolean(localOnlyS);
			doNotCompact = HttpUtils.parseBoolean(doNotCompactS);
			entityMap = HttpUtils.parseBoolean(entityMapS);
			distEntities = HttpUtils.parseBoolean(distEntitiesS);
		} catch (ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, tenant));
		}
		return queryForQueryResult(request, entityId, null, null, attrs, null, null, null, null, null, null,
				geometryProperty, lang, null, localOnly, options, 1, 0, false, containedBy, join, joinLevel,
				doNotCompact, entityMapToken, entityMap, null, null, pick, omit, format, null, datasetId, distEntities)
				.onItemOrFailure().transformToUni((t, e) -> {
					if (e != null) {
						return Uni.createFrom().failure(e);
					}
					QueryResult queryResult = t.getItem1();
					if (queryResult.getData().isEmpty()) {
						return Uni.createFrom()
								.failure(new ResponseException(ErrorType.NotFound, entityId + " was not found"));
					}
					return HttpUtils.generateQueryResult(request, queryResult, t.getItem2(), geometryProperty,
							t.getItem3(), false, t.getItem4(), queryResult.getLanguageQueryTerm(), t.getItem5(),
							ldService, false, false, entityMap, microServiceUtils.getGatewayString(),
							NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT);
				}).onFailure()
				.recoverWithItem(e -> HttpUtils.handleControllerExceptions(e, tenant));

	}

	@Path("/entities")
	@GET
	public Uni<RestResponse<Object>> query(HttpServerRequest request, @QueryParam("id") String id,
			@QueryParam("type") String typeQuery, @QueryParam("idPattern") String idPattern,
			@QueryParam("attrs") String attrs, @QueryParam("q") String qInput, @QueryParam("csf") String csf,
			@QueryParam("geometry") String geometry, @QueryParam("georel") String georelInput,
			@QueryParam("coordinates") String coordinates, @QueryParam("geoproperty") String geoproperty,
			@QueryParam("geometryProperty") String geometryProperty, @QueryParam("lang") String lang,
			@QueryParam("scopeQ") String scopeQ, @QueryParam("local") String localOnlyS,
			@QueryParam("options") String options, @QueryParam("limit") Integer limit, @QueryParam("offset") int offset,
			@QueryParam("count") String countS,
			@QueryParam("containedBy") @DefaultValue(AppConstants.EMPTY) String containedBy,
			@QueryParam("join") String join, @QueryParam("joinLevel") Integer joinLevel,
			@QueryParam("doNotCompact") String doNotCompactS, @HeaderParam("NGSILD-EntityMap") String entityMapToken,
			@QueryParam("entityMap") String entityMapRetrieveS, @QueryParam("maxDistance") String maxDistance,
			@QueryParam("minDistance") String minDistance, @QueryParam("pick") String pick,
			@QueryParam("omit") String omit, @QueryParam("format") String format,
			@QueryParam("jsonKeys") String jsonKeysQP, @QueryParam("datasetId") String datasetId,
			@QueryParam("splitEntities") @DefaultValue("true") String distEntitiesS) {
		boolean localOnly;
		boolean doNotCompact;
		boolean entityMapRetrieve;
		boolean distEntities;
		boolean count;
		String tenant = HttpUtils.getTenant(request);
		try {
			localOnly = HttpUtils.parseBoolean(localOnlyS);
			doNotCompact = HttpUtils.parseBoolean(doNotCompactS);
			entityMapRetrieve = HttpUtils.parseBoolean(entityMapRetrieveS);
			distEntities = HttpUtils.parseBoolean(distEntitiesS);
			count = HttpUtils.parseBoolean(countS);
		} catch (ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, tenant));
		}
		return queryForQueryResult(request, id, typeQuery, idPattern, attrs, qInput, csf, geometry, georelInput,
				coordinates, geoproperty, geometryProperty, lang, scopeQ, localOnly, options, limit, offset, count,
				containedBy, join, joinLevel, doNotCompact, entityMapToken, entityMapRetrieve, maxDistance, minDistance,
				pick, omit, format, jsonKeysQP, datasetId, distEntities).onItemOrFailure().transformToUni((t, e) -> {
					if (e != null) {
						return Uni.createFrom().failure(e);
					}
					QueryResult queryResult = t.getItem1();
					String finalOptions = t.getItem2();
					Integer acceptHeader = t.getItem3();
					Integer actualLimit = t.getItem4();
					Context context = t.getItem5();
					if (doNotCompact) {
						return Uni.createFrom().item(RestResponse.ok((Object) queryResult.getData()));
					}

					return HttpUtils.generateQueryResult(request, queryResult, finalOptions, geometryProperty,
							acceptHeader, count, actualLimit, queryResult.getLanguageQueryTerm(), context, ldService,
							entityMapRetrieve, microServiceUtils.getGatewayString(),
							NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT);
				}).onFailure()
				.recoverWithItem(e -> HttpUtils.handleControllerExceptions(e, tenant));
	}

	@Path("/types")
	@GET
	public Uni<RestResponse<Object>> getAllTypes(HttpServerRequest request,
			@QueryParam(value = "details") String detailsS, @QueryParam(value = "local") String localOnlyS,
			@QueryParam(value = "bbox") @DefaultValue("false") String bboxS) {
		boolean details;
		boolean localOnly;
		boolean bbox;
		String tenant = HttpUtils.getTenant(request);
		try {
			details = HttpUtils.parseBoolean(detailsS);
			localOnly = HttpUtils.parseBoolean(localOnlyS);
			bbox = HttpUtils.parseBoolean(bboxS);
		} catch (ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, tenant));
		}
		HttpUtils.getAtContext(request);
		int acceptHeader = HttpUtils.parseAcceptHeader(request.headers().getAll(HttpHeaders.ACCEPT));
		if (acceptHeader == -1) {
			return HttpUtils.getInvalidHeader();
		}
		ViaHeaders viaHeaders;
		try {
			viaHeaders = new ViaHeaders(request.headers().getAll(HttpHeaders.VIA),
					microServiceUtils.getSourceAlias(tenant));
		} catch (ResponseException e) {
			return Uni.createFrom().failure(e);
		}
		List<Object> contextHeader = HttpUtils.getAtContext(request);
		return HttpUtils.getContext(contextHeader, ldService).onItem().transformToUni(context -> {

			return queryService
					.getTypes(tenant, localOnly, request.headers(), details, bbox, viaHeaders)
					.onItem()
					.transformToUni(types -> {
						return HttpUtils.generateEntityResult(contextHeader, context, acceptHeader, types, null,
								null, null, ldService, null, null, true);
					});

		}).onFailure().recoverWithItem(e -> HttpUtils.handleControllerExceptions(e, tenant));

	}

	@Path("/types/{entityType}")
	@GET
	public Uni<RestResponse<Object>> getType(HttpServerRequest request, @PathParam("entityType") String type,
			@QueryParam(value = "local") String localOnlyS) {
		boolean localOnly;
		String tenant = HttpUtils.getTenant(request);
		try {
			localOnly = HttpUtils.parseBoolean(localOnlyS);
		} catch (ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, tenant));
		}
		int acceptHeader = HttpUtils.parseAcceptHeader(request.headers().getAll(HttpHeaders.ACCEPT));
		if (acceptHeader == -1) {
			return HttpUtils.getInvalidHeader();
		}
		List<Object> contextHeader = HttpUtils.getAtContext(request);
		return HttpUtils.getContext(contextHeader, ldService).onItem().transformToUni(context -> {
			return queryService.getType(tenant, context.expandIri(type, false, true, null, null),
					localOnly, request.headers(), false).onItem().transformToUni(map -> {
						if (map.isEmpty()) {
							return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound));
						} else {
							return HttpUtils.generateEntityResult(contextHeader, context, acceptHeader, map, null, null,
									null, ldService, null, null, true);
						}
					});
		}).onFailure().recoverWithItem(e -> HttpUtils.handleControllerExceptions(e, tenant));

	}

	@Path("/attributes")
	@GET
	public Uni<RestResponse<Object>> getAllAttributes(HttpServerRequest request,
			@QueryParam(value = "details") String detailsS, @QueryParam(value = "local") String localOnlyS) {
		boolean localOnly;
		boolean details;
		String tenant = HttpUtils.getTenant(request);
		try {
			localOnly = HttpUtils.parseBoolean(localOnlyS);
			details = HttpUtils.parseBoolean(detailsS);
		} catch (ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, tenant));
		}
		int acceptHeader = HttpUtils.parseAcceptHeader(request.headers().getAll(HttpHeaders.ACCEPT));
		if (acceptHeader == -1) {
			return HttpUtils.getInvalidHeader();
		}
		List<Object> contextHeader = HttpUtils.getAtContext(request);
		return HttpUtils.getContext(contextHeader, ldService).onItem().transformToUni(context -> {
			if (!details) {
				return queryService.getAttribs(tenant, localOnly, request.headers()).onItem()
						.transformToUni(map -> {
							return HttpUtils.generateEntityResult(contextHeader, context, acceptHeader, map, null, null,
									null, ldService, null, null, true);
						});
			} else {
				return queryService.getAttribsWithDetails(tenant, localOnly, request.headers())
						.onItem().transformToUni(list -> {
							return HttpUtils.generateEntityResult(contextHeader, context, acceptHeader, list, null,
									null, null, ldService, null, null, true);
						});
			}
		}).onFailure().recoverWithItem(e -> HttpUtils.handleControllerExceptions(e, tenant));

	}

	@Path("/attributes/{attribute}")
	@GET
	public Uni<RestResponse<Object>> getAttribute(HttpServerRequest request, @PathParam("attribute") String attribute,
			@QueryParam(value = "details") String detailsS, @QueryParam(value = "local") String localOnlyS) {
		boolean localOnly;
		boolean details;
		String tenant = HttpUtils.getTenant(request);
		try {
			localOnly = HttpUtils.parseBoolean(localOnlyS);
			details = HttpUtils.parseBoolean(detailsS);
		} catch (ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, tenant));
		}
		int acceptHeader = HttpUtils.parseAcceptHeader(request.headers().getAll(HttpHeaders.ACCEPT));
		if (acceptHeader == -1) {
			return HttpUtils.getInvalidHeader();
		}

		List<Object> headerContext = HttpUtils.getAtContext(request);
		return HttpUtils.getContext(headerContext, ldService).onItem().transformToUni(context -> {
			return queryService.getAttrib(tenant,
					context.expandIri(attribute, false, true, null, null), localOnly, request.headers()).onItem()
					.transformToUni(map -> {
						if (map.isEmpty()) {
							return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound));
						} else {
							return HttpUtils.generateEntityResult(headerContext, context, acceptHeader, map, null, null,
									null, ldService, null, null, true);
						}
					});
		}).onFailure().recoverWithItem(e -> HttpUtils.handleControllerExceptions(e, tenant));

	}

	@Path("/entityMap")
	@GET
	public Uni<RestResponse<Object>> queryEntityMap(HttpServerRequest request, @QueryParam("id") String id,
			@QueryParam("type") String typeQuery, @QueryParam("idPattern") String idPattern,
			@QueryParam("attrs") String attrs, @QueryParam("q") String qInput, @QueryParam("csf") String csf,
			@QueryParam("geometry") String geometry, @QueryParam("georel") String georelInput,
			@QueryParam("coordinates") String coordinates, @QueryParam("geoproperty") String geoproperty,
			@QueryParam("geometryProperty") String geometryProperty, @QueryParam("lang") String lang,
			@QueryParam("scopeQ") String scopeQ, @QueryParam("maxDistance") String maxDistance,
			@QueryParam("minDistance") String minDistance, @QueryParam("pick") String pick,
			@QueryParam("omit") String omit, @QueryParam("jsonKeys") String jsonKeysQP,
			@QueryParam("datasetId") String datasetId,
			@QueryParam("splitEntities") @DefaultValue("true") String distEntitiesS) {

		boolean distEntities;
		String tenant = HttpUtils.getTenant(request);
		try {
			distEntities = HttpUtils.parseBoolean(distEntitiesS);
		} catch (ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, tenant));
		}
		return getQueryParam(request, id, typeQuery, idPattern, attrs, qInput, csf, geometry, georelInput, coordinates,
				geoproperty, geometryProperty, lang, scopeQ, false, null, 1, 0, false, null, null, -1, false, null,
				false, maxDistance, minDistance, pick, omit, null, jsonKeysQP, datasetId, distEntities).onItem()
				.transformToUni(params -> {
					return queryService.getAndStoreEntityMap(tenant, params.getEntityMapToken(),
							params.getIdsAndTypeAndIdPattern(), params.getAttrsQueryTerm(), params.getGeoQueryTerm(),
							params.getqQueryTerm(), params.getScopeQueryTerm(), params.getLanguageQueryTerm(), 1, 0,
							params.getContext(), request.headers(), false, params.getDataSetIdTerm(), null, -1,
							distEntities, params.getPickTerm(), params.getOmitTerm(), params.getCheckSum(),
							params.getViaHeaders(), null, false, true, true).onItem().transform(t -> {
								return HttpUtils.generateEntityMapResult(t.getItem2());
							}).onFailure().recoverWithItem(
									e -> HttpUtils.handleControllerExceptions(e, tenant));
				});

	}

	@Path("/entityMap/{entityMapId}")
	@GET
	public Uni<RestResponse<Object>> getEntityMap(HttpServerRequest request,
			@PathParam("entityMapId") String entityMapId) {
		String tenant = HttpUtils.getTenant(request);
		return queryService.getEntityMap(tenant, entityMapId).onItem()
				.transform(entityMap -> HttpUtils.generateEntityMapResult(entityMap)).onFailure()
				.recoverWithItem(e -> HttpUtils.handleControllerExceptions(e, tenant));

	}

	@Path("/entityMap/{entityMapId}")
	@DELETE
	public Uni<RestResponse<Object>> deleteEntityMap(HttpServerRequest request,
			@PathParam("entityMapId") String entityMapId) {
		String tenant = HttpUtils.getTenant(request);
		return queryService.deleteEntityMap(tenant, entityMapId).onItem()
				.transform(v -> RestResponse.status(204));

	}

	@Path("/entityMap/{entityMapId}")
	@PATCH
	public Uni<RestResponse<Object>> updateEntityMap(HttpServerRequest request, String bodyStr,
			@PathParam("entityMapId") String entityMapId) {

		return JsonUtils.fromString(bodyStr).onItem().transformToUni(obj -> {
			@SuppressWarnings("unchecked")
			Map<String, Object> body = (Map<String, Object>) obj;
			String expiresAt = (String) body.get(NGSIConstants.EXPIRES_AT);
			String tenant = HttpUtils.getTenant(request);
			return queryService.updateEntityMap(tenant, entityMapId, expiresAt).onItem()
					.transform(v -> RestResponse.status(204)).onFailure()
					.recoverWithItem(e -> HttpUtils.handleControllerExceptions(e, tenant));

		});

	}

	public Uni<Tuple5<QueryResult, String, Integer, Integer, Context>> queryForQueryResult(HttpServerRequest request,
			String id, String typeQuery, String idPattern, String attrs, String qInput, String csf, String geometry,
			String georelInput, String coordinates, String geoproperty, String geometryProperty, String lang,
			String scopeQ, boolean localOnly, String options, Integer limit, int offset, boolean count,
			String containedBy, String join, Integer joinLevelInput, boolean doNotCompact, String entityMapToken,
			boolean entityMapRetrieve, String maxDistance, String minDistance, String pick, String omit, String format,
			String jsonKeysQP, String datasetId, boolean distEntities) {
		int joinLevel;
		if (joinLevelInput == null) {
			if (join == null) {
				joinLevel = 0;
			} else {
				joinLevel = 1;
			}
		} else {
			joinLevel = joinLevelInput;
		}
		String tenant = HttpUtils.getTenant(request);
		return getQueryParam(request, id, typeQuery, idPattern, attrs, qInput, csf, geometry, georelInput, coordinates,
				geoproperty, geometryProperty, lang, scopeQ, localOnly, options, limit, offset, count, containedBy,
				join, joinLevel, doNotCompact, entityMapToken, entityMapRetrieve, maxDistance, minDistance, pick, omit,
				format, jsonKeysQP, datasetId, distEntities).onItem().transformToUni(qP -> {
					return queryService
							.query(tenant, qP.getEntityMapToken(), qP.isTokenProvided(),
									qP.getIdsAndTypeAndIdPattern(), qP.getAttrsQueryTerm(), qP.getqQueryTerm(),
									qP.getCsfQueryTerm(), qP.getGeoQueryTerm(), qP.getScopeQueryTerm(),
									qP.getLanguageQueryTerm(), qP.getLimit(), offset, count, qP.isLocalOnly(),
									qP.getContext(), request.headers(), doNotCompact, qP.getJsonKeys(),
									qP.getDataSetIdTerm(), join, joinLevel, distEntities, qP.getPickTerm(),
									qP.getOmitTerm(), qP.getCheckSum(), qP.getViaHeaders(), null)
							.onItem().transform(qR -> Tuple5.of(qR, qP.getFinalOptions(), qP.getAcceptHeader(),
									qP.getLimit(), qP.getContext()));
				});

	}

	private Uni<Query> getQueryParam(HttpServerRequest request, String id, String typeQueryInput, String idPattern,
			String attrs, String qInput, String csf, String geometry, String georelInput, String coordinates,
			String geoproperty, String geometryProperty, String lang, String scopeQ, boolean localOnly, String options,
			Integer limit, int offset, boolean count, String containedBy, String join, int joinLevel,
			boolean doNotCompact, String entityMapToken, boolean entityMapRetrieve, String maxDistance,
			String minDistance, String pick, String omit, String format, String jsonKeysQP, String datasetId,
			boolean distEntities) {

		int acceptHeader = HttpUtils.parseAcceptHeader(request.headers().getAll("Accept"));
		if ((pick != null && omit != null) || (pick != null && attrs != null) || (attrs != null && omit != null)) {
			return Uni.createFrom().failure(
					new ResponseException(ErrorType.BadRequestData, "Omit, pick and attrs are mutually exclusive"));
		}
		String q;
		String georel;
		String typeQuery;

		if (format != null && !format.isEmpty()) {
			options += "," + format;
		}
		String decodedUri = URLDecoder.decode(request.absoluteURI(), StandardCharsets.UTF_8);
		if (qInput != null) {
			String uri = decodedUri;
			uri = uri.substring(uri.indexOf("q=") + 2);
			int index = uri.indexOf('&');
			if (index != -1) {
				uri = uri.substring(0, index);
			}
			q = uri.replaceAll("\"", "");
		} else {
			q = null;
		}
		if (typeQueryInput != null) {
			String uri = decodedUri;
			int start = uri.indexOf("type=") + 5;
			int end = uri.indexOf('&', start);
			if (end != -1) {
				typeQuery = uri.substring(start, end);
			} else {
				typeQuery = uri.substring(start);
			}

		} else {
			typeQuery = null;
		}

		if (maxDistance != null) {
			georel = georelInput + ";maxDistance=" + maxDistance;
		} else if (minDistance != null) {
			georel = georelInput + ";minDistance=" + minDistance;
		} else {
			georel = georelInput;
		}
		if (acceptHeader == -1) {
			return Uni.createFrom()
					.failure(new ResponseException(ErrorType.NotAcceptable, "Provided accept types are not supported"));
		}
		int actualLimit;
		if (limit == null) {
			actualLimit = defaultLimit;
		} else {
			actualLimit = limit;
		}
		if (actualLimit > maxLimit) {
			return Uni.createFrom().failure(new ResponseException(ErrorType.TooManyResults));
		}
		if (!localOnly && id == null && typeQuery == null && attrs == null && geometry == null && q == null
				&& pick == null) {
			return Uni.createFrom().failure(new ResponseException(ErrorType.BadRequestData));
		}
		if (omit != null && pick != null) {
			return Uni.createFrom().failure(new ResponseException(ErrorType.BadRequestData));
		}
		if (actualLimit == 0 && !count) {
			return Uni.createFrom().failure(new ResponseException(ErrorType.BadLimitQuery));
		}
		logger.debug("Query called: " + request.path());
		List<Object> headerContext;
		headerContext = HttpUtils.getAtContext(request);
		String finalOptions = options;
		Set<String> jsonKeys = new HashSet<>();
		if (jsonKeysQP != null) {
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
			OmitTerm omitTerm = null;
			PickTerm pickTerm = null;
			Query result = new Query();
			try {
				if (pick != null) {
					pickTerm = new PickTerm();
					QueryParser.parseProjectionTerm(pickTerm, pick, context);
				}
				if (omit != null) {
					omitTerm = OmitTerm.getNewRootInstance();
					QueryParser.parseProjectionTerm(omitTerm, omit, context);
				}

				attrsQuery = QueryParser.parseAttrs(attrs, context);
				dataSetIdTerm = QueryParser.parseDataSetId(datasetId);
				typeQueryTerm = QueryParser.parseTypeQuery(typeQuery, context);

				if (typeQueryTerm != null && typeQueryTerm.getAllTypes().contains(NGSIConstants.NGSI_LD_STAR)) {
					result.setLocalOnly(true);
					typeQueryTerm = null;
				} else {
					result.setLocalOnly(localOnly);
				}
				qQueryTerm = QueryParser.parseQuery(q, context);
				csfQueryTerm = QueryParser.parseCSFQuery(csf, context);
				geoQueryTerm = QueryParser.parseGeoQuery(georel, coordinates, geometry, geoproperty, context);
				scopeQueryTerm = QueryParser.parseScopeQuery(scopeQ);
				langQuery = QueryParser.parseLangQuery(lang);

			} catch (Exception e) {
				return Uni.createFrom().failure(e);
			}
			String[] ids;
			if (id != null) {
				List<String> tmpIds = Lists.newArrayList();
				int lastIdx = 0;
				int currentIdx = 0;
				String tmpId;
				while (true) {
					currentIdx = id.indexOf(',', lastIdx + 1);
					if (currentIdx == -1) {
						tmpId = id.substring(lastIdx);
						try {
							HttpUtils.validateUri(tmpId);
						} catch (Exception e) {
							return Uni.createFrom().failure(e);
						}
						tmpIds.add(tmpId);
						break;
					}
					tmpId = id.substring(lastIdx, currentIdx);
					try {
						HttpUtils.validateUri(tmpId);
					} catch (Exception e) {
						return Uni.createFrom().failure(e);
					}
					tmpIds.add(tmpId);
					lastIdx = currentIdx + 1;
				}

				ids = tmpIds.toArray(new String[0]);
			} else {
				ids = null;
			}
			String token;
			boolean tokenProvided;

			if (entityMapToken != null) {
				try {
					HttpUtils.validateUri(entityMapToken);
				} catch (ResponseException e) {
					return Uni.createFrom().failure(e);
				}
				token = entityMapToken;
				tokenProvided = true;
			} else {
				token = "urn:ngsi-ld:entitymap:" + UUID.randomUUID().toString();
				tokenProvided = false;
			}
			String checkSum;
			if (typeQuery == null && attrs == null && q == null && csf == null && geometry == null && georel == null
					&& coordinates == null && geoproperty == null && geometryProperty == null && scopeQ == null
					&& pick == null && omit == null) {
				checkSum = "";
			} else {
				checkSum = String.valueOf(Objects.hashCode(typeQuery, attrs, q, csf, geometry, georel, coordinates,
						geoproperty, geometryProperty, scopeQ, pick, omit));
			}
			String tenant = HttpUtils.getTenant(request);
			ViaHeaders viaHeaders;
			try {
				viaHeaders = new ViaHeaders(request.headers().getAll(HttpHeaders.VIA),
						microServiceUtils.getSourceAlias(tenant));
			} catch (ResponseException e) {
				return Uni.createFrom().failure(e);
			}
			List<Tuple3<String[], TypeQueryTerm, String>> idsAndTypeQueryAndIdPattern;
			if (typeQueryTerm != null || ids != null || idPattern != null) {
				idsAndTypeQueryAndIdPattern = new ArrayList<>(1);
				idsAndTypeQueryAndIdPattern.add(Tuple3.of(ids, typeQueryTerm, idPattern));
			} else {
				idsAndTypeQueryAndIdPattern = null;
			}

			result.setEntityMapToken(token);
			result.setTokenProvided(tokenProvided);
			result.setIdsAndTypeAndIdPattern(idsAndTypeQueryAndIdPattern);
			result.setAttrsQueryTerm(attrsQuery);
			result.setqQueryTerm(qQueryTerm);
			result.setCsfQueryTerm(csfQueryTerm);
			result.setGeoQueryTerm(geoQueryTerm);
			result.setScopeQueryTerm(scopeQueryTerm);
			result.setLanguageQueryTerm(langQuery);
			result.setJsonKeys(jsonKeys);
			result.setContext(context);
			result.setPickTerm(pickTerm);
			result.setOmitTerm(omitTerm);
			result.setCheckSum(checkSum);
			result.setViaHeaders(viaHeaders);
			result.setFinalOptions(finalOptions);
			result.setLimit(actualLimit);
			result.setAcceptHeader(acceptHeader);
			result.setDataSetIdTerm(dataSetIdTerm);
			return Uni.createFrom().item(result);
		});
	}

}