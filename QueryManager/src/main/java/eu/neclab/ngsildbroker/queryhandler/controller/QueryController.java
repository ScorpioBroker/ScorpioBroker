package eu.neclab.ngsildbroker.queryhandler.controller;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import eu.neclab.ngsildbroker.commons.datatypes.terms.DataSetIdTerm;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.QueryParam;

import jakarta.ws.rs.core.UriInfo;
import org.apache.commons.lang3.RandomStringUtils;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.resteasy.reactive.RestResponse;
import org.jboss.resteasy.reactive.server.jaxrs.RestResponseBuilderImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.core.JsonGenerationException;
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

	@ConfigProperty(name = "scorpio.entity.default-limit", defaultValue = "50")
	int defaultLimit;

	@ConfigProperty(name = "scorpio.entity.max-limit", defaultValue = "1000")
	int maxLimit;

	@ConfigProperty(name = "ngsild.corecontext", defaultValue = "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld")
	String coreContext;

	private String selfViaHeader;

	@PostConstruct
	public void setup() {
		URI gateway = microServiceUtils.getGatewayURL();
		this.selfViaHeader = gateway.getScheme().toUpperCase() + "/1.1 " + gateway.getAuthority();
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
			@QueryParam(value = "doNotCompact") boolean doNotCompact,
			@QueryParam("containedBy") @DefaultValue("") String containedBy, @QueryParam("join") String join,
			@QueryParam("joinLevel") @DefaultValue("0") int joinLevel, @QueryParam("pick") String pick,
			@QueryParam("omit") String omit, @QueryParam("format") String format,
			@QueryParam("entityMap") boolean entityMap, @QueryParam("datasetId") String datasetId,
			@QueryParam("splitEntities") @DefaultValue("true") boolean distEntities,
			@HeaderParam("NGSILD-EntityMap") String entityMapToken) {
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
							ldService, false, false, entityMap, microServiceUtils.getGatewayURL().toString(),
							NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT);
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
			@QueryParam("join") String join, @QueryParam("joinLevel") @DefaultValue("0") int joinLevel,
			@QueryParam("doNotCompact") boolean doNotCompact, @HeaderParam("NGSILD-EntityMap") String entityMapToken,
			@QueryParam("entityMap") boolean entityMapRetrieve, @QueryParam("maxDistance") String maxDistance,
			@QueryParam("minDistance") String minDistance, @QueryParam("pick") String pick,
			@QueryParam("omit") String omit, @QueryParam("format") String format,
			@QueryParam("jsonKeys") String jsonKeysQP, @QueryParam("datasetId") String datasetId,
			@QueryParam("splitEntities") @DefaultValue("true") boolean distEntities) {
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
							entityMapRetrieve, microServiceUtils.getGatewayURL().toString(),
							NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT);
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
									null, null, ldService, null, null);
						});
			} else {
				return queryService.getTypes(HttpUtils.getTenant(request), localOnly).onItem().transformToUni(types -> {
					return HttpUtils.generateEntityResult(contextHeader, context, acceptHeader, types, null, null, null,
							ldService, null, null);
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
									null, ldService, null, null);
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
							ldService, null, null);
				});
			} else {
				return queryService.getAttribsWithDetails(HttpUtils.getTenant(request), localOnly).onItem()
						.transformToUni(list -> {
							return HttpUtils.generateEntityResult(contextHeader, context, acceptHeader, list, null,
									null, null, ldService, null, null);
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
									null, ldService, null, null);
						}
					});
		}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);

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
			@QueryParam("datasetId") String datasetId) {
		int acceptHeader = HttpUtils.parseAcceptHeader(request.headers().getAll("Accept"));
		String q;
		String georel;

		if (id != null) {
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
			q = uri.replaceAll("\"", "");
		} else {
			q = null;
		}
		if (maxDistance != null) {
			georel = georelInput + ";maxDistance=" + maxDistance;
		} else if (minDistance != null) {
			georel = georelInput + ";minDistance=" + minDistance;
		} else {
			georel = georelInput;
		}
		if (acceptHeader == -1) {
			return HttpUtils.getInvalidHeader();
		}

//		if (!localOnly && id == null && typeQuery == null && attrs == null && geometry == null && q == null
//				&& pick == null) {
//			return Uni.createFrom()
//					.item(HttpUtils.handleControllerExceptions(new ResponseException(ErrorType.BadRequestData)));
//		}
		if (omit != null && pick != null) {
			return Uni.createFrom()
					.item(HttpUtils.handleControllerExceptions(new ResponseException(ErrorType.BadRequestData)));
		}

		logger.debug("Query called: " + request.path());
		List<Object> headerContext;
		headerContext = HttpUtils.getAtContext(request);

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
			List<String> pickListOriginal = pick == null ? new ArrayList<>()
					: new ArrayList<>(Arrays.asList(pick.replaceAll("[\"\\n\\s]", "").split(",")));
			List<String> omitList = (omit == null) ? new ArrayList<>()
					: Arrays.asList(omit.replaceAll("[\"\\n\\s]", "").split(","));
			try {
				if (!pickListOriginal.isEmpty()) {
					List<String> pickListCopy = new ArrayList<>(pickListOriginal);
					if (pickListCopy.contains(NGSIConstants.ID)) {
						pickListCopy.remove(NGSIConstants.ID);
					} else {
						omitList.add(NGSIConstants.ID);
					}
					if (pickListCopy.contains(NGSIConstants.TYPE)) {
						pickListCopy.remove(NGSIConstants.TYPE);
					} else {
						omitList.add(NGSIConstants.TYPE);
					}
					attrsQuery = QueryParser.parseAttrs(String.join(",", pickListCopy), context);
				} else {
					attrsQuery = QueryParser.parseAttrs(attrs == null ? null : attrs.replaceAll("[\"\\n\\s]", ""),
							context);
				}
				if (!jsonKeys.isEmpty()) {
					attrsQuery = QueryParser.parseAttrs(String.join(",", jsonKeys), context);
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
			String checkSum;
			if (typeQuery == null && attrs == null && q == null && csf == null && geometry == null && georel == null
					&& coordinates == null && geoproperty == null && geometryProperty == null && scopeQ == null
					&& pick == null && omit == null) {
				checkSum = null;
			} else {
				checkSum = String.valueOf(Objects.hashCode(typeQuery, attrs, q, csf, geometry, georel, coordinates,
						geoproperty, geometryProperty, scopeQ, pick, omit));
			}
			List<Tuple3<String[], TypeQueryTerm, String>> idsAndTypeQueryAndIdPattern;
			if (typeQuery != null || ids != null || idPattern != null) {
				idsAndTypeQueryAndIdPattern = new ArrayList<>(1);
				idsAndTypeQueryAndIdPattern.add(Tuple3.of(ids, typeQueryTerm, idPattern));
			} else {
				idsAndTypeQueryAndIdPattern = null;
			}
			String token = "urn:ngsi-ld:entitymap:" + UUID.randomUUID().toString();

			ViaHeaders viaHeaders = new ViaHeaders(request.headers().getAll(HttpHeaders.VIA), this.selfViaHeader);
			return queryService
					.getAndStoreEntityMap(HttpUtils.getTenant(request), token, idsAndTypeQueryAndIdPattern, attrsQuery,
							geoQueryTerm, qQueryTerm, scopeQueryTerm, langQuery, 1, 0, context, request.headers(),
							false, dataSetIdTerm, null, 0, true, null, null, checkSum, viaHeaders)
					.onItem().transform(t -> {
						return HttpUtils.generateEntityMapResult(t.getItem2());
					}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);
		});
	}

	@Path("/entityMap/{entityMapId}")
	@GET
	public Uni<RestResponse<Object>> getEntityMap(HttpServerRequest request,
			@PathParam("entityMapId") String entityMapId) {
		return queryService.getEntityMap(HttpUtils.getTenant(request), entityMapId).onItem()
				.transform(entityMap -> HttpUtils.generateEntityMapResult(entityMap));

	}

	private Uni<Tuple5<QueryResult, String, Integer, Integer, Context>> queryForQueryResult(HttpServerRequest request,
			String id, String typeQuery, String idPattern, String attrs, String qInput, String csf, String geometry,
			String georelInput, String coordinates, String geoproperty, String geometryProperty, String lang,
			String scopeQ, boolean localOnly, String options, Integer limit, int offset, boolean count,
			String containedBy, String join, int joinLevel, boolean doNotCompact, String entityMapToken,
			boolean entityMapRetrieve, String maxDistance, String minDistance, String pick, String omit, String format,
			String jsonKeysQP, String datasetId, boolean distEntities) {

		int acceptHeader = HttpUtils.parseAcceptHeader(request.headers().getAll("Accept"));
		if ((pick != null && omit != null) || (pick != null && attrs != null) || (attrs != null && omit != null)) {
			return Uni.createFrom().failure(
					new ResponseException(ErrorType.BadRequestData, "Omit, pick and attrs are mutually exclusive"));
		}
		String q;
		String georel;
		if (format != null && !format.isEmpty()) {
			options += "," + format;
		}

		if (qInput != null) {
			String uri = URLDecoder.decode(request.absoluteURI(), StandardCharsets.UTF_8);
			uri = uri.substring(uri.indexOf("q=") + 2);
			int index = uri.indexOf('&');
			if (index != -1) {
				uri = uri.substring(0, index);
			}
			q = uri.replaceAll("\"", "");
		} else {
			q = null;
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
				checkSum = null;
			} else {
				checkSum = String.valueOf(Objects.hashCode(typeQuery, attrs, q, csf, geometry, georel, coordinates,
						geoproperty, geometryProperty, scopeQ, pick, omit));
			}
			ViaHeaders viaHeaders = new ViaHeaders(request.headers().getAll(HttpHeaders.VIA), this.selfViaHeader);
			List<Tuple3<String[], TypeQueryTerm, String>> idsAndTypeQueryAndIdPattern;
			if (typeQuery != null || ids != null || idPattern != null) {
				idsAndTypeQueryAndIdPattern = new ArrayList<>(1);
				idsAndTypeQueryAndIdPattern.add(Tuple3.of(ids, typeQueryTerm, idPattern));
			} else {
				idsAndTypeQueryAndIdPattern = null;
			}

			return queryService
					.query(HttpUtils.getTenant(request), token, tokenProvided, idsAndTypeQueryAndIdPattern, attrsQuery,
							qQueryTerm, csfQueryTerm, geoQueryTerm, scopeQueryTerm, langQuery, actualLimit, offset,
							count, localOnly, context, request.headers(), doNotCompact, jsonKeys, dataSetIdTerm, join,
							joinLevel, distEntities, pickTerm, omitTerm, checkSum, viaHeaders)
					.onItem().transform(qR -> Tuple5.of(qR, finalOptions, acceptHeader, actualLimit, context));
		});
	}

}