package eu.neclab.ngsildbroker.historyquerymanager.controller;

import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.resteasy.reactive.RestResponse;

import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLDService;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.base.Objects;
import com.google.common.net.HttpHeaders;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.ViaHeaders;
import eu.neclab.ngsildbroker.commons.datatypes.results.QueryResult;
import eu.neclab.ngsildbroker.commons.datatypes.terms.AggrTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.AttrsQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.CSFQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.GeoQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.LanguageQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.OmitTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.PickTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.QQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.ScopeQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.TemporalQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.TypeQueryTerm;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import eu.neclab.ngsildbroker.commons.tools.QueryParser;
import eu.neclab.ngsildbroker.historyquerymanager.service.HistoryQueryService;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple3;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.QueryParam;

@Singleton
@Path("/ngsi-ld/v1/temporal/entityOperations")
public class HistoryOperationsController {

	@Inject
	HistoryQueryService queryService;

	@Inject
	JsonLDService ldService;
	@Inject
	MicroServiceUtils microServiceUtils;
	private String selfViaHeader;

	@ConfigProperty(name = "scorpio.history.defaultLimit", defaultValue = "50")
	int defaultLimit;
	@ConfigProperty(name = "scorpio.history.maxLimit", defaultValue = "1000")
	int maxLimit;
	@ConfigProperty(name = "scorpio.history.lastN", defaultValue = "50")
	int defaultLastN;
	@ConfigProperty(name = "scorpio.history.maxLastN", defaultValue = "1000")
	int maxLastN;

	@PostConstruct
	public void setup() {
		URI gateway = microServiceUtils.getGatewayURL();
		this.selfViaHeader = gateway.getScheme().toUpperCase() + "/1.1 " + gateway.getAuthority();
	}

	@Path("/query")
	@POST
	public Uni<RestResponse<Object>> postQuery(HttpServerRequest request, String bodyStr,
			@QueryParam(value = "limit") Integer limit, @QueryParam(value = "offset") int offset,
			@QueryParam("lastN") Integer lastN, @QueryParam(value = "options") String options,
			@QueryParam(value = "count") boolean count, @QueryParam(value = "local") boolean localOnly,
			@QueryParam(value = "geometryProperty") String geometryProperty,
			@HeaderParam("NGSILD-EntityMap") String entityMapToken, @QueryParam("entityMap") boolean retrieveEntityMap,
			@QueryParam(value = "doNotCompact") boolean doNotCompact) {
		int acceptHeader = HttpUtils.parseAcceptHeader(request.headers().getAll("Accept"));
		Map<String, Object> body;
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
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(
					new ResponseException(ErrorType.TooManyResults), HttpUtils.getTenant(request)));
		}
		try {
			body = new JsonObject(bodyStr).getMap();
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
		}
		int lastNTBU;
		if (lastN == null) {
			lastNTBU = defaultLastN;
		} else {
			lastNTBU = lastN;
		}
		// we are not expanding the complete payload here because there is some
		// weirdness in postquery payload. expanding item by item through the parsers is
		// fine

		Uni<Context> ctxUni;

		switch (request.getHeader(io.vertx.core.http.HttpHeaders.CONTENT_TYPE)) {
		case AppConstants.NGB_APPLICATION_JSON:
			if (body.containsKey(NGSIConstants.JSON_LD_CONTEXT)) {
				return Uni.createFrom()
						.item(HttpUtils.handleControllerExceptions(
								new ResponseException(ErrorType.BadRequestData,
										"@context is not allowed in content-type application/json"),
								HttpUtils.getTenant(request)));

			} else {
				ctxUni = ldService.parse(HttpUtils.getAtContext(request));
				break;
			}
		case AppConstants.NGB_APPLICATION_JSONLD:
			if (body.containsKey(NGSIConstants.JSON_LD_CONTEXT)) {
				ctxUni = ldService.parse(body.get(NGSIConstants.JSON_LD_CONTEXT));
				break;
			} else {
				return Uni.createFrom()
						.item(HttpUtils.handleControllerExceptions(
								new ResponseException(ErrorType.BadRequestData, "@context entry missing"),
								HttpUtils.getTenant(request)));
			}
		default:
			return Uni.createFrom()
					.item(HttpUtils.handleControllerExceptions(
							new ResponseException(ErrorType.InvalidRequest,
									"Only Content-Type " + AppConstants.NGB_APPLICATION_JSON + " and "
											+ AppConstants.NGB_APPLICATION_JSONLD + " are allowed"),
							HttpUtils.getTenant(request)));
		}
		return ctxUni.onItem().transformToUni(context -> {
			try {
				AttrsQueryTerm attrsQuery = null;
				TypeQueryTerm typeQueryTerm = null;
				QQueryTerm qQueryTerm = null;
				CSFQueryTerm csfQueryTerm = null;
				GeoQueryTerm geoQueryTerm = null;
				ScopeQueryTerm scopeQueryTerm = null;
				OmitTerm omitTerm = null;
				PickTerm pickTerm = null;
				boolean localOnlyTBU = localOnly;
				LanguageQueryTerm langQuery;
				AggrTerm aggrTerm = null;
				LanguageQueryTerm languageQueryTerm = null;
				TemporalQueryTerm temporalQueryTerm = null;
				Object entities = body.get(NGSIConstants.NGSI_LD_ENTITIES_SHORT);
				Object attrs = body.get(NGSIConstants.QUERY_PARAMETER_ATTRS);
				Object q = body.get(NGSIConstants.QUERY_PARAMETER_QUERY);
				Object geoQ = body.get(NGSIConstants.NGSI_LD_GEO_QUERY_SHORT);
				Object temporalQ = body.get(NGSIConstants.NGSI_LD_TEMPORAL_QUERY_SHORT);
				Object aggrQ = body.get(NGSIConstants.NGSI_LD_AGGR_QUERY_SHORT);
				Object joinObj = body.get(NGSIConstants.QUERY_PARAMETER_JOIN);
				Object joinLevelObj = body.get(NGSIConstants.QUERY_PARAMETER_JOINLEVEL);
				String join = joinObj == null ? null : (String) joinObj;
				int joinLevel = joinLevelObj == null ? 0 : (int) joinLevelObj;
				boolean entityDist = (boolean) body.getOrDefault(NGSIConstants.QUERY_PARAMETER_ENTITY_DIST, false);
				if (entities == null && attrs == null && q == null && geoQ == null) {
					return Uni.createFrom()
							.item(HttpUtils.handleControllerExceptions(
									new ResponseException(ErrorType.BadRequestData,
											"At least one of these entries is required: entities, attrs, q, geoQ"),
									HttpUtils.getTenant(request)));
				}

				Object lang = body.get(NGSIConstants.QUERY_PARAMETER_LANG);
				Object scopeQ = body.get(NGSIConstants.QUERY_PARAMETER_SCOPE_QUERY);
				Object csf = body.get(NGSIConstants.QUERY_PARAMETER_CSF);
				Object omit = body.get(NGSIConstants.QUERY_PARAMETER_OMIT);
				Object pick = body.get(NGSIConstants.QUERY_PARAMETER_PICK);
				if ((pick != null && omit != null) || (pick != null && attrs != null)
						|| (attrs != null && omit != null)) {
					return Uni.createFrom().failure(new ResponseException(ErrorType.BadRequestData,
							"Omit, pick and attrs are mutually exclusive"));
				}
				Object georel = null;
				String coordinates = null;
				Object geoproperty;
				Object geometry = null;
				ViaHeaders viaHeaders = new ViaHeaders(request.headers().getAll(HttpHeaders.VIA), this.selfViaHeader);

				if (attrs != null) {
					if (attrs instanceof List<?>) {
						attrsQuery = QueryParser.parseAttrs(String.join(",", (ArrayList<String>) attrs), context);
					} else if (attrs instanceof String) {
						attrsQuery = QueryParser.parseAttrs((String) attrs, context);
					} else {
						return Uni.createFrom()
								.failure(new ResponseException(ErrorType.InvalidRequest, "Unable to parse attrs."));
					}
				}

				if (temporalQ != null && temporalQ instanceof Map<?, ?> m) {
					Object timeProperty = m.get(NGSIConstants.QUERY_PARAMETER_TIMEPROPERTY);
					Object timeRel = m.get(NGSIConstants.QUERY_PARAMETER_TIMEREL);
					Object endTimeAt = m.get(NGSIConstants.QUERY_PARAMETER_ENDTIME);
					Object timeAt = m.get(NGSIConstants.QUERY_PARAMETER_TIME);
					temporalQueryTerm = QueryParser.parseTempQuery(timeProperty == null ? null : (String) timeProperty,
							timeRel == null ? null : (String) timeRel, timeAt == null ? null : (String) timeAt,
							endTimeAt == null ? null : (String) endTimeAt);
				}
				if (aggrQ != null && aggrQ instanceof Map<?, ?> m) {
					Object aggrMethods = m.get(NGSIConstants.NGSI_LD_AGGR_METHODS_SHORT);
					if (aggrMethods != null && aggrMethods instanceof List l) {
						aggrMethods = String.join(",", l);
					}
					Object aggrPeriodDuration = m.get(NGSIConstants.NGSI_LD_AGGR_PERIOD_DURATION_SHORT);

					aggrTerm = QueryParser.parseAggrTerm(aggrMethods == null ? null : (String) aggrMethods,
							aggrPeriodDuration == null ? null : (String) aggrPeriodDuration);
				}
				//
				//
				if (q != null) {
					qQueryTerm = QueryParser.parseQuery((String) q, context);
				}

				if (geoQ != null) {
					Map<String, Object> tmp = (Map<String, Object>) geoQ;
					georel = tmp.get(NGSIConstants.QUERY_PARAMETER_GEOREL);
					coordinates = JsonUtils.toString(tmp.get(NGSIConstants.QUERY_PARAMETER_COORDINATES));
					geoproperty = tmp.get(NGSIConstants.QUERY_PARAMETER_GEOPROPERTY);
					geometry = tmp.get(NGSIConstants.QUERY_PARAMETER_GEOMETRY);
					geoQueryTerm = QueryParser.parseGeoQuery(georel == null ? null : (String) georel, coordinates,
							geometry == null ? null : (String) geometry,
							geoproperty == null ? null : (String) geoproperty, context);
				} else {
					geoproperty = null;
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
				if (pick != null) {
					pickTerm = new PickTerm();
					if (pick instanceof List<?>) {
						QueryParser.parseProjectionTerm(pickTerm, String.join(",", (ArrayList<String>) pick), context);
					} else if (pick instanceof String) {
						QueryParser.parseProjectionTerm(pickTerm, (String) pick, context);
					} else {
						return Uni.createFrom()
								.failure(new ResponseException(ErrorType.InvalidRequest, "Unable to parse pick."));
					}

				}
				if (omit != null) {
					omitTerm = OmitTerm.getNewRootInstance();
					if (omit instanceof List<?>) {
						QueryParser.parseProjectionTerm(pickTerm, String.join(",", (ArrayList<String>) omit), context);
					} else if (omit instanceof String) {
						QueryParser.parseProjectionTerm(omitTerm, (String) omit, context);
					} else {
						return Uni.createFrom()
								.failure(new ResponseException(ErrorType.InvalidRequest, "Unable to parse omit."));
					}

				}
				String tenant = HttpUtils.getTenant(request);
				String token;
				boolean tokenProvided;
				if (entityMapToken != null) {
					try {
						HttpUtils.validateUri(entityMapToken);
					} catch (ResponseException e) {
						return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, tenant));
					}
					token = entityMapToken;
					tokenProvided = true;
				} else {
					token = "urn:ngsi-ld:entitymap:" + UUID.randomUUID().toString();
					tokenProvided = false;
				}
				List<Tuple3<String[], TypeQueryTerm, String>> idsAndTypeQueryAndIdPattern;

				if (entities != null) {
					int listSize = -1;
					if (entities instanceof List<?> l) {
						listSize = l.size();
					} else {
						return Uni.createFrom().item(HttpUtils.handleControllerExceptions(new ResponseException(
								ErrorType.BadRequestData, "entities needs to be an array with an entry"), tenant));
					}
					if (listSize <= 0) {
						return Uni.createFrom().item(HttpUtils.handleControllerExceptions(new ResponseException(
								ErrorType.BadRequestData, "entities needs to be an array with an entry"), tenant));
					}
					idsAndTypeQueryAndIdPattern = new ArrayList<>(listSize);
					for (Map<String, String> entityEntry : (List<Map<String, String>>) entities) {
						String id = entityEntry.get(NGSIConstants.QUERY_PARAMETER_ID);
						String idPattern = entityEntry.get(NGSIConstants.QUERY_PARAMETER_IDPATTERN);
						String typeQuery = entityEntry.get(NGSIConstants.QUERY_PARAMETER_TYPE);
						typeQueryTerm = QueryParser.parseTypeQuery(typeQuery, context);
						if (typeQueryTerm != null && typeQueryTerm.getAllTypes().contains(NGSIConstants.NGSI_LD_STAR)) {
							localOnlyTBU = true;
							typeQueryTerm = null;
						}
						String[] ids = id == null ? null : id.split(",");
						idsAndTypeQueryAndIdPattern.add(Tuple3.of(ids, typeQueryTerm, idPattern));
					}
				} else {
					idsAndTypeQueryAndIdPattern = null;
				}
				String checkSum;
				if (idsAndTypeQueryAndIdPattern == null && attrs == null && q == null && csf == null && geometry == null
						&& georel == null && coordinates == null && geoproperty == null && geometryProperty == null
						&& scopeQ == null && pick == null && omit == null) {
					checkSum = null;
				} else {
					checkSum = String.valueOf(Objects.hashCode(idsAndTypeQueryAndIdPattern, attrs, q, csf, geometry,
							georel, coordinates, geoproperty, geometryProperty, scopeQ, pick, omit));
				}

				return queryService.query(tenant, idsAndTypeQueryAndIdPattern, attrsQuery, qQueryTerm, csfQueryTerm,
						geoQueryTerm, scopeQueryTerm, temporalQueryTerm, aggrTerm, langQuery, lastNTBU, limit, offset,
						false, localOnly, context, request).onItem().transformToUni(queryResult -> {
							return HttpUtils.generateQueryResult(request, queryResult, options, (String) geoproperty,
									acceptHeader, count, actualLimit, langQuery, context, ldService, true, true, false,
									microServiceUtils.getGatewayURL().toString(),
									NGSIConstants.NGSI_LD_TEMPORAL_ENTITIES_ENDPOINT);
						});

			} catch (Exception e) {
				return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
			}
		}).onFailure().recoverWithItem(e -> HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));

	}

}
