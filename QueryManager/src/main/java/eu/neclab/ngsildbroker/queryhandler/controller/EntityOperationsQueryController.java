package eu.neclab.ngsildbroker.queryhandler.controller;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import io.vertx.mutiny.core.MultiMap;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.QueryParam;

import org.apache.commons.lang3.RandomStringUtils;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.resteasy.reactive.RestResponse;

import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLDService;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;

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

@Singleton
@Path("/ngsi-ld/v1/entityOperations")
public class EntityOperationsQueryController {
	@Inject
	QueryService queryService;

	@ConfigProperty(name = "scorpio.entity.default-limit", defaultValue = "50")
	int defaultLimit;

	@ConfigProperty(name = "scorpio.entity.max-limit", defaultValue = "1000")
	int maxLimit;

	@ConfigProperty(name = "ngsild.corecontext", defaultValue = "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld")
	String coreContext;

	@Inject
	JsonLDService ldService;

	@Path("/query")
	@POST
	public Uni<RestResponse<Object>> postQuery(HttpServerRequest request, Map<String, Object> body,
			@QueryParam(value = "limit") Integer limit, @QueryParam(value = "offset") int offset,
			@QueryParam(value = "options") String options, @QueryParam(value = "count") boolean count,
			@QueryParam(value = "localOnly") boolean localOnly,
			@QueryParam(value = "geometryProperty") String geometryProperty,
			@QueryParam("entityMap") String entityMapToken) {

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

		Uni<Context> ctxUni;

		switch (request.getHeader(io.vertx.core.http.HttpHeaders.CONTENT_TYPE)) {
		case AppConstants.NGB_APPLICATION_JSON:
			if (body.containsKey(NGSIConstants.JSON_LD_CONTEXT)) {
				return Uni.createFrom()
						.item(HttpUtils.handleControllerExceptions(new ResponseException(ErrorType.BadRequestData,
								"@context is not allowed in content-type application/json")));

			} else {
				ctxUni = ldService.parse(HttpUtils.getAtContext(request));
				break;
			}
		case AppConstants.NGB_APPLICATION_JSONLD:
			if (body.containsKey(NGSIConstants.JSON_LD_CONTEXT)) {
				ctxUni = ldService.parse(body.get(NGSIConstants.JSON_LD_CONTEXT));
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
		return ctxUni.onItem().transformToUni(context -> {
			try {
				AttrsQueryTerm attrsQuery = null;
				TypeQueryTerm typeQueryTerm = null;
				QQueryTerm qQueryTerm = null;
				CSFQueryTerm csfQueryTerm = null;
				GeoQueryTerm geoQueryTerm = null;
				ScopeQueryTerm scopeQueryTerm = null;
				LanguageQueryTerm langQuery;
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
							geometry == null ? null : (String) geometry,
							geoproperty == null ? null : (String) geoproperty, context);
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
						String token;
						boolean tokenProvided;
						String md5 = "" + Objects.hashCode(typeQuery, attrs, q, csf, geoQ, geometryProperty, lang,
								scopeQ, localOnly, options);
						String headerToken = request.headers().get(NGSIConstants.ENTITY_MAP_TOKEN_HEADER);
						if (headerToken != null && entityMapToken != null) {
							if (!headerToken.equals(entityMapToken)) {
								return Uni.createFrom().item(HttpUtils
										.handleControllerExceptions(new ResponseException(ErrorType.InvalidRequest)));
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
								return Uni.createFrom().item(HttpUtils
										.handleControllerExceptions(new ResponseException(ErrorType.InvalidRequest)));
							}
							token = tokenToTest;
							tokenProvided = true;
						} else {
							token = RandomStringUtils.randomAlphabetic(6) + md5;
							tokenProvided = false;
						}
						unis.add(queryService.query(HttpUtils.getTenant(request), token, tokenProvided,
								id == null ? null : new String[] { id }, typeQueryTerm, idPattern, attrsQuery,
								qQueryTerm, csfQueryTerm, geoQueryTerm, scopeQueryTerm, langQuery, actualLimit, offset,
								count, localOnly, context, request.headers()));
					}
					return Uni.combine().all().unis(unis).combinedWith(list -> {
						Iterator<?> it = list.iterator();
						QueryResult first = (QueryResult) it.next();

						while (it.hasNext()) {
							first.getData().addAll(((QueryResult) it.next()).getData());
						}
						return first;
					}).onItem().transformToUni(first -> HttpUtils.generateQueryResult(request, first, options,
							geometryProperty, acceptHeader, count, actualLimit, langQuery, context, ldService));
				} else {
					String token;
					boolean tokenProvided;
					String md5 = ""
							+ Objects.hashCode(attrs, q, csf, geoQ, geometryProperty, lang, scopeQ, localOnly, options);
					String headerToken = request.headers().get(NGSIConstants.ENTITY_MAP_TOKEN_HEADER);
					if (headerToken != null && entityMapToken != null) {
						if (!headerToken.equals(entityMapToken)) {
							return Uni.createFrom().item(HttpUtils
									.handleControllerExceptions(new ResponseException(ErrorType.InvalidRequest)));
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
							return Uni.createFrom().item(HttpUtils
									.handleControllerExceptions(new ResponseException(ErrorType.InvalidRequest)));
						}
						token = tokenToTest;
						tokenProvided = true;
					} else {
						token = RandomStringUtils.randomAlphabetic(6) + md5;
						tokenProvided = false;
					}
					return queryService.query(tenant, token, tokenProvided, null, null, null, attrsQuery, qQueryTerm,
							csfQueryTerm, geoQueryTerm, scopeQueryTerm, langQuery, actualLimit, offset, count,
							localOnly, context, request.headers()).onItem().transformToUni(queryResult -> {
								return HttpUtils.generateQueryResult(request, queryResult, options, geometryProperty,
										acceptHeader, count, actualLimit, langQuery, context, ldService);
							}).onFailure().recoverWithItem(e -> HttpUtils.handleControllerExceptions(e));
				}
			} catch (Exception e) {
				return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
			}
		}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);

	}
}
