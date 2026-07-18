package eu.neclab.ngsildbroker.historyquerymanager.controller;

import com.github.jsonldjava.core.JsonLDService;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.ParsedQueryParams;
import eu.neclab.ngsildbroker.commons.datatypes.terms.AggrTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.AttrsQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.CSFQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.GeoQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.LanguageQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.QQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.ScopeQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.TemporalQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.TypeQueryTerm;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.enums.NgsiLdOperation;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import eu.neclab.ngsildbroker.commons.tools.QueryParamParser;
import eu.neclab.ngsildbroker.commons.tools.QueryParser;
import eu.neclab.ngsildbroker.historyquerymanager.service.HistoryQueryService;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple3;
import io.vertx.core.http.HttpServerRequest;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.metrics.MetricUnits;
import org.eclipse.microprofile.metrics.annotation.ConcurrentGauge;
import org.eclipse.microprofile.metrics.annotation.Counted;
import org.eclipse.microprofile.metrics.annotation.Timed;
import org.jboss.resteasy.reactive.RestResponse;
import jakarta.inject.Inject;
import jakarta.enterprise.context.ApplicationScoped;
import io.quarkus.runtime.Startup;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@ApplicationScoped
@Startup
@Path("/ngsi-ld/v1/temporal/entities")
public class HistoryController {

	// private final static Logger logger =
	// LoggerFactory.getLogger(HistoryController.class);

	@Inject
	MicroServiceUtils microServiceUtils;
	@Inject
	HistoryQueryService historyQueryService;
	@ConfigProperty(name = "scorpio.history.default-limit")
	int defaultLimit;
	@ConfigProperty(name = "scorpio.history.max-limit")
	int maxLimit;
	// @ConfigProperty(name = "scorpio.history.lastn")
	// int defaultLastN;
	// @ConfigProperty(name = "scorpio.history.max-lastn")
	// int maxLastN;

	@Inject
	JsonLDService ldService;

	@GET
	@Blocking
	@Counted(name = "temp_entity_query_total", description = "Total number of temp entity query requests", absolute = true)
	@Timed(name = "temp_entity_query_duration", description = "Duration of temp entity query requests", unit = MetricUnits.MILLISECONDS, absolute = true)
	@ConcurrentGauge(name = "temp_entity_query_concurrent", description = "Number of concurrent temp entity query requests", absolute = true)
	public Uni<RestResponse<Object>> queryTemporalEntities(HttpServerRequest request) {
		String tenant = HttpUtils.getTenant(request);
		ParsedQueryParams queryParams;
		String ids;
		String typeQuery;
		String idPattern;
		String attrs;
		String q;
		String csf;
		String geometry;
		String georel;
		String coordinates;
		String geoproperty;
		String timeProperty;
		String timerel;
		String scopeQ;
		String timeAt;
		String endTimeAt;
		int lastN;
		String lang;
		String aggrMethods;
		String aggrPeriodDuration;
		Integer limit;
		int offset;
		int nInput;
		int offsetN;
		String nOrderInput;
		int firstN;
		boolean localOnly;
		boolean count;
		try {
			queryParams = QueryParamParser.parse(request, NgsiLdOperation.QUERY_TEMPORAL);
			ids = queryParams.getString(NGSIConstants.QUERY_PARAMETER_ID);
			typeQuery = queryParams.getString(NGSIConstants.QUERY_PARAMETER_TYPE);
			idPattern = queryParams.getString(NGSIConstants.QUERY_PARAMETER_IDPATTERN);
			attrs = queryParams.getString(NGSIConstants.QUERY_PARAMETER_ATTRS);
			q = queryParams.getQ();
			csf = queryParams.getString(NGSIConstants.QUERY_PARAMETER_CSF);
			geometry = queryParams.getString(NGSIConstants.QUERY_PARAMETER_GEOMETRY);
			georel = queryParams.getGeorel();
			coordinates = queryParams.getString(NGSIConstants.QUERY_PARAMETER_COORDINATES);
			geoproperty = queryParams.getString(NGSIConstants.QUERY_PARAMETER_GEOPROPERTY);
			timeProperty = queryParams.getString(NGSIConstants.QUERY_PARAMETER_TIMEPROPERTY);
			timerel = queryParams.getString(NGSIConstants.QUERY_PARAMETER_TIMEREL);
			scopeQ = queryParams.getScopeQ();
			timeAt = queryParams.getString(NGSIConstants.QUERY_PARAMETER_TIME);
			endTimeAt = queryParams.getString(NGSIConstants.QUERY_PARAMETER_ENDTIME);
			lastN = queryParams.getInt(NGSIConstants.QUERY_PARAMETER_LAST_N, -1);
			lang = queryParams.getString(NGSIConstants.QUERY_PARAMETER_LANG);
			aggrMethods = queryParams.getString(NGSIConstants.QUERY_PARAMETER_AGGR_METHODS);
			aggrPeriodDuration = queryParams.getString(NGSIConstants.QUERY_PARAMETER_AGGR_PERIOD_DURATION);
			limit = queryParams.getInteger(NGSIConstants.QUERY_PARAMETER_LIMIT);
			offset = queryParams.getInt(NGSIConstants.QUERY_PARAMETER_OFFSET, 0);
			nInput = queryParams.getInt(NGSIConstants.QUERY_PARAMETER_N, -1);
			offsetN = queryParams.getInt(NGSIConstants.QUERY_PARAMETER_OFFSET_N, 0);
			nOrderInput = queryParams.getString(NGSIConstants.QUERY_PARAMETER_ORDER_N, "ASC");
			firstN = queryParams.getInt(NGSIConstants.QUERY_PARAMETER_FIRST_N, -1);
			localOnly = queryParams.getLocal();
			count = queryParams.getBoolean(NGSIConstants.QUERY_PARAMETER_COUNT);
		} catch (ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, tenant));
		}

		if ((nInput != -1 && lastN != -1 && lastN != nInput) || (nInput != -1 && firstN != -1 && firstN != nInput)
				|| (firstN != -1 && lastN != -1)) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(
					new ResponseException(ErrorType.BadRequestData,
							"Conflicting input in n and lastN or firstN. Please remove one"),
					tenant));
		}
		String nOrder;
		int n;
		if (lastN != -1) {
			nOrder = "DESC";
			n = lastN;
		} else if (firstN != -1) {
			nOrder = "ASC";
			n = firstN;
		} else {
			nOrder = nOrderInput;
			n = nInput;
		}
		int acceptHeader = HttpUtils.parseAcceptHeader(request.headers().getAll("Accept"));
		if (acceptHeader != 1 && acceptHeader != 2) {
			return HttpUtils.getInvalidHeader();
		}
		String[] idList;
		if (ids != null) {
			idList = ids.split(",");
		} else {
			idList = null;
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
		if (!localOnly && typeQuery == null && attrs == null && geometry == null && q == null) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(
					new ResponseException(ErrorType.InvalidRequest), HttpUtils.getTenant(request)));
		}

		List<Object> ctx = HttpUtils.getAtContext(request);
		Set<String> finalOptions;
		try {
			finalOptions = queryParams.getFinalOptions();
		} catch (ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, tenant));
		}
		return HttpUtils.getContext(ctx, ldService).onItem().transformToUni(context -> {
			TypeQueryTerm typeQueryTerm;
			AttrsQueryTerm attrsQueryTerm;
			CSFQueryTerm csfQueryTerm;
			QQueryTerm qQueryTerm;
			GeoQueryTerm geoQueryTerm;
			ScopeQueryTerm scopeQueryTerm;
			AggrTerm aggrTerm;
			LanguageQueryTerm languageQueryTerm;
			TemporalQueryTerm temporalQueryTerm;
			try {
				typeQueryTerm = QueryParser.parseTypeQuery(typeQuery, context);
				attrsQueryTerm = QueryParser.parseAttrs(attrs, context);
				qQueryTerm = QueryParser.parseQuery(q, context);
				csfQueryTerm = QueryParser.parseCSFQuery(csf, context);
				geoQueryTerm = QueryParser.parseGeoQuery(georel, coordinates, geometry, geoproperty, context);
				scopeQueryTerm = QueryParser.parseScopeQuery(scopeQ);
				temporalQueryTerm = QueryParser.parseTempQuery(timeProperty, timerel, timeAt, endTimeAt);
				aggrTerm = QueryParser.parseAggrTerm(aggrMethods, aggrPeriodDuration);
				languageQueryTerm = QueryParser.parseLangQuery(lang);
			} catch (Exception e) {
				return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
			}
			List<Tuple3<String[], TypeQueryTerm, String>> tmp = new ArrayList<>(1);
			tmp.add(Tuple3.of(idList, typeQueryTerm, idPattern));
			return historyQueryService.query(tenant, tmp, attrsQueryTerm, qQueryTerm,
					csfQueryTerm, geoQueryTerm, scopeQueryTerm, temporalQueryTerm, aggrTerm, languageQueryTerm,
					n, offsetN, nOrder, actualLimit, offset, count, localOnly, context, request.headers(),
					queryParams).onItem()
					.transformToUni(queryResult -> {
						int payloadType;
						if (aggrTerm == null) {
							payloadType = AppConstants.QUERY_PAYLOAD;
						} else {
							payloadType = -1;
						}
						return HttpUtils.generateQueryResult(request, queryResult, finalOptions, geoproperty,
								acceptHeader, count, actualLimit, languageQueryTerm, context, ldService, true, true,
								false, microServiceUtils.getGatewayString(),
								NGSIConstants.NGSI_LD_TEMPORAL_ENTITIES_ENDPOINT, payloadType);
					});
		}).onFailure().recoverWithItem(e -> HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
	}

	@Path("/{entityId}")
	@GET
	@Counted(name = "temp_entity_retrieve_total", description = "Total number of temp entity retrieve requests", absolute = true)
	@Timed(name = "temp_entity_retrieve_duration", description = "Duration of temp entity retrieve requests", unit = MetricUnits.MILLISECONDS, absolute = true)
	@ConcurrentGauge(name = "temp_entity_retrieve_concurrent", description = "Number of concurrent temp entity retrieve requests", absolute = true)
	public Uni<RestResponse<Object>> retrieveTemporalEntity(HttpServerRequest request,
			@PathParam("entityId") String entityId) {
		ParsedQueryParams queryParams;
		String attrs;
		String aggrMethods;
		String aggrPeriodDuration;
		String lang;
		int lastN;
		String geometryProperty;
		String timeProperty;
		String timeRel;
		String timeAt;
		String endTimeAt;
		int nInput;
		int offsetN;
		String nOrderInput;
		int firstN;
		boolean localOnly;
		try {
			queryParams = QueryParamParser.parse(request, NgsiLdOperation.RETRIEVE_TEMPORAL);
			attrs = queryParams.getString(NGSIConstants.QUERY_PARAMETER_ATTRS);
			aggrMethods = queryParams.getString(NGSIConstants.QUERY_PARAMETER_AGGR_METHODS);
			aggrPeriodDuration = queryParams.getString(NGSIConstants.QUERY_PARAMETER_AGGR_PERIOD_DURATION);
			lang = queryParams.getString(NGSIConstants.QUERY_PARAMETER_LANG);
			lastN = queryParams.getInt(NGSIConstants.QUERY_PARAMETER_LAST_N, -1);
			geometryProperty = queryParams.getString(NGSIConstants.QUERY_PARAMETER_GEOMETRY_PROPERTY);
			timeProperty = queryParams.getString(NGSIConstants.QUERY_PARAMETER_TIMEPROPERTY);
			timeRel = queryParams.getString(NGSIConstants.QUERY_PARAMETER_TIMEREL);
			timeAt = queryParams.getString(NGSIConstants.QUERY_PARAMETER_TIME);
			endTimeAt = queryParams.getString(NGSIConstants.QUERY_PARAMETER_ENDTIME);
			nInput = queryParams.getInt(NGSIConstants.QUERY_PARAMETER_N, -1);
			offsetN = queryParams.getInt(NGSIConstants.QUERY_PARAMETER_OFFSET_N, 0);
			nOrderInput = queryParams.getString(NGSIConstants.QUERY_PARAMETER_ORDER_N, "ASC");
			firstN = queryParams.getInt(NGSIConstants.QUERY_PARAMETER_FIRST_N, -1);
			localOnly = queryParams.getLocal();
		} catch (ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
		}
		int acceptHeader = HttpUtils.parseAcceptHeader(request.headers().getAll("Accept"));
		if (acceptHeader != 1 && acceptHeader != 2) {
			return HttpUtils.getInvalidHeader();
		}
		if ((nInput != -1 && lastN != -1 && lastN != nInput) || (nInput != -1 && firstN != -1 && firstN != nInput)
				|| (firstN != -1 && lastN != -1)) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(
					new ResponseException(ErrorType.BadRequestData,
							"Conflicting input in n and lastN or firstN. Please remove one"),
					HttpUtils.getTenant(request)));
		}
		String nOrder;
		int n;
		if (lastN != -1) {
			nOrder = "DESC";
			n = lastN;
		} else if (firstN != -1) {
			nOrder = "ASC";
			n = firstN;
		} else {
			nOrder = nOrderInput;
			n = nInput;
		}

		List<Object> headerContext;
		headerContext = HttpUtils.getAtContext(request);

		Set<String> finalOptions;
		try {
			finalOptions = queryParams.getFinalOptions();
		} catch (ResponseException e) {
			return Uni.createFrom().failure(e);
		}
		return ldService.parse(headerContext).onItem().transformToUni(context -> {
			AttrsQueryTerm attrsQuery;
			AggrTerm aggrQuery;
			TemporalQueryTerm tempQuery;
			try {
				HttpUtils.validateUri(entityId);

				attrsQuery = QueryParser.parseAttrs(attrs, context);
				aggrQuery = QueryParser.parseAggrTerm(aggrMethods, aggrPeriodDuration);
				tempQuery = QueryParser.parseTempQuery(timeProperty, timeRel, timeAt, endTimeAt);
			} catch (Exception e) {
				return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
			}
			return historyQueryService.retrieveEntity(HttpUtils.getTenant(request), entityId, attrsQuery, aggrQuery,
					tempQuery, lang, n, offsetN, nOrder, localOnly, context, request.headers()).onItem()
					.transformToUni(entity -> {
						if (aggrQuery != null || (finalOptions != null && !finalOptions
								.contains(NGSIConstants.QUERY_PARAMETER_OPTIONS_TEMPORALVALUES))) {
							return HttpUtils.generateResult(headerContext, context, acceptHeader, entity,
									geometryProperty,
									finalOptions, null,
									ldService, null, null, false, true,
									-1);
						} else {

							return HttpUtils.generateEntityResult(headerContext, context, acceptHeader, entity,
									geometryProperty, finalOptions, null, ldService, null, null, true);
						}
					});
		}).onFailure().recoverWithItem(e -> HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));

	}

}
