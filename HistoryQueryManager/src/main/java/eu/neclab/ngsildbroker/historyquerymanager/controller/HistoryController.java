package eu.neclab.ngsildbroker.historyquerymanager.controller;

import com.github.jsonldjava.core.JsonLDService;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
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
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
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
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.QueryParam;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
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
	public Uni<RestResponse<Object>> queryTemporalEntities(HttpServerRequest request, @QueryParam("id") String ids,
			@QueryParam("type") String typeQuery, @QueryParam("idPattern") String idPattern,
			@QueryParam("attrs") String attrs, @QueryParam("q") String qInput, @QueryParam("csf") String csf,
			@QueryParam("geometry") String geometry, @QueryParam("georel") String georel,
			@QueryParam("coordinates") String coordinates, @QueryParam("geoproperty") String geoproperty,
			@QueryParam("timeproperty") String timeProperty, @QueryParam("timerel") String timerel,
			@QueryParam("scopeQ") String scopeQ, @QueryParam("timeAt") String timeAt,
			@QueryParam("endTimeAt") String endTimeAt, @QueryParam("lastN") @DefaultValue("-1") int lastN,
			@QueryParam("lang") String lang, @QueryParam("aggrMethods") String aggrMethods,
			@QueryParam("aggrPeriodDuration") String aggrPeriodDuration, @QueryParam(value = "limit") Integer limit,
			@QueryParam(value = "offset") int offset, @QueryParam(value = "entityMap") String qToken,
			@QueryParam(value = "options") String options, @QueryParam(value = "count") String countS,
			@QueryParam(value = "localOnly") String localOnlyS, @QueryParam("format") String format,
			@QueryParam("n") @DefaultValue("-1") int nInput,
			@QueryParam("offsetN") @DefaultValue("0") int offsetN,
			@QueryParam("orderN") @DefaultValue("ASC") String nOrderInput) {
		boolean localOnly;
		boolean count;
		String tenant = HttpUtils.getTenant(request);

		if (nInput != -1 && lastN != -1 && lastN != nInput) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(
					new ResponseException(ErrorType.BadRequestData,
							"Conflicting input in n and lastN. Please remove one"),
					tenant));
		}
		String nOrder;
		int n;
		if (lastN != -1) {
			nOrder = "DESC";
			n = lastN;
		} else {
			nOrder = nOrderInput;
			n = nInput;
		}
		try {
			localOnly = HttpUtils.parseBoolean(localOnlyS);
			count = HttpUtils.parseBoolean(countS);
		} catch (ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
		}
		int acceptHeader = HttpUtils.parseAcceptHeader(request.headers().getAll("Accept"));
		if (format != null && !format.isEmpty()) {
			options += "," + format;
		}
		String q;
		if (qInput != null) {
			try {
				q = URLDecoder.decode(request.absoluteURI().split("q=")[1].split("&")[0], "UTF-8");
			} catch (UnsupportedEncodingException e) {
				return Uni.createFrom()
						.item(HttpUtils.handleControllerExceptions(
								new ResponseException(ErrorType.BadRequestData, "failed to decode q query"),
								HttpUtils.getTenant(request)));
			}
		} else {
			q = null;
		}
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
			finalOptions = HttpUtils.parseOptionsAndFormat(options, null);
		} catch (ResponseException e) {
			return Uni.createFrom().failure(e);
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
					n, offsetN, nOrder, actualLimit, offset, count, localOnly, context, request).onItem()
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
			@PathParam("entityId") String entityId, @QueryParam("attrs") String attrs,
			@QueryParam("aggrMethods") String aggrMethods, @QueryParam("aggrPeriodDuration") String aggrPeriodDuration,
			@QueryParam("lang") String lang, @QueryParam("lastN") @DefaultValue("-1") int lastN,
			@QueryParam("localOnly") String localOnlyS, @QueryParam(value = "options") String optionsString,
			@QueryParam(value = "geometryProperty") String geometryProperty,
			@QueryParam("timeproperty") String timeProperty, @QueryParam("timerel") String timeRel,
			@QueryParam("timeAt") String timeAt, @QueryParam("endTimeAt") String endTimeAt,
			@QueryParam("format") String format, @QueryParam("n") @DefaultValue("-1") int nInput,
			@QueryParam("offsetN") @DefaultValue("0") int offsetN,
			@QueryParam("orderN") @DefaultValue("ASC") String nOrderInput) {
		boolean localOnly;
		try {
			localOnly = HttpUtils.parseBoolean(localOnlyS);
		} catch (ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
		}
		int acceptHeader = HttpUtils.parseAcceptHeader(request.headers().getAll("Accept"));
		if (format != null && !format.isEmpty()) {
			optionsString += "," + format;
		}
		if (acceptHeader != 1 && acceptHeader != 2) {
			return HttpUtils.getInvalidHeader();
		}
		if (nInput != -1 && lastN != -1 && lastN != nInput) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(
					new ResponseException(ErrorType.BadRequestData,
							"Conflicting input in n and lastN. Please remove one"),
					HttpUtils.getTenant(request)));
		}
		String nOrder;
		int n;
		if (lastN != -1) {
			nOrder = "DESC";
			n = lastN;
		} else {
			nOrder = nOrderInput;
			n = nInput;
		}

		List<Object> headerContext;
		headerContext = HttpUtils.getAtContext(request);

		Set<String> finalOptions;
		try {
			finalOptions = HttpUtils.parseOptionsAndFormat(optionsString, null);
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
