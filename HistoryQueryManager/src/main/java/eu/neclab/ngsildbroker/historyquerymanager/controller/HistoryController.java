package eu.neclab.ngsildbroker.historyquerymanager.controller;

import com.github.jsonldjava.core.JsonLDService;
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
import eu.neclab.ngsildbroker.commons.tools.QueryParser;
import eu.neclab.ngsildbroker.historyquerymanager.service.HistoryQueryService;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpServerRequest;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.resteasy.reactive.RestResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.QueryParam;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.List;

@Singleton
@Path("/ngsi-ld/v1/temporal/entities")
public class HistoryController {

	private final static Logger logger = LoggerFactory.getLogger(HistoryController.class);

	@Inject
	HistoryQueryService historyQueryService;
	@ConfigProperty(name = "scorpio.history.defaultLimit", defaultValue = "50")
	int defaultLimit;
	@ConfigProperty(name = "scorpio.history.maxLimit", defaultValue = "1000")
	int maxLimit;
	@ConfigProperty(name = "scorpio.history.lastN", defaultValue = "50")
	int defaultLastN;
	@ConfigProperty(name = "scorpio.history.maxLastN", defaultValue = "1000")
	int maxLastN;
	@ConfigProperty(name = "ngsild.corecontext", defaultValue = "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld")
	String coreContext;

	@Inject
	JsonLDService ldService;

	@GET
	@Blocking
	public Uni<RestResponse<Object>> queryTemporalEntities(HttpServerRequest request, @QueryParam("id") String ids,
			@QueryParam("type") String typeQuery, @QueryParam("idPattern") String idPattern,
			@QueryParam("attrs") String attrs, @QueryParam("q") String qInput, @QueryParam("csf") String csf,
			@QueryParam("geometry") String geometry, @QueryParam("georel") String georel,
			@QueryParam("coordinates") String coordinates, @QueryParam("geoproperty") String geoproperty,
			@QueryParam("timeproperty") String timeProperty, @QueryParam("timerel") String timerel,
			@QueryParam("scopeQ") String scopeQ, @QueryParam("timeAt") String timeAt,
			@QueryParam("endTimeAt") String endTimeAt, @QueryParam("lastN") Integer lastN,
			@QueryParam("lang") String lang, @QueryParam("aggrMethods") String aggrMethods,
			@QueryParam("aggrPeriodDuration") String aggrPeriodDuration, @QueryParam(value = "limit") Integer limit,
			@QueryParam(value = "offset") int offset, @QueryParam(value = "entityMap") String qToken,
			@QueryParam(value = "options") String options, @QueryParam(value = "count") boolean count,
			@QueryParam(value = "localOnly") boolean localOnly,@QueryParam("format") String format) {
		int acceptHeader = HttpUtils.parseAcceptHeader(request.headers().getAll("Accept"));
		if(format!=null && !format.isEmpty()){
			options+=","+format;
		}
		String q;
		if (qInput != null) {
			try {
				q = URLDecoder.decode(request.absoluteURI().split("q=")[1].split("&")[0], "UTF-8");
			} catch (UnsupportedEncodingException e) {
				return Uni.createFrom().item(HttpUtils.handleControllerExceptions(
						new ResponseException(ErrorType.BadRequestData, "failed to decode q query")));
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
			return Uni.createFrom()
					.item(HttpUtils.handleControllerExceptions(new ResponseException(ErrorType.TooManyResults)));
		}
		if (!localOnly && typeQuery == null && attrs == null && geometry == null && q == null) {
			return Uni.createFrom()
					.item(HttpUtils.handleControllerExceptions(new ResponseException(ErrorType.InvalidRequest)));
		}
		int lastNTBU;
		if (lastN == null) {
			lastNTBU = defaultLastN;
		} else {
			lastNTBU = lastN;
		}
		List<Object> ctx = HttpUtils.getAtContext(request);
		String finalOptions = options;
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
				return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
			}
			return historyQueryService
					.query(HttpUtils.getTenant(request), idList, typeQueryTerm, idPattern, attrsQueryTerm, qQueryTerm,
							csfQueryTerm, geoQueryTerm, scopeQueryTerm, temporalQueryTerm, aggrTerm, languageQueryTerm,
							lastNTBU, actualLimit, offset, count, localOnly, context, request)
					.onItem().transformToUni(queryResult -> {
						return HttpUtils.generateQueryResult(request, queryResult, finalOptions, geoproperty, acceptHeader,
								count, actualLimit, languageQueryTerm, context, ldService,null,null,true);
					});
		}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);
	}

	@Path("/{entityId}")
	@GET

	public Uni<RestResponse<Object>> retrieveTemporalEntity(HttpServerRequest request,
			@PathParam("entityId") String entityId, @QueryParam("attrs") String attrs,
			@QueryParam("aggrMethods") String aggrMethods, @QueryParam("aggrPeriodDuration") String aggrPeriodDuration,
			@QueryParam("lang") String lang, @QueryParam("lastN") Integer lastN,
			@QueryParam("localOnly") boolean localOnly, @QueryParam(value = "options") String optionsString,
			@QueryParam(value = "geometryProperty") String geometryProperty,
			@QueryParam("timeproperty") String timeProperty, @QueryParam("timerel") String timeRel,
			@QueryParam("timeAt") String timeAt, @QueryParam("endTimeAt") String endTimeAt,@QueryParam("format") String format) {

		int acceptHeader = HttpUtils.parseAcceptHeader(request.headers().getAll("Accept"));
		if(format!=null && !format.isEmpty()){
			optionsString+=","+format;
		}
		if (acceptHeader != 1 && acceptHeader != 2) {
			return HttpUtils.getInvalidHeader();
		}
		int lastNTBU;
		if (lastN == null) {
			lastNTBU = defaultLastN;
		} else {
			lastNTBU = lastN;
		}

		List<Object> headerContext;
		headerContext = HttpUtils.getAtContext(request);
		String finalOptionsString = optionsString;
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
				return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
			}
			return historyQueryService.retrieveEntity(HttpUtils.getTenant(request), entityId, attrsQuery, aggrQuery,
					tempQuery, lang, lastNTBU, localOnly, context,request.headers()).onItem().transformToUni(entity -> {
						return HttpUtils.generateEntityResult(headerContext, context, acceptHeader, entity,
								geometryProperty, finalOptionsString, null, ldService,null,null,true);
					});
		}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);

	}

}
