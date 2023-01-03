package eu.neclab.ngsildbroker.historyquerymanager.controller;

import java.util.List;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.resteasy.reactive.RestResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Sets;

import eu.neclab.ngsildbroker.commons.datatypes.terms.AggrTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.AttrsQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.TemporalQueryTerm;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.QueryParser;
import eu.neclab.ngsildbroker.historyquerymanager.service.HistoryQueryService;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpServerRequest;

@Singleton
@Path("/ngsi-ld/v1/temporal/entities")
public class HistoryController {

	private final static Logger logger = LoggerFactory.getLogger(HistoryController.class);

	@Inject
	HistoryQueryService historyQueryService;
	@ConfigProperty(name = "atcontext.url")
	String atContextServerUrl;
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

	@PostConstruct
	public void init() {
		JsonLdProcessor.init(coreContext);
	}

	@GET
	public Uni<RestResponse<Object>> queryTemporalEntities(HttpServerRequest request,
			@QueryParam(value = "limit") Integer limit, @QueryParam(value = "offset") Integer offset,
			@QueryParam(value = "qtoken") String qToken, @QueryParam(value = "options") List<String> options,
			@QueryParam(value = "count") Boolean countResult) {
		return null;
		// return QueryControllerFunctions.queryForEntries(historyService, request,
		// true, defaultLimit, maxLimit, true);
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
			@QueryParam("timeAt") String timeAt, @QueryParam("endTimeAt") String endTimeAt) {
		ArrayListMultimap<String, String> headers = HttpUtils.getHeaders(request);
		int acceptHeader = HttpUtils.parseAcceptHeader(headers.get("Accept"));
		if (acceptHeader == -1) {
			return HttpUtils.INVALID_HEADER;
		}
		if (lastN == null) {
			lastN = defaultLastN;
		}

		Context context;
		List<Object> headerContext;
		AttrsQueryTerm attrsQuery;
		AggrTerm aggrQuery;
		TemporalQueryTerm tempQuery;
		try {
			HttpUtils.validateUri(entityId);
			headerContext = HttpUtils.getAtContextNoUni(request);
			context = JsonLdProcessor.getCoreContextClone().parse(headerContext, false);
			attrsQuery = QueryParser.parseAttrs(attrs, context);
			aggrQuery = QueryParser.parseAggrTerm(aggrMethods, aggrPeriodDuration);
			tempQuery = QueryParser.parseTempQuery(timeProperty, timeRel, timeAt, endTimeAt);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
		}
		return historyQueryService.retrieveEntity(HttpUtils.getTenant(request), entityId, attrsQuery, aggrQuery, tempQuery, lang,
				lastN.intValue(), localOnly, context).onItem().transform(entity -> {
					return HttpUtils.generateEntityResult(headerContext, context, acceptHeader, entity,
							geometryProperty, Sets.newHashSet(optionsString.split(",")));
				});

	}

}
