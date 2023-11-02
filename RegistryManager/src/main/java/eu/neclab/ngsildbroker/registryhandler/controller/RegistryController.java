package eu.neclab.ngsildbroker.registryhandler.controller;

import java.util.List;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.PATCH;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.QueryParam;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.resteasy.reactive.RestResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.jsonldjava.core.JsonLDService;
import com.google.common.collect.Sets;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.terms.AttrsQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.CSFQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.GeoQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.QQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.ScopeQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.TypeQueryTerm;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.QueryParser;
import eu.neclab.ngsildbroker.registryhandler.service.CSourceService;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpServerRequest;

/**
 * 
 * @version 1.0
 * @date 20-Jul-2018
 */
@Singleton
@Path("/ngsi-ld/v1/csourceRegistrations")
public class RegistryController {
	private final static Logger logger = LoggerFactory.getLogger(RegistryController.class);

	@Inject
	CSourceService csourceService;
	@ConfigProperty(name = "scorpio.entity.default-limit", defaultValue = "50")
	int defaultLimit;
	@ConfigProperty(name = "scorpio.entity.max-limit", defaultValue = "1000")
	int maxLimit;

	@ConfigProperty(name = "ngsild.corecontext", defaultValue = "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld")
	String coreContext;

	@Inject
	JsonLDService ldService;

	@GET
	public Uni<RestResponse<Object>> queryCSource(HttpServerRequest request, @QueryParam("id") String ids,
			@QueryParam("type") String type, @QueryParam("idPattern") String idPattern,
			@QueryParam("attrs") String attrs, @QueryParam("q") String q, @QueryParam("csf") String csf,
			@QueryParam("geometry") String geometry, @QueryParam("georel") String georel,
			@QueryParam("coordinates") String coordinates, @QueryParam("geoproperty") String geoproperty,
			@QueryParam("geometryProperty") String geometryProperty, @QueryParam("timeproperty") String timeProperty,
			@QueryParam("timerel") String timerel, @QueryParam("scopeQ") String scopeQ,
			@QueryParam("timeAt") String timeAt, @QueryParam("endTimeAt") String endTimeAt,
			@QueryParam(value = "limit") Integer limit, @QueryParam(value = "offset") int offset,
			@QueryParam(value = "options") String options, @QueryParam(value = "count") boolean count) {
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
		if (ids == null && type == null && attrs == null && geometry == null && q == null) {
			return Uni.createFrom()
					.item(HttpUtils.handleControllerExceptions(new ResponseException(ErrorType.BadRequestData)));
		}
		if(ids!=null) {
			try {
				HttpUtils.validateUri(ids);
			} catch (Exception e) {
				return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
			}
		}

		List<Object> headerContext = HttpUtils.getAtContext(request);
		return ldService.parse(headerContext).onItem().transformToUni(context -> {
			AttrsQueryTerm attrsQuery;
			TypeQueryTerm typeQueryTerm;
			QQueryTerm qQueryTerm;
			CSFQueryTerm csfQueryTerm;
			GeoQueryTerm geoQueryTerm;
			ScopeQueryTerm scopeQueryTerm;

			try {
				attrsQuery = QueryParser.parseAttrs(attrs, context);
				typeQueryTerm = QueryParser.parseTypeQuery(type, context);
				qQueryTerm = QueryParser.parseQuery(q, context);
				csfQueryTerm = QueryParser.parseCSFQuery(csf, context);
				geoQueryTerm = QueryParser.parseGeoQuery(georel, coordinates, geometry, geoproperty, context);
				scopeQueryTerm = QueryParser.parseScopeQuery(scopeQ);
			} catch (Exception e) {
				return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
			}

			return csourceService
					.queryRegistrations(HttpUtils.getTenant(request),
							ids == null ? null : Sets.newHashSet(ids.split(",")), typeQueryTerm, idPattern, attrsQuery,
							csfQueryTerm, geoQueryTerm, scopeQueryTerm, actualLimit, offset, count)
					.onItem().transformToUni(queryResult -> {
						return HttpUtils.generateQueryResult(request, queryResult, options, geometryProperty,
								acceptHeader, count, actualLimit, null, context, ldService);
					});
		}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);
	}

	@POST
	public Uni<RestResponse<Object>> registerCSource(HttpServerRequest request, String payload) {
		return HttpUtils.expandBody(request, payload, AppConstants.CSOURCE_REG_CREATE_PAYLOAD, ldService).onItem()
				.transformToUni(tuple -> {
					return csourceService.createRegistration(HttpUtils.getTenant(request), tuple.getItem2()).onItem()
							.transform(opResult -> {
								return HttpUtils.generateCreateResult(opResult, AppConstants.CSOURCE_URL);
							});
				}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);

	}

	@Path("/{registrationId}")
	@GET
	public Uni<RestResponse<Object>> getCSourceById(HttpServerRequest request,
			@PathParam("registrationId") String registrationId) {
		logger.debug("get CSource() ::" + registrationId);
		int acceptHeader = HttpUtils.parseAcceptHeader(request.headers().getAll("Accept"));
		if (acceptHeader == -1) {
			return HttpUtils.getInvalidHeader();
		}

		try {
			HttpUtils.validateUri(registrationId);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
		}
		List<Object> headerContext = HttpUtils.getAtContext(request);
		return ldService.parse(headerContext).onItem().transformToUni(context -> {
			return csourceService.retrieveRegistration(HttpUtils.getTenant(request), registrationId).onItem()
					.transformToUni(entity -> {
						return HttpUtils.generateEntityResult(headerContext, context, acceptHeader, entity, null, null,
								null, ldService);
					});
		}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);
	}

	@Path("/{registrationId}")
	@PATCH
	public Uni<RestResponse<Object>> updateCSource(HttpServerRequest request,
			@PathParam("registrationId") String registrationId, String payload) {
		return HttpUtils.expandBody(request, payload, AppConstants.CSOURCE_REG_UPDATE_PAYLOAD, ldService).onItem()
				.transformToUni(tuple -> {
					return csourceService
							.updateRegistration(HttpUtils.getTenant(request), registrationId, tuple.getItem2()).onItem()
							.transform(opResult -> {
								return HttpUtils.generateUpdateResultResponse(opResult);
							});
				}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);
	}

	@Path("/{registrationId}")
	@DELETE
	public Uni<RestResponse<Object>> deleteCSource(HttpServerRequest request,
			@PathParam("registrationId") String registrationId) {
		int acceptHeader = HttpUtils.parseAcceptHeader(request.headers().getAll("Accept"));
		if (acceptHeader == -1) {
			return HttpUtils.getInvalidHeader();
		}
		try {
			HttpUtils.validateUri(registrationId);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
		}
		return csourceService.deleteRegistration(HttpUtils.getTenant(request), registrationId).onItem()
				.transform(opResult -> {
					return HttpUtils.generateDeleteResult(opResult);
				}).onFailure().recoverWithItem(e -> HttpUtils.handleControllerExceptions(e));
	}

}
