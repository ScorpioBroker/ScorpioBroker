package eu.neclab.ngsildbroker.registryhandler.controller;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PATCH;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

import com.github.jsonldjava.core.JsonLdProcessor;
import com.google.common.collect.ArrayListMultimap;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.resteasy.reactive.RestResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.controllers.EntryControllerFunctions;
import eu.neclab.ngsildbroker.commons.controllers.QueryControllerFunctions;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
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

	@PostConstruct
	public void init() {
		JsonLdProcessor.init(coreContext);
	}

	@GET
	public Uni<RestResponse<Object>> discoverCSource(HttpServerRequest request,
			@QueryParam(value = "limit") Integer limit, @QueryParam(value = "offset") Integer offset,
			@QueryParam(value = "qtoken") String qToken, @QueryParam(value = "count") boolean count) {
		return QueryControllerFunctions.queryForEntries(csourceService, request, false, defaultLimit, maxLimit, false);
	}

	@POST
	public Uni<RestResponse<Object>> registerCSource(HttpServerRequest request, String payload) {
		return EntryControllerFunctions.createEntry(csourceService, request, payload,
				AppConstants.CSOURCE_REG_CREATE_PAYLOAD, AppConstants.CSOURCE_URL, logger, false);
	}

	@Path("/{registrationId}")
	@GET
	public Uni<RestResponse<Object>> getCSourceById(HttpServerRequest request,
			@PathParam("registrationId") String registrationId) {
		logger.debug("get CSource() ::" + registrationId);
		ArrayListMultimap<String, String> headers = HttpUtils.getHeaders(request);
		return HttpUtils.validateUri(registrationId).onItem().transformToUni(t1 -> {
			String tenant = HttpUtils.getTenantFromHeaders(headers);
			return csourceService.getRegistrationById(registrationId, tenant).onItem().transformToUni(t -> {
				return HttpUtils.generateReply(request, t, AppConstants.CSOURCE_URL_ID, null);
			});
		}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);
	}

	@Path("/{registrationId}")
	@PATCH
	public Uni<RestResponse<Object>> updateCSource(HttpServerRequest request,
			@PathParam("registrationId") String registrationId, String payload) {
		return EntryControllerFunctions.appendToEntry(csourceService, request, registrationId, payload, "",
				AppConstants.CSOURCE_REG_UPDATE_PAYLOAD, logger, false);
	}

	@Path("/{registrationId}")
	@DELETE
	public Uni<RestResponse<Object>> deleteCSource(HttpServerRequest request,
			@PathParam("registrationId") String registrationId) {
		return EntryControllerFunctions.deleteEntry(csourceService, request, registrationId, logger);
	}

}
