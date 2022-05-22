package eu.neclab.ngsildbroker.registryhandler.controller;

import java.util.HashMap;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PATCH;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.resteasy.reactive.RestResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.jsonldjava.core.JsonLdProcessor;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
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
	private CSourceService csourceService;

	@ConfigProperty(name = "scorpio.entity.default-limit", defaultValue = "50")
	private int defaultLimit;
	@ConfigProperty(name = "scorpio.entity.max-limit", defaultValue = "1000")
	private int maxLimit;

	@ConfigProperty(name = "ngsild.corecontext", defaultValue = "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld")
	private String coreContext;

	@PostConstruct
	public void init() {
		JsonLdProcessor.init(coreContext);
	}

	@GET
	public Uni<RestResponse<Object>> discoverCSource(HttpServerRequest request, HashMap<String, String> queryMap,
			@QueryParam(value = "limit") Integer limit, @QueryParam(value = "offset") Integer offset,
			@QueryParam(value = "qtoken") String qToken, @QueryParam(value = "count") boolean count) {
		return QueryControllerFunctions.queryForEntries(csourceService, request, false, defaultLimit, maxLimit, false);
	}

	@POST
	public Uni<RestResponse<Object>> registerCSource(HttpServerRequest request, String payload) {
		return EntryControllerFunctions.createEntry(csourceService, request, payload,
				AppConstants.CSOURCE_REG_CREATE_PAYLOAD, AppConstants.CSOURCE_URL, logger);
	}

	@Path("/{registrationId}")
	@GET
	public Uni<RestResponse<Object>> getCSourceById(HttpServerRequest request,
			@PathParam("registrationId") String registrationId) {
		logger.debug("get CSource() ::" + registrationId);
		return QueryControllerFunctions.getEntity(csourceService, request, null, null, registrationId, false, defaultLimit, maxLimit);
//		
//		return HttpUtils.validateUri(registrationId).onItem().transformToUni(t -> {
//			String tenantid = request.getHeader(NGSIConstants.TENANT_HEADER_FOR_INTERNAL_CHECK);
//			return csourceService.getCSourceRegistrationById(tenantid, registrationId);
//		}).onItem().transformToUni(t -> {
//			return HttpUtils.generateReply(request, t, AppConstants.REGISTRY_ENDPOINT);
//		}).onFailure().recoverWithItem(e -> {
//			return HttpUtils.handleControllerExceptions(e);
//		});
	}

	@Path("/{registrationId}")
	@PATCH
	public Uni<RestResponse<Object>> updateCSource(HttpServerRequest request,
			@PathParam("registrationId") String registrationId, String payload) {
		return EntryControllerFunctions.appendToEntry(csourceService, request, registrationId, payload, "",
				AppConstants.CSOURCE_REG_UPDATE_PAYLOAD, logger);
	}

	@Path("/{registrationId}")
	@DELETE
	public Uni<RestResponse<Object>> deleteCSource(HttpServerRequest request,
			@PathParam("registrationId") String registrationId) {
		return EntryControllerFunctions.deleteEntry(csourceService, request, registrationId, logger);
	}

}
