package eu.neclab.ngsildbroker.registryhandler.controller;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PATCH;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

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
import io.vertx.core.http.HttpServerRequest;

/**
 * 
 * @version 1.0
 * @date 20-Jul-2018
 */

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
	public RestResponse<Object> discoverCSource(HttpServerRequest request) {
		return QueryControllerFunctions.queryForEntries(csourceService, request, false, defaultLimit, maxLimit, false);
	}

	@POST
	public RestResponse<Object> registerCSource(HttpServerRequest request, String payload) {
		return EntryControllerFunctions.createEntry(csourceService, request, payload,
				AppConstants.CSOURCE_REG_CREATE_PAYLOAD, AppConstants.CSOURCE_URL, logger);
	}

	@Path("/{registrationId}")
	@GET
	public RestResponse<Object> getCSourceById(HttpServerRequest request, String registrationId) {
		try {
			logger.debug("get CSource() ::" + registrationId);
			HttpUtils.validateUri(registrationId);
			String tenantid = request.getHeader(NGSIConstants.TENANT_HEADER_FOR_INTERNAL_CHECK);
			return HttpUtils.generateReply(request, csourceService.getCSourceRegistrationById(tenantid, registrationId),
					AppConstants.REGISTRY_ENDPOINT);
		} catch (Exception exception) {
			return HttpUtils.handleControllerExceptions(exception);
		}
	}

	@Path("/{registrationId}")
	@PATCH
	public RestResponse<Object> updateCSource(HttpServerRequest request, String registrationId, String payload) {
		return EntryControllerFunctions.appendToEntry(csourceService, request, registrationId, payload, "",
				AppConstants.CSOURCE_REG_UPDATE_PAYLOAD, logger);
	}

	@Path("/{registrationId}")
	@DELETE
	public RestResponse<Object> deleteCSource(HttpServerRequest request, String registrationId) {
		return EntryControllerFunctions.deleteEntry(csourceService, request, registrationId, logger);
	}

}
