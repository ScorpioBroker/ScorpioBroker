package eu.neclab.ngsildbroker.registryhandler.controller;

import java.util.HashMap;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.github.jsonldjava.core.JsonLdProcessor;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.controllers.EntryControllerFunctions;
import eu.neclab.ngsildbroker.commons.controllers.QueryControllerFunctions;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.registryhandler.service.CSourceService;

/**
 * 
 * @version 1.0
 * @date 20-Jul-2018
 */
@RestController
@RequestMapping("/ngsi-ld/v1/csourceRegistrations")
public class RegistryController {
	private final static Logger logger = LoggerFactory.getLogger(RegistryController.class);

	@Autowired
	private CSourceService csourceService;

	@Value("${scorpio.entity.default-limit:50}")
	private int defaultLimit;
	@Value("${scorpio.entity.max-limit:1000}")
	private int maxLimit;

	@Value("${ngsild.corecontext:https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld}")
	private String coreContext;

	@PostConstruct
	public void init() {
		JsonLdProcessor.init(coreContext);
	}

	@GetMapping
	public ResponseEntity<String> discoverCSource(HttpServletRequest request,
			@RequestParam HashMap<String, String> queryMap,
			@RequestParam(required = false, name = "limit") Integer limit,
			@RequestParam(value = "offset", required = false) Integer offset,
			@RequestParam(value = "qtoken", required = false) String qToken,
			@RequestParam(value = "count", required = false) boolean count) {
		return QueryControllerFunctions.queryForEntries(csourceService, request, false, defaultLimit, maxLimit, false);
	}

	@PostMapping
	public ResponseEntity<String> registerCSource(HttpServletRequest request,
			@RequestBody(required = false) String payload) {
		return EntryControllerFunctions.createEntry(csourceService, request, payload,
				AppConstants.CSOURCE_REG_CREATE_PAYLOAD, AppConstants.CSOURCE_URL, logger);
	}

	@GetMapping("/{registrationId}")
	public ResponseEntity<String> getCSourceById(HttpServletRequest request,
			@PathVariable("registrationId") String registrationId) {
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

	@PatchMapping("/{registrationId}")
	public ResponseEntity<String> updateCSource(HttpServletRequest request,
			@PathVariable("registrationId") String registrationId, @RequestBody String payload) {
		return EntryControllerFunctions.appendToEntry(csourceService, request, registrationId, payload, "",
				AppConstants.CSOURCE_REG_UPDATE_PAYLOAD, logger);
	}

	@DeleteMapping("/{registrationId}")
	public ResponseEntity<String> deleteCSource(HttpServletRequest request,
			@PathVariable("registrationId") String registrationId) {
		return EntryControllerFunctions.deleteEntry(csourceService, request, registrationId, logger);
	}

}
