package eu.neclab.ngsildbroker.registryhandler.controller;

import java.util.HashMap;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLdProcessor;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.datatypes.QueryResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.ngsiqueries.ParamsResolver;
import eu.neclab.ngsildbroker.commons.tools.ControllerFunctions;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.Validator;
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
	CSourceService csourceService;

	@Value("${ngsild.corecontext:https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld}")
	String coreContext;

	@PostConstruct
	public void init() {
		JsonLdProcessor.init(coreContext);
	}

	@GetMapping
	public ResponseEntity<String> discoverCSource(HttpServletRequest request,
			@RequestParam HashMap<String, String> queryMap,
			@RequestParam(required = false, name = "limit", defaultValue = "0") int limit,
			@RequestParam(value = "offset", required = false) Integer offset,
			@RequestParam(value = "qtoken", required = false) String qToken,
			@RequestParam(value = "count", required = false) boolean count) {
		try {
			logger.trace("getCSources() ::");
			MultiValueMap<String, String> params = HttpUtils.getQueryParamMap(request);
			Validator.validateCsourceGetParameter(params);

			String tenantid = request.getHeader(NGSIConstants.TENANT_HEADER);
			if (!params.isEmpty()) {

				List<Object> linkHeaders = HttpUtils.getAtContext(request);
				Context context = JsonLdProcessor.getCoreContextClone();
				context = context.parse(linkHeaders, true);
				QueryParams qp = ParamsResolver.getQueryParamsFromUriQuery(params, context);
				if (offset == null) {
					offset = 0;
				}
				if (qp == null) // invalid query
					throw new ResponseException(ErrorType.InvalidRequest, "Empty query params");
				qp.setTenant(tenantid);
				qp.setLimit(limit);
				qp.setOffSet(offset);
				QueryResult queryResult = csourceService.query(qp);

				return HttpUtils.generateReply(request, queryResult, true, count, context, linkHeaders,
						AppConstants.REGISTRY_ENDPOINT);
			} else {
				// spec v0.9.0 section 5.10.2.4: if neither Entity types nor Attribute names are
				// provided, an error of BadRequestData shall be raised
				throw new ResponseException(ErrorType.BadRequestData,
						"You must provide at least type or attrs as parameter");
			}
		} catch (Exception exception) {
			return HttpUtils.handleControllerExceptions(exception);
		}
	}

	@PostMapping
	public ResponseEntity<String> registerCSource(HttpServletRequest request,
			@RequestBody(required = false) String payload) {
		return ControllerFunctions.createEntry(csourceService, request, payload, logger);
	}

	@GetMapping("/{registrationId}")
	public ResponseEntity<String> getCSourceById(HttpServletRequest request,
			@PathVariable("registrationId") String registrationId) {
		try {
			logger.debug("get CSource() ::" + registrationId);
			HttpUtils.validateUri(registrationId);
			String tenantid = request.getHeader(NGSIConstants.TENANT_HEADER);
			return HttpUtils.generateReply(request, csourceService.getCSourceRegistrationById(tenantid, registrationId),
					AppConstants.REGISTRY_ENDPOINT);
		} catch (Exception exception) {
			return HttpUtils.handleControllerExceptions(exception);
		}
	}

	@PatchMapping("/{registrationId}")
	public ResponseEntity<String> updateCSource(HttpServletRequest request,
			@PathVariable("registrationId") String registrationId, @RequestBody String payload) {
		return ControllerFunctions.appendToEntry(csourceService, request, registrationId, payload, "", logger);
	}

	@DeleteMapping("/{registrationId}")
	public ResponseEntity<String> deleteCSource(HttpServletRequest request,
			@PathVariable("registrationId") String registrationId) {
		return ControllerFunctions.deleteEntity(csourceService, request, registrationId, logger);
	}

}
