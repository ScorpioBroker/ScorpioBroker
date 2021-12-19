package eu.neclab.ngsildbroker.registryhandler.controller;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.server.reactive.ServerHttpRequest;
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.CSourceRegistration;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.datatypes.QueryResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.ngsiqueries.ParamsResolver;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.Validator;
import eu.neclab.ngsildbroker.registryhandler.repository.CSourceDAO;
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
	private final static String MY_REQUEST_MAPPING = "/ngsi-ld/v1/csourceRegistrations";
	private final static String MY_REQUEST_MAPPING_ALT = "/ngsi-ld/v1/csourceRegistrations/";

	@Autowired
	CSourceService csourceService;

	@Autowired
	CSourceDAO csourceDAO;
	@Autowired
	ObjectMapper objectMapper;
	private JsonLdOptions opts = new JsonLdOptions(JsonLdOptions.JSON_LD_1_1);

	@Value("${ngsild.corecontext:https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld}")
	String coreContext;

	@PostConstruct
	public void init() {
		JsonLdProcessor.init(coreContext);
	}

	@GetMapping
	public ResponseEntity<byte[]> discoverCSource(ServerHttpRequest request,
			@RequestParam HashMap<String, String> queryMap,
			@RequestParam(required = false, name = "limit", defaultValue = "0") int limit,
			@RequestParam(value = "offset", required = false) Integer offset,
			@RequestParam(value = "qtoken", required = false) String qToken) {
		try {
			logger.trace("getCSources() ::");
			MultiValueMap<String, String> params = request.getQueryParams();
			Validator.validateCsourceGetParameter(params);

			String tenantid = request.getHeaders().getFirst(NGSIConstants.TENANT_HEADER);
			if ((request.getPath().toString().equals(MY_REQUEST_MAPPING)
					|| request.getPath().toString().equals(MY_REQUEST_MAPPING_ALT)) && !params.isEmpty()) {

				List<Object> linkHeaders = HttpUtils.getAtContext(request);
				Context context = JsonLdProcessor.getCoreContextClone();
				context = context.parse(linkHeaders, true);
				QueryParams qp = ParamsResolver.getQueryParamsFromUriQuery(request.getQueryParams(), context);
				if (offset == null) {
					offset = 0;
				}
				if (qp == null) // invalid query
					throw new ResponseException(ErrorType.InvalidRequest);
				qp.setTenant(tenantid);
				qp.setLimit(limit);
				qp.setOffSet(offset);
				QueryResult queryResult = csourceDAO.query(qp);
				String nextLink = HttpUtils.generateNextLink(request, queryResult);
				String prevLink = HttpUtils.generatePrevLink(request, queryResult);
				ArrayList<String> additionalLinks = new ArrayList<String>();
				if (nextLink != null) {
					additionalLinks.add(nextLink);
				}
				if (prevLink != null) {
					additionalLinks.add(prevLink);
				}
				HashMap<String, List<String>> additionalHeaders = new HashMap<String, List<String>>();
				if (!additionalLinks.isEmpty()) {
					additionalHeaders.put(HttpHeaders.LINK, additionalLinks);
				}
				List<String> csourceList = queryResult.getActualDataString();
//				if (csourceList.size() > 0) {
				return HttpUtils.generateReply(request, csourceDAO.getListAsJsonArray(csourceList), AppConstants.REGISTRY_ENDPOINT);
//				} else {
//					//TODO this needs to be change to respect query results
//					throw new ResponseException(ErrorType.NotFound);
//				}
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
	public ResponseEntity<byte[]> registerCSource(ServerHttpRequest request,
			@RequestBody(required = false) String payload) {
		try {

			logger.debug("payload received :: " + payload);

			this.validate(payload);
			List<Object> linkHeaders = HttpUtils.getAtContext(request);
			boolean atContextAllowed = HttpUtils.doPreflightCheck(request, linkHeaders);
			// TODO change this to remove deserialization
			String resolved = JsonUtils.toString(JsonLdProcessor.expand(linkHeaders, JsonUtils.fromString(payload),
					opts, AppConstants.CSOURCE_REG_CREATE_PAYLOAD, atContextAllowed).get(0));

			logger.debug("Resolved payload::" + resolved);
			CSourceRegistration csourceRegistration = DataSerializer.getCSourceRegistration(resolved);
			logger.debug("Csource :: " + csourceRegistration);
			URI uri = csourceService.registerCSource(HttpUtils.getHeaders(request), csourceRegistration);

			return ResponseEntity.status(HttpStatus.CREATED).header("location", AppConstants.CSOURCE_URL + uri).build();
		} catch (Exception exception) {
			return HttpUtils.handleControllerExceptions(exception);
		}
	}

	@GetMapping("{registrationId}")
	public ResponseEntity<byte[]> getCSourceById(ServerHttpRequest request,
			@PathVariable("registrationId") String registrationId) {
		try {
			logger.debug("get CSource() ::" + registrationId);
			HttpUtils.validateUri(registrationId);
			String tenantid = request.getHeaders().getFirst(NGSIConstants.TENANT_HEADER);
			List<String> csourceList = new ArrayList<String>();

			csourceList.add(DataSerializer.toJson(csourceService.getCSourceRegistrationById(tenantid, registrationId)));
			return HttpUtils.generateReply(request, csourceDAO.getListAsJsonArray(csourceList), AppConstants.REGISTRY_ENDPOINT);
		} catch (Exception exception) {
			return HttpUtils.handleControllerExceptions(exception);
		}
	}

	@PatchMapping("{registrationId}")
	public ResponseEntity<byte[]> updateCSource(ServerHttpRequest request,
			@PathVariable("registrationId") String registrationId, @RequestBody String payload) {
		try {
			logger.debug("update CSource() ::" + registrationId);
			List<Object> linkHeaders = HttpUtils.getAtContext(request);
			boolean atContextAllowed = HttpUtils.doPreflightCheck(request, linkHeaders);
			String resolved = JsonUtils.toString(JsonLdProcessor.expand(linkHeaders, JsonUtils.fromString(payload),
					opts, AppConstants.CSOURCE_REG_CREATE_PAYLOAD, atContextAllowed).get(0));
			csourceService.updateCSourceRegistration(HttpUtils.getHeaders(request), registrationId, resolved);
			logger.debug("update CSource request completed::" + registrationId);
			return ResponseEntity.noContent().build();
		} catch (Exception exception) {
			return HttpUtils.handleControllerExceptions(exception);
		}
	}

	@DeleteMapping("{registrationId}")
	public ResponseEntity<byte[]> deleteCSource(ServerHttpRequest request,
			@PathVariable("registrationId") String registrationId) {
		try {
			HttpUtils.validateUri(registrationId);
			logger.debug("delete CSource() ::" + registrationId);
			csourceService.deleteCSourceRegistration(HttpUtils.getHeaders(request), registrationId);
			logger.debug("delete CSource() completed::" + registrationId);
			return ResponseEntity.noContent().build();
		} catch (Exception exception) {
			return HttpUtils.handleControllerExceptions(exception);
		}
	}

	private void validate(String payload) throws ResponseException {
		logger.trace("validation :: started");
		if (payload == null) {
			throw new ResponseException(ErrorType.UnprocessableEntity);
		}
		JsonNode json = null;
		try {
			json = objectMapper.readTree(payload);
			if (json.isNull()) {
				throw new ResponseException(ErrorType.UnprocessableEntity);
			}

		} catch (JsonProcessingException e) {
			throw new ResponseException(ErrorType.BadRequestData);
		}
		logger.trace("validation :: completed");
	}
}
