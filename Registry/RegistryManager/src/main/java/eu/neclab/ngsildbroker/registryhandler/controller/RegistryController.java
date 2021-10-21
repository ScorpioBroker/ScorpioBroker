package eu.neclab.ngsildbroker.registryhandler.controller;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.discovery.EurekaClient;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.CSourceRegistration;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.datatypes.QueryResult;
import eu.neclab.ngsildbroker.commons.datatypes.RestResponse;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.ldcontext.ContextResolverBasic;
import eu.neclab.ngsildbroker.commons.ngsiqueries.ParamsResolver;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
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
	EurekaClient eurekaClient;
	@Autowired
	CSourceService csourceService;
	@Autowired
	@Qualifier("rmconRes")
	ContextResolverBasic contextResolver;
	@Autowired
	@Qualifier("rmparamsResolver")
	ParamsResolver paramsResolver;
	@Autowired
	CSourceDAO csourceDAO;
	@Autowired
	ObjectMapper objectMapper;
	private HttpUtils httpUtils;

	@PostConstruct
	private void setup() {
		this.httpUtils = HttpUtils.getInstance(contextResolver);
	}

	// @GetMapping
	// public ResponseEntity<byte[]> discoverCSource(HttpServletRequest request,
	// @RequestParam HashMap<String, String> queryMap) {
	// try {
	// return ResponseEntity.status(HttpStatus.OK)
	// .body(csourceService.getCSourceRegistrations(queryMap));
	// } catch (ResponseException exception) {
	// return ResponseEntity.status(exception.getHttpStatus()).body(new
	// RestResponse(exception));
	// } catch (Exception e) {
	// return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
	// .body(new RestResponse(HttpStatus.INTERNAL_SERVER_ERROR, "Internal server
	// error",
	// "Internal error")));
	// }
	// }

	@GetMapping
	public ResponseEntity<byte[]> discoverCSource(HttpServletRequest request,
			@RequestParam HashMap<String, String> queryMap,
			@RequestParam(required = false, name = "limit", defaultValue = "0") int limit) {
		try {
			logger.trace("getCSources() ::");
			String queryParams = request.getQueryString();
			String tenantid = request.getHeader(NGSIConstants.TENANT_HEADER);
			if ((request.getRequestURI().equals(MY_REQUEST_MAPPING)
					|| request.getRequestURI().equals(MY_REQUEST_MAPPING_ALT)) && queryParams != null) {

				List<Object> linkHeaders = HttpUtils.parseLinkHeader(request, NGSIConstants.HEADER_REL_LDCONTEXT);
				QueryParams qp = paramsResolver.getQueryParamsFromUriQuery(request.getParameterMap(), linkHeaders);
				if (qp == null) // invalid query
					throw new ResponseException(ErrorType.InvalidRequest);
				qp.setTenant(tenantid);
				qp.setLimit(limit);
				QueryResult queryResult = csourceDAO.query(qp);
				List<String> csourceList = queryResult.getActualDataString();
				if (csourceList.size() > 0) {
					return httpUtils.generateReply(request, csourceDAO.getListAsJsonArray(csourceList));
				} else {
					throw new ResponseException(ErrorType.NotFound);
				}
			} else {
				// spec v0.9.0 section 5.10.2.4: if neither Entity types nor Attribute names are
				// provided, an error of BadRequestData shall be raised
				throw new ResponseException(ErrorType.BadRequestData,
						"You must provide at least type or attrs as parameter");
			}
		} catch (ResponseException exception) {
			logger.error("Exception ::", exception);
			return ResponseEntity.status(exception.getHttpStatus()).body(new RestResponse(exception).toJsonBytes());
		} catch (Exception exception) {
			logger.error("Exception ::", exception);
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
					.body(new RestResponse(ErrorType.InternalError, exception.getLocalizedMessage()).toJsonBytes());
		}
	}

	@PostMapping
	public ResponseEntity<byte[]> registerCSource(HttpServletRequest request,
			@RequestBody(required = false) String payload) {
		try {
			HttpUtils.doPreflightCheck(request, payload);
			logger.debug("payload received :: " + payload);

			this.validate(payload);

			String resolved = httpUtils.expandPayload(request, payload, AppConstants.CSOURCE_URL_ID);

			logger.debug("Resolved payload::" + resolved);
			CSourceRegistration csourceRegistration = DataSerializer.getCSourceRegistration(resolved);
			logger.debug("Csource :: " + csourceRegistration);
			URI uri = csourceService.registerCSource(HttpUtils.getHeaders(request), csourceRegistration);

			return ResponseEntity.status(HttpStatus.CREATED).header("location", AppConstants.CSOURCE_URL + uri).build();
		} catch (ResponseException exception) {
			return ResponseEntity.status(exception.getHttpStatus()).body(new RestResponse(exception).toJsonBytes());
		} catch (Exception e) {
			e.printStackTrace();
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
					.body(new RestResponse(ErrorType.InternalError, e.getLocalizedMessage()).toJsonBytes());
		}
	}

	@GetMapping("{registrationId}")
	public ResponseEntity<byte[]> getCSourceById(HttpServletRequest request,
			@PathVariable("registrationId") String registrationId) {
		try {
			logger.debug("get CSource() ::" + registrationId);
			String tenantid = request.getHeader(NGSIConstants.TENANT_HEADER);
			List<String> csourceList = new ArrayList<String>();
			csourceList.add(DataSerializer.toJson(csourceService.getCSourceRegistrationById(tenantid, registrationId)));
			return httpUtils.generateReply(request, csourceDAO.getListAsJsonArray(csourceList));
		} catch (ResponseException exception) {
			return ResponseEntity.status(exception.getHttpStatus()).body(new RestResponse(exception).toJsonBytes());
		} catch (Exception e) {
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
					.body(new RestResponse(ErrorType.InternalError, e.getLocalizedMessage()).toJsonBytes());
		}
	}

	@PatchMapping("{registrationId}")
	public ResponseEntity<byte[]> updateCSource(HttpServletRequest request,
			@PathVariable("registrationId") String registrationId, @RequestBody String payload) {
		try {
			HttpUtils.doPreflightCheck(request, payload);
			logger.debug("update CSource() ::" + registrationId);
			String resolved = httpUtils.expandPayload(request, payload, AppConstants.CSOURCE_URL_ID);

			csourceService.updateCSourceRegistration(HttpUtils.getHeaders(request), registrationId, resolved);
			logger.debug("update CSource request completed::" + registrationId);
			return ResponseEntity.noContent().build();
		} catch (ResponseException exception) {
			return ResponseEntity.status(exception.getHttpStatus()).body(new RestResponse(exception).toJsonBytes());
		} catch (Exception e) {
			e.printStackTrace();
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
					.body(new RestResponse(ErrorType.InternalError, e.getLocalizedMessage()).toJsonBytes());
		}
	}

	@DeleteMapping("{registrationId}")
	public ResponseEntity<byte[]> deleteCSource(HttpServletRequest request,
			@PathVariable("registrationId") String registrationId) {
		try {
			logger.debug("delete CSource() ::" + registrationId);
			csourceService.deleteCSourceRegistration(HttpUtils.getHeaders(request), registrationId);
			logger.debug("delete CSource() completed::" + registrationId);
			return ResponseEntity.noContent().build();
		} catch (ResponseException exception) {
			return ResponseEntity.status(exception.getHttpStatus()).body(new RestResponse(exception).toJsonBytes());
		} catch (Exception e) {
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
					.body(new RestResponse(ErrorType.InternalError, e.getLocalizedMessage()).toJsonBytes());
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
//			if (json.get(NGSIConstants.QUERY_PARAMETER_ID) == null) {
//				if(json.isObject()) {
//					((ObjectNode)json).set(NGSIConstants.QUERY_PARAMETER_ID, new TextNode(generateUniqueRegId(payload)));
//				}else {
//					throw new ResponseException(ErrorType.BadRequestData);
//				}
//				
//			}
		} catch (JsonParseException e) {
			throw new ResponseException(ErrorType.BadRequestData);
		} catch (IOException e) {
			throw new ResponseException(ErrorType.BadRequestData);
		}
		logger.trace("validation :: completed");
	}

}
