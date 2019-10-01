package eu.neclab.ngsildbroker.registryhandler.controller;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.netflix.discovery.EurekaClient;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.CSourceRegistration;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.datatypes.RestResponse;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
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
public class RegistryController {
	private final static Logger logger = LoggerFactory.getLogger(RegistryController.class);

	@Autowired
	EurekaClient eurekaClient;
	@Autowired
	CSourceService csourceService;
	@Autowired
	ContextResolverBasic contextResolver;
	@Autowired
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
	// public ResponseEntity<Object> discoverCSource(HttpServletRequest request,
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
	// Collections.singletonList("Internal Server Error")));
	// }
	// }

	@GetMapping
	public ResponseEntity<Object> discoverCSource(HttpServletRequest request,
			@RequestParam HashMap<String, String> queryMap) {
		try {
			logger.trace("getCSources() ::");
			String queryParams = request.getQueryString();
			if (request.getRequestURI().equals(NGSIConstants.CHECK_QUERY_STRING_URI) && queryParams != null) {

				List<Object> linkHeaders = HttpUtils.parseLinkHeader(request, NGSIConstants.HEADER_REL_LDCONTEXT);
				QueryParams qp = paramsResolver.getQueryParamsFromUriQuery(request.getParameterMap(), linkHeaders);
				if (qp == null) // invalid query
					throw new ResponseException(ErrorType.InvalidRequest);
				List<String> csourceList = csourceDAO.query(qp);
				if (csourceList.size() > 0) {
					return httpUtils.generateReply(request, csourceDAO.getListAsJsonArray(csourceList));
				} else {
					throw new ResponseException(ErrorType.ResourceNotFound);
				}
			} else {
				// spec v0.9.0 section 5.10.2.4: if neither Entity types nor Attribute names are
				// provided, an error of BadRequestData shall be raised
				throw new ResponseException(ErrorType.BadRequestData);
			}
		} catch (ResponseException exception) {
			logger.error("Exception ::", exception);
			return ResponseEntity.status(exception.getHttpStatus()).body(new RestResponse(exception));
		} catch (Exception exception) {
			logger.error("Exception ::", exception);
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
					.body(new RestResponse(HttpStatus.INTERNAL_SERVER_ERROR, "Internal server error",
							Collections.singletonList("Internal Server Error")));
		}
	}

	@PostMapping
	public ResponseEntity<Object> registerCSource(HttpServletRequest request,
			@RequestBody(required = false) String payload) {
		try {
			logger.debug("payload received :: " + payload);

			this.validate(payload);

			String resolved = httpUtils.expandPayload(request, payload);

			logger.debug("Resolved payload::" + resolved);
			CSourceRegistration csourceRegistration = DataSerializer.getCSourceRegistration(resolved);
			logger.debug("Csource :: " + csourceRegistration);
			URI uri = csourceService.registerCSource(csourceRegistration);

			return ResponseEntity.status(HttpStatus.CREATED).header("location", AppConstants.CSOURCE_URL + uri)
					.body(uri);
		} catch (ResponseException exception) {
			return ResponseEntity.status(exception.getHttpStatus()).body(new RestResponse(exception));
		} catch (Exception e) {
			e.printStackTrace();
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
					.body(new RestResponse(HttpStatus.INTERNAL_SERVER_ERROR, "Internal server error",
							Collections.singletonList(e.toString())));
		}
	}

	@GetMapping("{registrationId}")
	public ResponseEntity<Object> getCSourceById(HttpServletRequest request,
			@PathVariable("registrationId") String registrationId) {
		try {
			logger.debug("get CSource() ::" + registrationId);
			List<String> csourceList = new ArrayList<String>();
			csourceList.add( DataSerializer.toJson(csourceService.getCSourceRegistrationById(registrationId)) );			
			return httpUtils.generateReply(request, csourceDAO.getListAsJsonArray(csourceList));            
		} catch (ResponseException exception) {
			return ResponseEntity.status(exception.getHttpStatus()).body(new RestResponse(exception));
		} catch (Exception e) {
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
					.body(new RestResponse(HttpStatus.INTERNAL_SERVER_ERROR, "Internal server error",
							Collections.singletonList("Internal Server Error")));
		}
	}

	@PatchMapping("{registrationId}")
	public ResponseEntity<Object> updateCSource(HttpServletRequest request,
			@PathVariable("registrationId") String registrationId, @RequestBody String payload) {
		try {
			logger.debug("update CSource() ::" + registrationId);
			String resolved = httpUtils.expandPayload(request, payload);

			csourceService.updateCSourceRegistration(registrationId, resolved);
			logger.debug("update CSource request completed::" + registrationId);
			return ResponseEntity.noContent().build();
		} catch (ResponseException exception) {
			return ResponseEntity.status(exception.getHttpStatus()).body(new RestResponse(exception));
		} catch (Exception e) {
			e.printStackTrace();
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
					.body(new RestResponse(HttpStatus.INTERNAL_SERVER_ERROR, "Internal server error",
							Collections.singletonList("Internal Server Error")));
		}
	}

	@DeleteMapping("{registrationId}")
	public ResponseEntity<Object> deleteCSource(@PathVariable("registrationId") String registrationId) {
		try {
			logger.debug("delete CSource() ::" + registrationId);
			csourceService.deleteCSourceRegistration(registrationId);
			logger.debug("delete CSource() completed::" + registrationId);
			return ResponseEntity.noContent().build();
		} catch (ResponseException exception) {
			return ResponseEntity.status(exception.getHttpStatus()).body(new RestResponse(exception));
		} catch (Exception e) {
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
					.body(new RestResponse(HttpStatus.INTERNAL_SERVER_ERROR, "Internal server error",
							Collections.singletonList("Internal Server Error")));
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
