package eu.neclab.ngsildbroker.queryhandler.controller;

import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.datatypes.RestResponse;
import eu.neclab.ngsildbroker.commons.datatypes.results.QueryResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.ngsiqueries.ParamsResolver;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.Validator;
import eu.neclab.ngsildbroker.queryhandler.services.QueryService;

@RestController
@RequestMapping("/ngsi-ld/v1")
public class QueryController {// implements QueryHandlerInterface {
	private final static Logger logger = LogManager.getLogger(QueryController.class);

	@Autowired
	QueryService queryService;

	@Value("${atcontext.url}")
	String atContextServerUrl;

	@Value("${defaultLimit}")
	int defaultLimit = 50;
	@Value("${maxLimit}")
	int maxLimit = 1000;

	@Value("${ngb.debugmode}")
	boolean debug = false;

	private final String emptyResult1 = "[ ]";
	private final String emptyResult2 = "[]";

	@Value("${ngsild.corecontext:https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld}")
	String coreContext;

	@PostConstruct
	public void init() {
		JsonLdProcessor.init(coreContext);
	}

	/**
	 * Method(GET) for multiple attributes separated by comma list
	 * 
	 * @param request
	 * @param entityId
	 * @param attrs
	 * @return
	 * @throws ResponseException
	 */
	@GetMapping(path = "/entities/{entityId}")
	public ResponseEntity<String> getEntity(HttpServletRequest request,
			@RequestParam(value = "attrs", required = false) List<String> attrs,
			@RequestParam(value = "options", required = false) List<String> options,
			@PathVariable("entityId") String entityId) throws ResponseException {

		try {
			HttpUtils.validateUri(entityId);
		} catch (ResponseException exception) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST)
					.body(new RestResponse(ErrorType.BadRequestData, "id is not a URI").toJson());
		}
		String originalQuery = NGSIConstants.QUERY_PARAMETER_ID + "=" + entityId;
		LinkedMultiValueMap<String, String> paramMap = new LinkedMultiValueMap<String, String>();
		paramMap.add(NGSIConstants.QUERY_PARAMETER_ID, entityId);
		ResponseEntity<String> result = getQueryData(request, originalQuery, paramMap, attrs, null, null, null, options,
				false, true, false, null);
		if (result.getBody().equals(emptyResult1) || result.getBody().equals(emptyResult2)) {
			return ResponseEntity.status(HttpStatus.NOT_FOUND)
					.body(new RestResponse(ErrorType.NotFound, "Resource not found.").toJson());
		}
		return result;
		/*
		 * String result = null; try {
		 * logger.trace("getEntity() ::query operation by kafka ::");
		 * 
		 * if (!request.getParameterMap().isEmpty() && attrs == null && options == null)
		 * { throw new ResponseException(ErrorType.InvalidRequest); }
		 * 
		 * boolean includeSysAttrs = (options != null &&
		 * options.contains(NGSIConstants.QUERY_PARAMETER_OPTIONS_SYSATTRS)); boolean
		 * keyValues = (options != null &&
		 * options.contains(NGSIConstants.QUERY_PARAMETER_OPTIONS_KEYVALUES));
		 * ArrayList<String> expandedAttrs = new ArrayList<String>();
		 * 
		 * if (attrs != null) {
		 * 
		 * List<Object> linkHeaders = HttpUtils.parseLinkHeader(request,
		 * NGSIConstants.HEADER_REL_LDCONTEXT);
		 * 
		 * for (String attrib : attrs) { try {
		 * expandedAttrs.add(paramsResolver.expandAttribute(attrib, linkHeaders)); }
		 * catch (ResponseException exception) { continue; } } // TODO valid this. spec
		 * doesn't say what to do here!!! if (expandedAttrs.isEmpty()) { return
		 * ResponseEntity.status(HttpStatus.ACCEPTED).body("{}".getBytes()); } }
		 * 
		 * result = queryService.retrieveEntity(entityId, expandedAttrs, keyValues,
		 * includeSysAttrs); if (result != "null" && !result.isEmpty()) { return
		 * httpUtils.generateReply(request, result); } else { return
		 * ResponseEntity.status(HttpStatus.NOT_FOUND) .body(new
		 * RestResponse(ErrorType.NotFound, "Resource not found.").toJson()); }
		 * 
		 * } catch (ResponseException exception) { logger.error("Exception ::",
		 * exception); return ResponseEntity.status(exception.getHttpStatus()).body(new
		 * RestResponse(exception).toJson()); } catch (Exception exception) {
		 * logger.error("Exception ::", exception); return
		 * ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR) .body(new
		 * RestResponse(ErrorType.InternalError, "Internal error").toJson()); }
		 */ }

	/**
	 * Method(GET) for fetching all entities by kafka and other geo query operation
	 * by database
	 * 
	 * @param request
	 * @param type
	 * @return ResponseEntity object
	 */
	@GetMapping("/entities")
	public ResponseEntity<String> getAllEntity(HttpServletRequest request,
			@RequestParam(value = "attrs", required = false) List<String> attrs,
			@RequestParam(value = "limit", required = false) Integer limit,
			@RequestParam(value = "offset", required = false) Integer offset,
			@RequestParam(value = "qtoken", required = false) String qToken,
			@RequestParam(name = "options", required = false) List<String> options,
			@RequestParam(name = "services", required = false) Boolean showServices,
			@RequestParam(value = "count", required = false, defaultValue = "false") boolean count) {

		return getQueryData(request, request.getQueryString(), HttpUtils.getQueryParamMap(request), attrs, limit,
				offset, qToken, options, showServices, false, count, null);
	}

	@GetMapping(path = "/types")
	public ResponseEntity<String> getAllTypes(HttpServletRequest request,
			@RequestParam(value = "details", required = false, defaultValue = "false") boolean details) {
		String check = "NonDeatilsType";
		if (details == true) {
			check = "deatilsType";
		}
		ResponseEntity<String> result = getQueryData(request, null, HttpUtils.getQueryParamMap(request), null, null,
				null, null, null, false, true, false, check);
		if (result.getBody().equals(emptyResult1) || result.getBody().equals(emptyResult2)) {
			return ResponseEntity.status(HttpStatus.NOT_FOUND)
					.body(new RestResponse(ErrorType.NotFound, "Resource not found.").toJson());
		}
		return result;
	}

	@GetMapping(path = "/types/{entityType}")
	public ResponseEntity<String> getType(HttpServletRequest request, @PathVariable("entityType") String type,
			@RequestParam(value = "details", required = false, defaultValue = "false") boolean details) {
		String check = "type";
		ArrayList<String> types = new ArrayList<String>();
		types.add(type);
		ResponseEntity<String> result = getQueryData(request, null, HttpUtils.getQueryParamMap(request), types, null,
				null, null, null, false, true, false, check);
		if (result.getBody().equals(emptyResult1) || result.getBody().equals(emptyResult2)) {
			return ResponseEntity.status(HttpStatus.NOT_FOUND)
					.body(new RestResponse(ErrorType.NotFound, "Resource not found.").toJson());
		}
		return result;
	}

	@GetMapping(path = "/attributes")
	public ResponseEntity<String> getAllAttribute(HttpServletRequest request,
			@RequestParam(value = "details", required = false, defaultValue = "false") boolean details) {

		String check = "NonDeatilsAttributes";
		if (details == true) {
			check = "deatilsAttributes";
		}
		ResponseEntity<String> result = getQueryData(request, null, HttpUtils.getQueryParamMap(request), null, null,
				null, null, null, false, true, false, check);
		if (result.getBody().equals(emptyResult1) || result.getBody().equals(emptyResult2)) {
			return ResponseEntity.status(HttpStatus.NOT_FOUND)
					.body(new RestResponse(ErrorType.NotFound, "Resource not found.").toJson());
		}
		return result;
	}

	@GetMapping(path = "/attributes/{attributes}")
	public ResponseEntity<String> getAttributes(HttpServletRequest request,
			@PathVariable("attributes") String attributes,
			@RequestParam(value = "details", required = false, defaultValue = "false") boolean details) {
		String check = "Attribute";
		ArrayList<String> types = new ArrayList<String>();
		types.add(attributes);
		ResponseEntity<String> result = getQueryData(request, null, HttpUtils.getQueryParamMap(request), types, null,
				null, null, null, false, true, false, check);
		if (result.getBody().equals(emptyResult1) || result.getBody().equals(emptyResult2)) {
			return ResponseEntity.status(HttpStatus.NOT_FOUND)
					.body(new RestResponse(ErrorType.NotFound, "Resource not found.").toJson());
		}
		return result;
	}

	private ResponseEntity<String> getQueryData(HttpServletRequest request, String originalQueryParams,
			MultiValueMap<String, String> paramMap, List<String> attrs, Integer limit, Integer offset, String qToken,
			List<String> options, Boolean showServices, boolean retrieve, Boolean countResult, String check) {
		// long start = System.currentTimeMillis();
		String tenantid = request.getHeader(NGSIConstants.TENANT_HEADER);

		if (limit == null) {
			limit = defaultLimit;
		}
		if (offset == null) {
			offset = 0;
		}

		try {
			logger.trace("getAllEntity() ::");
			if (countResult == false && limit == 0) {
				return ResponseEntity.status(HttpStatus.BAD_REQUEST).build();
			}
			// long prelink = System.currentTimeMillis();
			List<Object> linkHeaders = HttpUtils.getAtContext(request);
			Context context = JsonLdProcessor.getCoreContextClone();
			context = context.parse(linkHeaders, true);
			// long postlink = System.currentTimeMillis();

			if (retrieve || originalQueryParams != null) {
				Validator.validate(HttpUtils.getQueryParamMap(request), maxLimit, retrieve);
				if (originalQueryParams != null) {
					originalQueryParams = URLDecoder.decode(originalQueryParams, NGSIConstants.ENCODE_FORMAT);
				}
				QueryParams qp = ParamsResolver.getQueryParamsFromUriQuery(paramMap, context);
				if (qp == null) // invalid query
					throw new ResponseException(ErrorType.InvalidRequest, "Empty query parameters");
				qp.setTenant(tenantid);
				qp.setCheck(check);
				qp.setKeyValues((options != null && options.contains(NGSIConstants.QUERY_PARAMETER_OPTIONS_KEYVALUES)));
				qp.setIncludeSysAttrs(
						(options != null && options.contains(NGSIConstants.QUERY_PARAMETER_OPTIONS_SYSATTRS)));
				if (attrs != null) {
					ArrayList<String> expandedAttrs = new ArrayList<String>();
					for (String attrib : attrs) {
						try {
							expandedAttrs.add(ParamsResolver.expandAttribute(attrib, context));
						} catch (ResponseException exception) {
							continue;
						}
					}
					qp.setAttrs(String.join(",", expandedAttrs));
				}

				checkParamsForValidity(qp);
				ArrayListMultimap<String, String> headers = HttpUtils.getHeaders(request);
				QueryResult qResult;
				try {
					qResult = queryService.getData(qp, originalQueryParams, linkHeaders, limit, offset, qToken,
							showServices, countResult, headers, false);
				} catch (Exception e) {
					logger.debug("Tenant for the request not found", e);
					return ResponseEntity.status(HttpStatus.NOT_FOUND)
							.body(new RestResponse(ErrorType.TenantNotFound, "Tenant not found.").toJson());
				}
				ResponseEntity<String> result = HttpUtils.generateReply(request, qResult, !retrieve, countResult, context,
						linkHeaders, AppConstants.QUERY_ENDPOINT);
				return result;
			} else {
				ResponseException responseException = new ResponseException(ErrorType.BadRequestData, "invalid query");
				return ResponseEntity.status(responseException.getHttpStatus())
						.body(new RestResponse(responseException).toJson());
			}
		} catch (Exception exception) {
			return HttpUtils.handleControllerExceptions(exception);
		}
	}

	private void checkParamsForValidity(QueryParams qp) throws ResponseException {
		if (qp.getGeometry() != null && !qp.getGeometry().isEmpty()) {
			if (!NGSIConstants.ALLOWED_GEOMETRIES.contains(qp.getGeometry())) {
				throw new ResponseException(ErrorType.BadRequestData, "Invalid geometry provided");
			}
		}
		if (qp.getGeorel() != null && qp.getGeorel().getGeorelOp() != null && !qp.getGeorel().getGeorelOp().isEmpty()) {
			if (!NGSIConstants.ALLOWED_GEOREL.contains(qp.getGeorel().getGeorelOp())) {
				throw new ResponseException(ErrorType.BadRequestData, "Invalid georel provided");
			}
		}

	}
}