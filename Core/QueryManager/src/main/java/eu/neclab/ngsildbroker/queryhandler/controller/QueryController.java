package eu.neclab.ngsildbroker.queryhandler.controller;

import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import com.google.common.collect.ArrayListMultimap;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.datatypes.QueryResult;
import eu.neclab.ngsildbroker.commons.datatypes.RestResponse;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.ldcontext.ContextResolverBasic;
import eu.neclab.ngsildbroker.commons.ngsiqueries.ParamsResolver;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.Validator;
import eu.neclab.ngsildbroker.commons.tools.ValidateURI;
import eu.neclab.ngsildbroker.queryhandler.services.QueryService;

@RestController
@RequestMapping("/ngsi-ld/v1")
public class QueryController {// implements QueryHandlerInterface {
	private final static Logger logger = LogManager.getLogger(QueryController.class);
	private final static String MY_REQUEST_URL = "/ngsi-ld/v1/entities";
	private final static String MY_REQUEST_URL_ALT = "/ngsi-ld/v1/entities/";
	@Autowired
	QueryService queryService;

	@Autowired
	@Qualifier("qmparamsResolver")
	ParamsResolver paramsResolver;

	@Autowired
	@Qualifier("qmconRes")
	ContextResolverBasic contextResolver;
	
	@Value("${atcontext.url}")
	String atContextServerUrl;

	@Value("${defaultLimit}")
	int defaultLimit = 50;
	@Value("${maxLimit}")
	int maxLimit = 1000;

	@Value("${ngb.debugmode}")
	boolean debug = false;

	private HttpUtils httpUtils;

	private final byte[] emptyResult1 = { '{', ' ', '}' };
	private final byte[] emptyResult2 = { '{', '}' };
	

	@PostConstruct
	private void setup() {
		httpUtils = HttpUtils.getInstance(contextResolver);
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
	@GetMapping(path = "/entities/**")
	public ResponseEntity<byte[]> getEntity(HttpServletRequest request,
			@RequestParam(value = "attrs", required = false) List<String> attrs,
			@RequestParam(value = "options", required = false) List<String> options) throws ResponseException{
		String entityId = HttpUtils.denormalize(request.getServletPath().replace("/ngsi-ld/v1/entities/", ""));
		try {
			ValidateURI.validateUri(entityId);
		} catch(ResponseException exception){
			return ResponseEntity.status(HttpStatus.BAD_REQUEST)
					.body(new RestResponse(ErrorType.BadRequestData, "id is not a URI").toJsonBytes());
		}
		String originalQuery = NGSIConstants.QUERY_PARAMETER_ID + "=" + entityId;
		HashMap<String, String[]> paramMap = new HashMap<String, String[]>();
		paramMap.put(NGSIConstants.QUERY_PARAMETER_ID, new String[] { entityId });
		ResponseEntity<byte[]> result = getQueryData(request, originalQuery, paramMap, attrs, null, null, null, options,
				false, true, false, null);
		if (Arrays.equals(emptyResult1, result.getBody()) || Arrays.equals(emptyResult2, result.getBody())) {
			return ResponseEntity.status(HttpStatus.NOT_FOUND)
					.body(new RestResponse(ErrorType.NotFound, "Resource not found.").toJsonBytes());
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
		 * RestResponse(ErrorType.NotFound, "Resource not found.").toJsonBytes()); }
		 * 
		 * } catch (ResponseException exception) { logger.error("Exception ::",
		 * exception); return ResponseEntity.status(exception.getHttpStatus()).body(new
		 * RestResponse(exception).toJsonBytes()); } catch (Exception exception) {
		 * logger.error("Exception ::", exception); return
		 * ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR) .body(new
		 * RestResponse(ErrorType.InternalError, "Internal error").toJsonBytes()); }
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
	public ResponseEntity<byte[]> getAllEntity(HttpServletRequest request,
			@RequestParam(value = "attrs", required = false) List<String> attrs,
			@RequestParam(value = "limit", required = false) Integer limit,
			@RequestParam(value = "offset", required = false) Integer offset,
			@RequestParam(value = "qtoken", required = false) String qToken,
			@RequestParam(name = "options", required = false) List<String> options,
			@RequestParam(name = "services", required = false) Boolean showServices,
			@RequestParam(value = "count", required = false, defaultValue = "false") boolean count) {
		
		return getQueryData(request, request.getQueryString(), request.getParameterMap(), attrs, limit, offset, qToken,
				options, showServices, false, count, null);
	}

	@GetMapping(path = "/types")
	public ResponseEntity<byte[]> getAllTypes(HttpServletRequest request,
			@RequestParam(value = "details", required = false, defaultValue = "false") boolean details) {
		String check = "NonDeatilsType";
		if (details == true) {
			check = "deatilsType";
		}
		ResponseEntity<byte[]> result = getQueryData(request, null, request.getParameterMap(), null, null, null, null,
				null, false, true, false, check);
		if (Arrays.equals(emptyResult1, result.getBody()) || Arrays.equals(emptyResult2, result.getBody())) {
			return ResponseEntity.status(HttpStatus.NOT_FOUND)
					.body(new RestResponse(ErrorType.NotFound, "Resource not found.").toJsonBytes());
		}
		return result;
	}

	@GetMapping(path = "/types/{entityType}")
	public ResponseEntity<byte[]> getType(HttpServletRequest request, @PathVariable("entityType") String type,
			@RequestParam(value = "details", required = false, defaultValue = "false") boolean details) {
		String check = "type";
		ArrayList<String> types = new ArrayList<String>();
		types.add(type);
		ResponseEntity<byte[]> result = getQueryData(request, null, request.getParameterMap(), types, null, null, null,
				null, false, true, false, check);
		if (Arrays.equals(emptyResult1, result.getBody()) || Arrays.equals(emptyResult2, result.getBody())) {
			return ResponseEntity.status(HttpStatus.NOT_FOUND)
					.body(new RestResponse(ErrorType.NotFound, "Resource not found.").toJsonBytes());
		}
		return result;
	}

	@GetMapping(path = "/attributes")
	public ResponseEntity<byte[]> getAllAttribute(HttpServletRequest request,
			@RequestParam(value = "details", required = false, defaultValue = "false") boolean details) {
		
		String check = "NonDeatilsAttributes";
		if (details == true) {
			check = "deatilsAttributes";
		}
		ResponseEntity<byte[]> result = getQueryData(request, null, request.getParameterMap(), null, null, null, null,
				null, false, true, false, check);
		if (Arrays.equals(emptyResult1, result.getBody()) || Arrays.equals(emptyResult2, result.getBody())) {
			return ResponseEntity.status(HttpStatus.NOT_FOUND)
					.body(new RestResponse(ErrorType.NotFound, "Resource not found.").toJsonBytes());
		}
		return result;
	}

	@GetMapping(path = "/attributes/{attributes}")
	public ResponseEntity<byte[]> getAttributes(HttpServletRequest request,
			@PathVariable("attributes") String attributes,
			@RequestParam(value = "details", required = false, defaultValue = "false") boolean details) {
		String check = "Attribute";
		ArrayList<String> types = new ArrayList<String>();
		types.add(attributes);
		ResponseEntity<byte[]> result = getQueryData(request, null, request.getParameterMap(), types, null, null, null,
				null, false, true, false, check);
		if (Arrays.equals(emptyResult1, result.getBody()) || Arrays.equals(emptyResult2, result.getBody())) {
			return ResponseEntity.status(HttpStatus.NOT_FOUND)
					.body(new RestResponse(ErrorType.NotFound, "Resource not found.").toJsonBytes());
		}
		return result;
	}

	private ResponseEntity<byte[]> getQueryData(HttpServletRequest request, String originalQueryParams,
			Map<String, String[]> paramMap, List<String> attrs, Integer limit, Integer offset, String qToken,
			List<String> options, Boolean showServices, boolean retrieve, Boolean countResult, String check) {
		//long start = System.currentTimeMillis();
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
			//long prelink = System.currentTimeMillis();
			List<Object> linkHeaders = HttpUtils.parseLinkHeader(request, NGSIConstants.HEADER_REL_LDCONTEXT);
			//long postlink = System.currentTimeMillis();
			if (retrieve || request.getRequestURI().equals(MY_REQUEST_URL)
					|| request.getRequestURI().equals(MY_REQUEST_URL_ALT)) {
				if (retrieve || originalQueryParams != null) {
					Validator.validate(request.getParameterMap(), maxLimit, retrieve);
					if (originalQueryParams != null) {
						originalQueryParams = URLDecoder.decode(originalQueryParams, NGSIConstants.ENCODE_FORMAT);
					}
					QueryParams qp = paramsResolver.getQueryParamsFromUriQuery(paramMap, linkHeaders);
					if (qp == null) // invalid query
						throw new ResponseException(ErrorType.InvalidRequest);
					qp.setTenant(tenantid);
					qp.setKeyValues(
							(options != null && options.contains(NGSIConstants.QUERY_PARAMETER_OPTIONS_KEYVALUES)));
					qp.setIncludeSysAttrs(
							(options != null && options.contains(NGSIConstants.QUERY_PARAMETER_OPTIONS_SYSATTRS)));
					if (attrs != null) {
						ArrayList<String> expandedAttrs = new ArrayList<String>();
						for (String attrib : attrs) {
							try {
								expandedAttrs.add(paramsResolver.expandAttribute(attrib, linkHeaders));
							} catch (ResponseException exception) {
								continue;
							}
						}
						qp.setAttrs(String.join(",", expandedAttrs));
					}

					checkParamsForValidity(qp);
					//long pregenheades = System.currentTimeMillis();
					ArrayListMultimap<String, String> headers = HttpUtils.getHeaders(request);
					//long postgenheaders = System.currentTimeMillis();
					QueryResult qResult;
					try {
					 qResult = queryService.getData(qp, originalQueryParams, linkHeaders, limit, offset,
							qToken, showServices, countResult, headers, false, check);
					}catch(Exception e){
						return ResponseEntity.status(HttpStatus.NOT_FOUND)
								.body(new RestResponse(ErrorType.TenantNotFound, "Tenant not found.").toJsonBytes());
					}
					//long pregenresult = System.currentTimeMillis();
					ResponseEntity<byte[]> result = generateReply(httpUtils, request, qResult, !retrieve, countResult);
					//long end = System.currentTimeMillis();
					//System.err.println(start);
					//System.err.println(prelink);
					//System.err.println(postlink);
					//System.err.println(pregenheades);
					//System.err.println(postgenheaders);
					//System.err.println(pregenresult);
					//System.err.println(end);
					return result;

				} else {

					if (debug) {
						ArrayList<String> allEntityResult = queryService.retriveAllEntity();
						if (allEntityResult.size() > 1) {
							return httpUtils.generateReply(request, allEntityResult.get(0));
						} else {
							return ResponseEntity.accepted()
									.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSONLD)
									.body(allEntityResult.get(0).getBytes());
						}
					} else {
						// as per [5.7.2.4]
						throw new ResponseException(ErrorType.BadRequestData);
					}

				}
			} else {
				throw new ResponseException(ErrorType.BadRequestData);
			}
		} catch (

		ResponseException exception) {
			logger.error("Exception ::", exception);
			return ResponseEntity.status(exception.getHttpStatus()).body(new RestResponse(exception).toJsonBytes());
		} catch (Exception exception) {
			logger.error("Exception ::", exception);
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
					.body(new RestResponse(ErrorType.InternalError, exception.getLocalizedMessage()).toJsonBytes());
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

	public static ResponseEntity<byte[]> generateReply(HttpUtils httpUtils, HttpServletRequest request, QueryResult qResult, boolean forceArray, boolean count)
			throws ResponseException {
		String nextLink = HttpUtils.generateNextLink(request, qResult);
		String prevLink = HttpUtils.generatePrevLink(request, qResult);
		ArrayList<String> additionalLinks = new ArrayList<String>();
		if (nextLink != null) {
			additionalLinks.add(nextLink);
		}
		if (prevLink != null) {
			additionalLinks.add(prevLink);
		}
		ArrayList<String> additionalHeaderCount = new ArrayList<String>();
		HashMap<String, List<String>> additionalHeaders = new HashMap<String, List<String>>();
       
		if (count == true) {
			additionalHeaderCount.add(String.valueOf(qResult.getCount()));
			additionalHeaders.put(NGSIConstants.COUNT_HEADER_RESULT, additionalHeaderCount);
		}
		
		if (!additionalLinks.isEmpty()) {
			additionalHeaders.put(HttpHeaders.LINK, additionalLinks);
		}
		return httpUtils.generateReply(request, "[" + String.join(",", qResult.getDataString()) + "]",
				additionalHeaders, null, forceArray);
	}	
}