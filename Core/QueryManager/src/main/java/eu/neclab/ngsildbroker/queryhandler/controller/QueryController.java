package eu.neclab.ngsildbroker.queryhandler.controller;

import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
import eu.neclab.ngsildbroker.queryhandler.services.QueryService;
import eu.neclab.ngsildbroker.queryhandler.utils.Validator;

@RestController
@RequestMapping("/ngsi-ld/v1/entities")
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
	 */
	@GetMapping(path = "/{entityId}")
	public ResponseEntity<byte[]> getEntity(HttpServletRequest request, @PathVariable("entityId") String entityId,
			@RequestParam(value = "attrs", required = false) List<String> attrs,
			@RequestParam(value = "options", required = false) List<String> options) {
		String originalQuery = NGSIConstants.QUERY_PARAMETER_ID + "=" + entityId;
		HashMap<String, String[]> paramMap = new HashMap<String, String[]>();
		paramMap.put(NGSIConstants.QUERY_PARAMETER_ID, new String[] { entityId });
		ResponseEntity<byte[]> result = getQueryData(request, originalQuery, paramMap, attrs, null, null, null, options,
				false, true);
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
	@GetMapping()
	public ResponseEntity<byte[]> getAllEntity(HttpServletRequest request,
			@RequestParam(value = "attrs", required = false) List<String> attrs,
			@RequestParam(value = "limit", required = false) Integer limit,
			@RequestParam(value = "offset", required = false) Integer offset,
			@RequestParam(value = "qtoken", required = false) String qToken,
			@RequestParam(name = "options", required = false) List<String> options,
			@RequestParam(name = "services", required = false) Boolean showServices) {
		return getQueryData(request, request.getQueryString(), request.getParameterMap(), attrs, limit, offset, qToken,
				options, showServices, false);
	}

	private ResponseEntity<byte[]> getQueryData(HttpServletRequest request, String originalQueryParams,
			Map<String, String[]> paramMap, List<String> attrs, Integer limit, Integer offset, String qToken,
			List<String> options, Boolean showServices, boolean retrieve) {

		if (limit == null) {
			limit = defaultLimit;
		}
		if (offset == null) {
			offset = 0;
		}

		try {
			logger.trace("getAllEntity() ::");

			List<Object> linkHeaders = HttpUtils.parseLinkHeader(request, NGSIConstants.HEADER_REL_LDCONTEXT);
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
					QueryResult qResult = queryService.getData(qp, originalQueryParams, linkHeaders, limit, offset,
							qToken, showServices);

					return generateReply(request, qResult, !retrieve);

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

	public ResponseEntity<byte[]> generateReply(HttpServletRequest request, QueryResult qResult, boolean forceArray)
			throws ResponseException {
		String nextLink = generateNextLink(request, qResult);
		String prevLink = generatePrevLink(request, qResult);
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

		return httpUtils.generateReply(request, "[" + String.join(",", qResult.getDataString()) + "]",
				additionalHeaders, null, forceArray);
	}

	private String generateNextLink(HttpServletRequest request, QueryResult qResult) {
		if (qResult.getResultsLeftAfter() == null || qResult.getResultsLeftAfter() <= 0) {
			return null;
		}
		return generateFollowUpLinkHeader(request, qResult.getOffset() + qResult.getLimit(), qResult.getLimit(),
				qResult.getqToken(), "next");
	}

	private String generateFollowUpLinkHeader(HttpServletRequest request, int offset, int limit, String token,
			String rel) {

		StringBuilder builder = new StringBuilder("</");
		builder.append("?");

		for (Entry<String, String[]> entry : request.getParameterMap().entrySet()) {
			String[] values = entry.getValue();
			String key = entry.getKey();
			if (key.equals("offset")) {
				continue;
			}
			if (key.equals("qtoken")) {
				continue;
			}
			if (key.equals("limit")) {
				continue;
			}

			for (String value : values) {
				builder.append(key + "=" + value + "&");
			}

		}
		builder.append("offset=" + offset + "&");
		builder.append("limit=" + limit + "&");
		builder.append("qtoken=" + token + ">;rel=\"" + rel + "\"");
		return builder.toString();
	}

	private String generatePrevLink(HttpServletRequest request, QueryResult qResult) {
		if (qResult.getResultsLeftBefore() == null || qResult.getResultsLeftBefore() <= 0) {
			return null;
		}
		int offset = qResult.getOffset() - qResult.getLimit();
		if (offset < 0) {
			offset = 0;
		}
		int limit = qResult.getLimit();

		return generateFollowUpLinkHeader(request, offset, limit, qResult.getqToken(), "prev");
	}

}
