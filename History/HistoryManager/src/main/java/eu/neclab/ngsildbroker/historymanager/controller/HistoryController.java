package eu.neclab.ngsildbroker.historymanager.controller;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
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
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.datatypes.QueryResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.ngsiqueries.ParamsResolver;
import eu.neclab.ngsildbroker.commons.tools.ControllerFunctions;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.historymanager.repository.HistoryDAO;
import eu.neclab.ngsildbroker.historymanager.service.HistoryService;

@RestController
@RequestMapping("/ngsi-ld/v1/temporal/entities")
public class HistoryController {

	private final static Logger logger = LoggerFactory.getLogger(HistoryController.class);

	@Autowired
	HistoryDAO historyDAO;
	@Autowired
	HistoryService historyService;
	@Value("${atcontext.url}")
	String atContextServerUrl;
	@Value("${defaultLimit}")
	int defaultLimit = 50;
	@Value("${maxLimit}")
	int maxLimit = 1000;

	private JsonLdOptions opts = new JsonLdOptions(JsonLdOptions.JSON_LD_1_1);

	@Value("${ngsild.corecontext:https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld}")
	String coreContext;

	@PostConstruct
	public void init() {
		JsonLdProcessor.init(coreContext);
	}

	@PostMapping
	public ResponseEntity<String> createTemporalEntity(HttpServletRequest request,
			@RequestBody(required = false) String payload) {
		return ControllerFunctions.createEntry(historyService, request, payload,
				AppConstants.TEMP_ENTITY_CREATE_PAYLOAD, logger);
	}

	@GetMapping
	public ResponseEntity<String> retrieveTemporalEntity(HttpServletRequest request,
			@RequestParam(value = "limit", required = false) Integer limit,
			@RequestParam(value = "offset", required = false) Integer offset,
			@RequestParam(value = "qtoken", required = false) String qToken,
			@RequestParam(name = "options", required = false) List<String> options,
			@RequestParam(value = "count", required = false) Boolean countResult) {

		try {
			logger.trace("retrieveTemporalEntity :: started");

			Context context = JsonLdProcessor.getCoreContextClone();
			List<Object> links = HttpUtils.getAtContext(request);
			context = context.parse(links, true);
			QueryParams qp = ParamsResolver.getQueryParamsFromUriQuery(HttpUtils.getQueryParamMap(request), context,
					true);
			if (limit == null) {
				limit = defaultLimit;
			}
			if (offset == null) {
				offset = 0;
			}
			if (qp == null) // invalid query
				throw new ResponseException(ErrorType.InvalidRequest, "No query parameters provided");
			if (qp.getTimerel() == null || qp.getTimeAt() == null) {
				throw new ResponseException(ErrorType.BadRequestData, "Time filter is required");
			}
			if ((qp.getEntities() == null || qp.getEntities().isEmpty())
					&& (qp.getAttrs() == null || qp.getAttrs().isEmpty())) {
				throw new ResponseException(ErrorType.BadRequestData, "Type or attrs is required");
			}
			if ((countResult == null || countResult == false) && limit == 0) {
				throw new ResponseException(ErrorType.BadRequestData, "Bad Request Data");
			}
			logger.trace("retrieveTemporalEntity :: completed");
			qp.setLimit(limit);
			qp.setOffSet(offset);
			qp.setCountResult(countResult);

			QueryResult qResult = historyDAO.query(qp);
			String nextLink = HttpUtils.generateNextLink(request, qResult);
			String prevLink = HttpUtils.generatePrevLink(request, qResult);
			ArrayList<String> additionalLinks = new ArrayList<String>();
			if (nextLink != null) {
				additionalLinks.add(nextLink);
			}
			if (prevLink != null) {
				additionalLinks.add(prevLink);
			}
			ArrayListMultimap<String, String> additionalHeaders = ArrayListMultimap.create();

			if (countResult != null) {
				if (countResult == true) {
					additionalHeaders.put(NGSIConstants.COUNT_HEADER_RESULT, String.valueOf(qResult.getCount()));
				}
			}
			if (!additionalLinks.isEmpty()) {
				additionalHeaders.putAll(HttpHeaders.LINK, additionalLinks);
			}
			if (qResult.getActualDataString() != null) {
				return HttpUtils.generateReply(request, historyDAO.getListAsJsonArray(qResult.getActualDataString()),
						additionalHeaders, AppConstants.HISTORY_ENDPOINT);
			} else {
				return HttpUtils.generateReply(request, "[]", additionalHeaders, AppConstants.HISTORY_ENDPOINT);
			}
		} catch (Exception exception) {
			return HttpUtils.handleControllerExceptions(exception);
		}
	}

	@GetMapping("/{entityId}")
	public ResponseEntity<String> retrieveTemporalEntityById(HttpServletRequest request,
			@PathVariable("entityId") String entityId) {
		// String params = request.getQueryString();
		try {
			logger.trace("retrieveTemporalEntityById :: started " + entityId);
			logger.debug("entityId : " + entityId);
			HttpUtils.validateUri(entityId);
			MultiValueMap<String, String> params = HttpUtils.getQueryParamMap(request);
			params.add(NGSIConstants.QUERY_PARAMETER_ID, entityId);

			Context context = JsonLdProcessor.getCoreContextClone();
			List<Object> links = HttpUtils.getAtContext(request);
			context = context.parse(links, true);
			QueryParams qp = ParamsResolver.getQueryParamsFromUriQuery(params, context, true);
			logger.trace("retrieveTemporalEntityById :: completed");
			List<String> queryResult = historyDAO.query(qp).getActualDataString();
			if (queryResult == null) {
				System.err.println();
			}
			if (queryResult.isEmpty()) {
				throw new ResponseException(ErrorType.NotFound, "Entity not found");
			}
			return HttpUtils.generateReply(request, historyDAO.getListAsJsonArray(queryResult),
					AppConstants.HISTORY_ENDPOINT);
		} catch (Exception exception) {
			return HttpUtils.handleControllerExceptions(exception);
		}
	}

	@DeleteMapping("/{entityId}")
	public ResponseEntity<String> deleteTemporalEntityById(HttpServletRequest request,
			@PathVariable("entityId") String entityId) {
		try {
			logger.trace("deleteTemporalEntityById :: started");
			logger.debug("entityId : " + entityId);
			HttpUtils.validateUri(entityId);
			MultiValueMap<String, String> params = HttpUtils.getQueryParamMap(request);
			params.add(NGSIConstants.QUERY_PARAMETER_ID, entityId);
			Context context = JsonLdProcessor.getCoreContextClone();
			List<Object> links = HttpUtils.getAtContext(request);
			context = context.parse(links, true);
			QueryParams qp = ParamsResolver.getQueryParamsFromUriQuery(params, context, true);
			logger.trace("retrieveTemporalEntityById :: completed");

			List<String> queryResult = historyDAO.query(qp).getActualDataString();
			if (queryResult.isEmpty()) {
				throw new ResponseException(ErrorType.NotFound, "Entity not found");
			}

			historyService.delete(HttpUtils.getHeaders(request), entityId, null, null, context);
			logger.trace("deleteTemporalEntityById :: completed");
			return ResponseEntity.noContent().build();
		} catch (Exception exception) {
			return HttpUtils.handleControllerExceptions(exception);
		}
	}

	@PostMapping("/{entityId}/attrs")
	public ResponseEntity<String> addAttrib2TemopralEntity(HttpServletRequest request,
			@PathVariable("entityId") String entityId, @RequestBody(required = false) String payload,
			@RequestParam(required = false, name = "options") String options) {
		return ControllerFunctions.appendToEntry(historyService, request, entityId, payload, options,
				AppConstants.TEMP_ENTITY_UPDATE_PAYLOAD, logger);
	}

	@DeleteMapping("/{entityId}/attrs/{attrId}")
	public ResponseEntity<String> deleteAttrib2TemporalEntity(HttpServletRequest request,
			@PathVariable("entityId") String entityId, @PathVariable("attrId") String attrId) {
		try {
			HttpUtils.validateUri(entityId);
			Context context = JsonLdProcessor.getCoreContextClone();
			List<Object> links = HttpUtils.getAtContext(request);
			context = context.parse(links, true);
			logger.trace("deleteAttrib2TemporalEntity :: started");
			logger.debug("entityId : " + entityId + " attrId : " + attrId);
			historyService.delete(HttpUtils.getHeaders(request), entityId, attrId, null, context);
			logger.trace("deleteAttrib2TemporalEntity :: completed");
			return ResponseEntity.noContent().build();
		} catch (Exception exception) {
			return HttpUtils.handleControllerExceptions(exception);
		}
	}

	@PatchMapping("/{entityId}/attrs/{attrId}/{instanceId}")
	public ResponseEntity<String> modifyAttribInstanceTemporalEntity(HttpServletRequest request,
			@PathVariable("entityId") String entityId, @PathVariable("attrId") String attrId,
			@PathVariable("instanceId") String instanceId, @RequestBody(required = false) String payload) {
		try {
			logger.trace("modifyAttribInstanceTemporalEntity :: started");
			logger.debug("entityId : " + entityId + " attrId : " + attrId + " instanceId : " + instanceId);
			Context context = JsonLdProcessor.getCoreContextClone();

			List<Object> linkHeaders = HttpUtils.getAtContext(request);
			boolean atContextAllowed = HttpUtils.doPreflightCheck(request, linkHeaders);
			context = context.parse(linkHeaders, true);

			@SuppressWarnings("unchecked")
			Map<String, Object> resolved = (Map<String, Object>) JsonLdProcessor.expand(linkHeaders,
					JsonUtils.fromString(payload), opts, AppConstants.TEMP_ENTITY_UPDATE_PAYLOAD, atContextAllowed)
					.get(0);
			// TODO : TBD- conflict between specs and implementation <mentioned no request
			// body in specs>
			historyService.modifyAttribInstanceTemporalEntity(HttpUtils.getHeaders(request), entityId, resolved, attrId,
					instanceId, context);
			logger.trace("modifyAttribInstanceTemporalEntity :: completed");
			return ResponseEntity.noContent().build();
		} catch (Exception exception) {
			return HttpUtils.handleControllerExceptions(exception);
		}
	}

	@DeleteMapping("/{entityId}/attrs/{attrId}/{instanceId}")
	public ResponseEntity<String> deleteAtrribInstanceTemporalEntity(HttpServletRequest request,
			@PathVariable("entityId") String entityId, @PathVariable("attrId") String attrId,
			@PathVariable("instanceId") String instanceId) {
		try {
			logger.trace("deleteAtrribInstanceTemporalEntity :: started");
			logger.debug("entityId : " + entityId + " attrId : " + attrId + " instanceId : " + instanceId);
			Context context = JsonLdProcessor.getCoreContextClone();
			List<Object> links = HttpUtils.getAtContext(request);
			context = context.parse(links, true);

			historyService.delete(HttpUtils.getHeaders(request), entityId, attrId, instanceId, context);
			logger.trace("deleteAtrribInstanceTemporalEntity :: completed");
			return ResponseEntity.noContent().build();
		} catch (Exception exception) {
			return HttpUtils.handleControllerExceptions(exception);
		}
	}
}
