package eu.neclab.ngsildbroker.historymanager.controller;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.QueryHistoryEntitiesRequest;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.datatypes.QueryResult;
import eu.neclab.ngsildbroker.commons.datatypes.RestResponse;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.ngsiqueries.ParamsResolver;
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
	public ResponseEntity<byte[]> createTemporalEntity(ServerHttpRequest request,
			@RequestBody(required = false) String payload) {
		try {
			logger.trace("createTemporalEntity :: started");
			if (payload == null) {
				return HttpUtils.handleControllerExceptions(
						new ResponseException(ErrorType.BadRequestData, "Empty Body is not allowed here"));
			}
			List<Object> linkHeaders = HttpUtils.getAtContext(request);
			boolean atContextAllowed = HttpUtils.doPreflightCheck(request, linkHeaders);
			@SuppressWarnings("unchecked")
			Map<String, Object> resolved = (Map<String, Object>) JsonLdProcessor.expand(linkHeaders,
					JsonUtils.fromString(payload), opts, AppConstants.TEMP_ENTITY_CREATE_PAYLOAD, atContextAllowed)
					.get(0);
			String uri = historyService.createMessage(HttpUtils.getHeaders(request), resolved);
			logger.trace("createTemporalEntity :: completed");
			return ResponseEntity.status(HttpStatus.CREATED).header("Location", uri).body(uri.getBytes());
		} catch (Exception exception) {
			return HttpUtils.handleControllerExceptions(exception);
		}
	}

	@GetMapping
	public ResponseEntity<byte[]> retrieveTemporalEntity(ServerHttpRequest request,
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
			QueryParams qp = ParamsResolver.getQueryParamsFromUriQuery(request.getQueryParams(), context, true);
			if (limit == null) {
				limit = defaultLimit;
			}
			if (offset == null) {
				offset = 0;
			}
			if (qp == null) // invalid query
				throw new ResponseException(ErrorType.InvalidRequest);
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
			QueryHistoryEntitiesRequest req = new QueryHistoryEntitiesRequest(HttpUtils.getHeaders(request), qp);
			QueryResult qResult = historyDAO.query(req.getQp());
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
	public ResponseEntity<byte[]> retrieveTemporalEntityById(ServerHttpRequest request,
			@PathVariable("entityId") String entityId) {
		// String params = request.getQueryString();
		try {
			logger.trace("retrieveTemporalEntityById :: started " + entityId);
			logger.debug("entityId : " + entityId);
			HttpUtils.validateUri(entityId);
			LinkedMultiValueMap<String, String> params = new LinkedMultiValueMap<String, String>(
					request.getQueryParams());
			params.add(NGSIConstants.QUERY_PARAMETER_ID, entityId);

			Context context = JsonLdProcessor.getCoreContextClone();
			List<Object> links = HttpUtils.getAtContext(request);
			context = context.parse(links, true);
			QueryParams qp = ParamsResolver.getQueryParamsFromUriQuery(params, context, true);
			logger.trace("retrieveTemporalEntityById :: completed");
			QueryHistoryEntitiesRequest req = new QueryHistoryEntitiesRequest(HttpUtils.getHeaders(request), qp);
			List<String> queryResult = historyDAO.query(req.getQp()).getActualDataString();
			if (queryResult == null) {
				System.err.println();
			}
			if (queryResult.isEmpty()) {
				throw new ResponseException(ErrorType.NotFound);
			}
			return HttpUtils.generateReply(request, historyDAO.getListAsJsonArray(queryResult),
					AppConstants.HISTORY_ENDPOINT);
		} catch (Exception exception) {
			return HttpUtils.handleControllerExceptions(exception);
		}
	}

	@DeleteMapping("/{entityId}")
	public ResponseEntity<byte[]> deleteTemporalEntityById(ServerHttpRequest request,
			@PathVariable("entityId") String entityId) {
		try {
			logger.trace("deleteTemporalEntityById :: started");
			logger.debug("entityId : " + entityId);
			HttpUtils.validateUri(entityId);
			LinkedMultiValueMap<String, String> params = new LinkedMultiValueMap<String, String>(
					request.getQueryParams());
			params.add(NGSIConstants.QUERY_PARAMETER_ID, entityId);
			Context context = JsonLdProcessor.getCoreContextClone();
			List<Object> links = HttpUtils.getAtContext(request);
			context = context.parse(links, true);
			QueryParams qp = ParamsResolver.getQueryParamsFromUriQuery(params, context, true);
			logger.trace("retrieveTemporalEntityById :: completed");
			QueryHistoryEntitiesRequest req = new QueryHistoryEntitiesRequest(HttpUtils.getHeaders(request), qp);
			List<String> queryResult = historyDAO.query(req.getQp()).getActualDataString();
			if (queryResult.isEmpty()) {
				throw new ResponseException(ErrorType.NotFound);
			}

			historyService.delete(HttpUtils.getHeaders(request), entityId, null, null, context);
			logger.trace("deleteTemporalEntityById :: completed");
			return ResponseEntity.noContent().build();
		} catch (Exception exception) {
			return HttpUtils.handleControllerExceptions(exception);
		}
	}

	@RequestMapping(method = RequestMethod.DELETE)
	public ResponseEntity<byte[]> deleteTemporalEntityIdIsEmpty(ServerHttpRequest request) {
		return ResponseEntity.status(HttpStatus.BAD_REQUEST)
				.body(new RestResponse(ErrorType.BadRequestData, "Bad Request").toJsonBytes());
	}

	@PostMapping("/{entityId}/attrs")
	public ResponseEntity<byte[]> addAttrib2TemopralEntity(ServerHttpRequest request,
			@PathVariable("entityId") String entityId, @RequestBody(required = false) String payload) {
		try {
			logger.trace("addAttrib2TemopralEntity :: started");
			logger.debug("entityId : " + entityId);
			List<Object> linkHeaders = HttpUtils.getAtContext(request);
			boolean atContextAllowed = HttpUtils.doPreflightCheck(request, linkHeaders);
			@SuppressWarnings("unchecked")
			Map<String, Object> resolved = (Map<String, Object>) JsonLdProcessor.expand(linkHeaders,
					JsonUtils.fromString(payload), opts, AppConstants.TEMP_ENTITY_UPDATE_PAYLOAD, atContextAllowed)
					.get(0);
			historyService.appendMessage(HttpUtils.getHeaders(request), entityId, resolved, null);
			logger.trace("addAttrib2TemopralEntity :: completed");
			return ResponseEntity.noContent().build();
		} catch (Exception exception) {
			return HttpUtils.handleControllerExceptions(exception);
		}
	}

	@DeleteMapping("/{entityId}/attrs/{attrId}")
	public ResponseEntity<byte[]> deleteAttrib2TemporalEntity(ServerHttpRequest request,
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
	public ResponseEntity<byte[]> modifyAttribInstanceTemporalEntity(ServerHttpRequest request,
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
	public ResponseEntity<byte[]> deleteAtrribInstanceTemporalEntity(ServerHttpRequest request,
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
