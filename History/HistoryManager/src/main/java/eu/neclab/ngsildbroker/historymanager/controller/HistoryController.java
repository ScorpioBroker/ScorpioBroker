package eu.neclab.ngsildbroker.historymanager.controller;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.ArrayListMultimap;

import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.http.HttpHeaders;
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
import eu.neclab.ngsildbroker.commons.tools.ValidateURI;
import eu.neclab.ngsildbroker.historymanager.repository.HistoryDAO;
import eu.neclab.ngsildbroker.historymanager.service.HistoryService;
import eu.neclab.ngsildbroker.historymanager.utils.Validator;

@RestController
@RequestMapping("/ngsi-ld/v1/temporal/entities")
public class HistoryController {

	private final static Logger logger = LoggerFactory.getLogger(HistoryController.class);

	@Autowired
	ParamsResolver paramsResolver;

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

			Map<String, Object> resolved = (Map<String, Object>) JsonLdProcessor
					.expand(HttpUtils.getAtContext(request), JsonUtils.fromString(payload), opts,
							AppConstants.TEMP_ENTITY_CREATE_PAYLOAD, HttpUtils.doPreflightCheck(request))
					.get(0);
			URI uri = historyService.createTemporalEntityFromBinding(HttpUtils.getHeaders(request), resolved);
			logger.trace("createTemporalEntity :: completed");
			return ResponseEntity.status(HttpStatus.CREATED).header("Location", uri.toString())
					.body(uri.toString().getBytes());
		} catch (ResponseException exception) {
			logger.error("Exception", exception);
			return ResponseEntity.status(exception.getHttpStatus()).body(new RestResponse(exception).toJsonBytes());
		} catch (Exception exception) {
			logger.error("Exception", exception);
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
					.body(new RestResponse(ErrorType.InternalError, exception.getLocalizedMessage()).toJsonBytes());
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
			Context context = JsonLdProcessor.coreContext.clone();
			List<Object> links = HttpUtils.getAtContext(request);
			context = context.parse(links, true);
			QueryParams qp = paramsResolver.getQueryParamsFromUriQuery(request.getQueryParams(), context, true);
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
			if (qp.getEntities() == null && qp.getAttrs() == null) {
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
						additionalHeaders);
			} else {
				return HttpUtils.generateReply(request, "[]", additionalHeaders);
			}
		} catch (ResponseException ex) {
			logger.error("Exception", ex);
			return ResponseEntity.status(ex.getHttpStatus()).body(new RestResponse(ex).toJsonBytes());
		} catch (Exception ex) {
			logger.error("Exception", ex);
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
					.body(new RestResponse(ErrorType.InternalError, ex.getLocalizedMessage()).toJsonBytes());
		}
	}

	@GetMapping("/{entityId}")
	public ResponseEntity<byte[]> retrieveTemporalEntityById(ServerHttpRequest request,
			@PathVariable("entityId") String entityId) {
		// String params = request.getQueryString();
		try {
			logger.trace("retrieveTemporalEntityById :: started " + entityId);
			logger.debug("entityId : " + entityId);
			ValidateURI.validateUri(entityId);
			// if (params != null && !Validator.validate(params))
			// throw new ResponseException(ErrorType.BadRequestData);
			// Map<String, String[]> queryParam = new HashMap<>(request.getParameterMap());
			// String[] entityArray = new String[] { entityId };
			// queryParam.put(NGSIConstants.QUERY_PARAMETER_ID, entityArray);
			LinkedMultiValueMap<String, String> params = new LinkedMultiValueMap<String, String>(
					request.getQueryParams());
			params.add(NGSIConstants.QUERY_PARAMETER_ID, entityId);

			Context context = JsonLdProcessor.coreContext.clone();
			List<Object> links = HttpUtils.getAtContext(request);
			context = context.parse(links, true);
			QueryParams qp = paramsResolver.getQueryParamsFromUriQuery(params, context, true);
			logger.trace("retrieveTemporalEntityById :: completed");
			QueryHistoryEntitiesRequest req = new QueryHistoryEntitiesRequest(HttpUtils.getHeaders(request), qp);
			List<String> queryResult = historyDAO.query(req.getQp()).getActualDataString();
			if (queryResult.isEmpty()) {
				throw new ResponseException(ErrorType.NotFound);
			}
			return HttpUtils.generateReply(request, historyDAO.getListAsJsonArray(queryResult));
		} catch (ResponseException ex) {
			logger.error("Exception", ex);
			return ResponseEntity.status(ex.getHttpStatus()).body(new RestResponse(ex).toJsonBytes());
		} catch (Exception ex) {
			logger.error("Exception", ex);
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
					.body(new RestResponse(ErrorType.InternalError, ex.getLocalizedMessage()).toJsonBytes());
		}
	}

	@DeleteMapping("/{entityId}")
	public ResponseEntity<byte[]> deleteTemporalEntityById(ServerHttpRequest request,
			@PathVariable("entityId") String entityId) {
		try {
			logger.trace("deleteTemporalEntityById :: started");
			logger.debug("entityId : " + entityId);
			ValidateURI.validateUri(entityId);
			LinkedMultiValueMap<String, String> params = new LinkedMultiValueMap<String, String>(
					request.getQueryParams());
			params.add(NGSIConstants.QUERY_PARAMETER_ID, entityId);
			Context context = JsonLdProcessor.coreContext.clone();
			List<Object> links = HttpUtils.getAtContext(request);
			context = context.parse(links, true);
			QueryParams qp = paramsResolver.getQueryParamsFromUriQuery(params, context, true);
			logger.trace("retrieveTemporalEntityById :: completed");
			QueryHistoryEntitiesRequest req = new QueryHistoryEntitiesRequest(HttpUtils.getHeaders(request), qp);
			List<String> queryResult = historyDAO.query(req.getQp()).getActualDataString();
			if (queryResult.isEmpty()) {
				throw new ResponseException(ErrorType.NotFound);
			}

			historyService.delete(HttpUtils.getHeaders(request), entityId, null, null, context);
			logger.trace("deleteTemporalEntityById :: completed");
			return ResponseEntity.noContent().build();
		} catch (ResponseException ex) {
			logger.error("Exception", ex);
			return ResponseEntity.status(ex.getHttpStatus()).body(new RestResponse(ex).toJsonBytes());
		} catch (Exception ex) {
			logger.error("Exception", ex);
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
					.body(new RestResponse(ErrorType.InternalError, ex.getLocalizedMessage()).toJsonBytes());
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
			Map<String, Object> resolved = (Map<String, Object>) JsonLdProcessor
					.expand(HttpUtils.getAtContext(request), JsonUtils.fromString(payload), opts,
							AppConstants.TEMP_ENTITY_UPDATE_PAYLOAD, HttpUtils.doPreflightCheck(request))
					.get(0);
			historyService.addAttrib2TemporalEntity(HttpUtils.getHeaders(request), entityId, resolved);
			logger.trace("addAttrib2TemopralEntity :: completed");
			return ResponseEntity.noContent().build();
		} catch (ResponseException ex) {
			logger.error("Exception", ex);
			return ResponseEntity.status(ex.getHttpStatus()).body(new RestResponse(ex).toJsonBytes());
		} catch (Exception ex) {
			logger.error("Exception", ex);
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
					.body(new RestResponse(ErrorType.InternalError, ex.getLocalizedMessage()).toJsonBytes());
		}
	}

	@DeleteMapping("/{entityId}/attrs/{attrId}")
	public ResponseEntity<byte[]> deleteAttrib2TemporalEntity(ServerHttpRequest request,
			@PathVariable("entityId") String entityId, @PathVariable("attrId") String attrId) {
		try {
			ValidateURI.validateUri(entityId);
			Context context = JsonLdProcessor.coreContext.clone();
			List<Object> links = HttpUtils.getAtContext(request);
			context = context.parse(links, true);
			logger.trace("deleteAttrib2TemporalEntity :: started");
			logger.debug("entityId : " + entityId + " attrId : " + attrId);
			historyService.delete(HttpUtils.getHeaders(request), entityId, attrId, null, context);
			logger.trace("deleteAttrib2TemporalEntity :: completed");
			return ResponseEntity.noContent().build();
		} catch (ResponseException ex) {
			logger.error("Exception", ex);
			return ResponseEntity.status(ex.getHttpStatus()).body(new RestResponse(ex).toJsonBytes());
		} catch (Exception ex) {
			logger.error("Exception", ex);
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
					.body(new RestResponse(ErrorType.InternalError, ex.getLocalizedMessage()).toJsonBytes());
		}
	}

	@PatchMapping("/{entityId}/attrs/{attrId}/{instanceId}")
	public ResponseEntity<byte[]> modifyAttribInstanceTemporalEntity(ServerHttpRequest request,
			@PathVariable("entityId") String entityId, @PathVariable("attrId") String attrId,
			@PathVariable("instanceId") String instanceId, @RequestBody(required = false) String payload) {
		try {
			logger.trace("modifyAttribInstanceTemporalEntity :: started");
			logger.debug("entityId : " + entityId + " attrId : " + attrId + " instanceId : " + instanceId);
			Context context = JsonLdProcessor.coreContext.clone();
			List<Object> links = HttpUtils.getAtContext(request);
			context = context.parse(links, true);

			Map<String, Object> resolved = (Map<String, Object>) JsonLdProcessor
					.expand(links, JsonUtils.fromString(payload), opts, AppConstants.TEMP_ENTITY_UPDATE_PAYLOAD,
							HttpUtils.doPreflightCheck(request))
					.get(0);
			// TODO : TBD- conflict between specs and implementation <mentioned no request
			// body in specs>
			historyService.modifyAttribInstanceTemporalEntity(HttpUtils.getHeaders(request), entityId, resolved, attrId,
					instanceId, context);
			logger.trace("modifyAttribInstanceTemporalEntity :: completed");
			return ResponseEntity.noContent().build();
		} catch (ResponseException ex) {
			logger.error("Exception", ex);
			return ResponseEntity.status(ex.getHttpStatus()).body(new RestResponse(ex).toJsonBytes());
		} catch (Exception ex) {
			logger.error("Exception", ex);
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
					.body(new RestResponse(ErrorType.InternalError, ex.getLocalizedMessage()).toJsonBytes());
		}
	}

	@DeleteMapping("/{entityId}/attrs/{attrId}/{instanceId}")
	public ResponseEntity<byte[]> deleteAtrribInstanceTemporalEntity(ServerHttpRequest request,
			@PathVariable("entityId") String entityId, @PathVariable("attrId") String attrId,
			@PathVariable("instanceId") String instanceId) {
		try {
			logger.trace("deleteAtrribInstanceTemporalEntity :: started");
			logger.debug("entityId : " + entityId + " attrId : " + attrId + " instanceId : " + instanceId);
			Context context = JsonLdProcessor.coreContext.clone();
			List<Object> links = HttpUtils.getAtContext(request);
			context = context.parse(links, true);

			historyService.delete(HttpUtils.getHeaders(request), entityId, attrId, instanceId, context);
			logger.trace("deleteAtrribInstanceTemporalEntity :: completed");
			return ResponseEntity.noContent().build();
		} catch (ResponseException ex) {
			logger.error("Exception", ex);
			return ResponseEntity.status(ex.getHttpStatus()).body(new RestResponse(ex).toJsonBytes());
		} catch (Exception ex) {
			logger.error("Exception", ex);
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
					.body(new RestResponse(ErrorType.InternalError, ex.getLocalizedMessage()).toJsonBytes());
		}
	}
}
