package eu.neclab.ngsildbroker.historymanager.controller;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.datatypes.RestResponse;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.ldcontext.ContextResolverBasic;
import eu.neclab.ngsildbroker.commons.ngsiqueries.ParamsResolver;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
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
	@Autowired
	ContextResolverBasic contextResolver;
	@Value("${atcontext.url}")
	String atContextServerUrl;

	private HttpUtils httpUtils;

	@PostConstruct
	private void setup() {
		this.httpUtils = HttpUtils.getInstance(contextResolver);
	}

	@PostMapping
	public ResponseEntity<byte[]> createTemporalEntity(HttpServletRequest request,
			@RequestBody(required = false) String payload) {
		try {
			logger.trace("createTemporalEntity :: started");
			Validator.validateTemporalEntity(payload);

			String resolved = httpUtils.expandPayload(request, payload, AppConstants.HISTORY_URL_ID);

			URI uri = historyService.createTemporalEntityFromBinding(resolved);
			logger.trace("createTemporalEntity :: completed");
			return ResponseEntity.status(HttpStatus.CREATED).header("Location", uri.toString()).body(uri.toString().getBytes());
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
	public ResponseEntity<byte[]> retrieveTemporalEntity(HttpServletRequest request) {
		String params = request.getQueryString();
		try {
			logger.trace("retrieveTemporalEntity :: started");
			if (params != null && !Validator.validate(params))
				throw new ResponseException(ErrorType.BadRequestData);

			QueryParams qp = paramsResolver.getQueryParamsFromUriQuery(request.getParameterMap(),
					HttpUtils.parseLinkHeader(request, NGSIConstants.HEADER_REL_LDCONTEXT), true);
			if (qp == null) // invalid query
				throw new ResponseException(ErrorType.InvalidRequest);
			if (qp.getTimerel() == null || qp.getTime() == null) {
				throw new ResponseException(ErrorType.BadRequestData, "Time filter is required");
			}
			if (qp.getType() == null && qp.getAttrs() == null) {
				throw new ResponseException(ErrorType.BadRequestData, "Type or attrs is required");
			}

			logger.trace("retrieveTemporalEntity :: completed");
			return httpUtils.generateReply(request, historyDAO.getListAsJsonArray(historyDAO.query(qp)));
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
	public ResponseEntity<byte[]> retrieveTemporalEntityById(HttpServletRequest request,
			@PathVariable("entityId") String entityId) {
		String params = request.getQueryString();
		try {
			logger.trace("retrieveTemporalEntityById :: started " + entityId);
			logger.debug("entityId : " + entityId);
			if (params != null && !Validator.validate(params))
				throw new ResponseException(ErrorType.BadRequestData);

			QueryParams qp = paramsResolver.getQueryParamsFromUriQuery(request.getParameterMap(),
					HttpUtils.parseLinkHeader(request, NGSIConstants.HEADER_REL_LDCONTEXT), true);
			qp.setId(entityId);
			logger.trace("retrieveTemporalEntityById :: completed");
			return httpUtils.generateReply(request, historyDAO.getListAsJsonArray(historyDAO.query(qp)));
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
	public ResponseEntity<byte[]> deleteTemporalEntityById(HttpServletRequest request,
			@PathVariable("entityId") String entityId) {
		try {
			logger.trace("deleteTemporalEntityById :: started");
			logger.debug("entityId : " + entityId);
			historyService.delete(entityId, null, null,
					HttpUtils.parseLinkHeader(request, NGSIConstants.HEADER_REL_LDCONTEXT));
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

	@PostMapping("/{entityId}/attrs")
	public ResponseEntity<byte[]> addAttrib2TemopralEntity(HttpServletRequest request,
			@PathVariable("entityId") String entityId, @RequestBody(required = false) String payload) {
		try {
			logger.trace("addAttrib2TemopralEntity :: started");
			logger.debug("entityId : " + entityId);
			String resolved = httpUtils.expandPayload(request, payload, AppConstants.HISTORY_URL_ID);

			historyService.addAttrib2TemporalEntity(entityId, resolved);
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
	public ResponseEntity<byte[]> deleteAttrib2TemporalEntity(HttpServletRequest request,
			@PathVariable("entityId") String entityId, @PathVariable("attrId") String attrId) {
		try {
			logger.trace("deleteAttrib2TemporalEntity :: started");
			logger.debug("entityId : " + entityId + " attrId : " + attrId);
			historyService.delete(entityId, attrId, null,
					HttpUtils.parseLinkHeader(request, NGSIConstants.HEADER_REL_LDCONTEXT));
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
	public ResponseEntity<byte[]> modifyAttribInstanceTemporalEntity(HttpServletRequest request,
			@PathVariable("entityId") String entityId, @PathVariable("attrId") String attrId,
			@PathVariable("instanceId") String instanceId, @RequestBody(required = false) String payload) {
		try {
			logger.trace("modifyAttribInstanceTemporalEntity :: started");
			logger.debug("entityId : " + entityId + " attrId : " + attrId + " instanceId : " + instanceId);

			String resolved = httpUtils.expandPayload(request, payload, AppConstants.HISTORY_URL_ID);

			// TODO : TBD- conflict between specs and implementation <mentioned no request
			// body in specs>
			historyService.modifyAttribInstanceTemporalEntity(entityId, resolved, attrId, instanceId,
					HttpUtils.parseLinkHeader(request, NGSIConstants.HEADER_REL_LDCONTEXT));
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
	public ResponseEntity<byte[]> deleteAtrribInstanceTemporalEntity(HttpServletRequest request,
			@PathVariable("entityId") String entityId, @PathVariable("attrId") String attrId,
			@PathVariable("instanceId") String instanceId) {
		try {
			logger.trace("deleteAtrribInstanceTemporalEntity :: started");
			logger.debug("entityId : " + entityId + " attrId : " + attrId + " instanceId : " + instanceId);
			historyService.delete(entityId, attrId, instanceId,
					HttpUtils.parseLinkHeader(request, NGSIConstants.HEADER_REL_LDCONTEXT));
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
