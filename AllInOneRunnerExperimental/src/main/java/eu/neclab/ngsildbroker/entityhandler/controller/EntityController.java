package eu.neclab.ngsildbroker.entityhandler.controller;

import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLdConsts;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.gson.JsonParseException;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.AppendResult;
import eu.neclab.ngsildbroker.commons.datatypes.RestResponse;
import eu.neclab.ngsildbroker.commons.datatypes.UpdateResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.ngsiqueries.ParamsResolver;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.ValidateURI;
import eu.neclab.ngsildbroker.entityhandler.services.EntityService;
import eu.neclab.ngsildbroker.entityhandler.validationutil.Validator;

/**
 * 
 * @version 1.0
 * @date 10-Jul-2018
 */
@RestController
@RequestMapping("/ngsi-ld/v1/entities")
public class EntityController {// implements EntityHandlerInterface {

	private final static Logger logger = LoggerFactory.getLogger(EntityController.class);

	@Autowired
	EntityService entityService;
	@Autowired
	ObjectMapper objectMapper;

	LocalDateTime startAt;
	LocalDateTime endAt;

	private JsonLdOptions opts = new JsonLdOptions(JsonLdOptions.JSON_LD_1_1);
	@Value("${ngsild.corecontext:https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld}")
	String coreContext;

	@PostConstruct
	public void init() {
		JsonLdProcessor.init(coreContext);
	}

	public EntityController() {
	}

	/**
	 * Method(POST) for "/ngsi-ld/v1/entities/" rest endpoint.
	 * 
	 * @param payload jsonld message
	 * @return ResponseEntity object
	 */
	@PostMapping
	public ResponseEntity<byte[]> createEntity(ServerHttpRequest request,
			@RequestBody(required = false) String payload) {
		String result = null;
		try {
			logger.trace("create entity :: started");
			Map<String, Object> resolved = (Map<String, Object>) JsonLdProcessor
					.expand(HttpUtils.getAtContext(request), JsonUtils.fromString(payload), opts,
							AppConstants.ENTITY_CREATE_PAYLOAD, HttpUtils.doPreflightCheck(request))
					.get(0);
			result = entityService.createMessage(HttpUtils.getHeaders(request), resolved);
			logger.trace("create entity :: completed");
			return ResponseEntity.status(HttpStatus.CREATED).header("location", AppConstants.ENTITES_URL + result)
					.build();
		} catch (ResponseException exception) {
			logger.error("Exception :: ", exception);
			exception.printStackTrace();
			return ResponseEntity.status(exception.getHttpStatus()).body(new RestResponse(exception).toJsonBytes());
		} catch (DateTimeParseException exception) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST)
					.body(new RestResponse(ErrorType.BadRequestData, "Failed to parse provided datetime field.")
							.toJsonBytes());
		} catch (JsonProcessingException exception) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST)
					.body(new RestResponse(ErrorType.BadRequestData, "There is an error in the provided json document")
							.toJsonBytes());
		} catch (Exception exception) {
			logger.error("Exception :: ", exception);
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
					.body(new RestResponse(ErrorType.InternalError, exception.getLocalizedMessage()).toJsonBytes());
		}
	}

	/**
	 * Method(PATCH) for "/ngsi-ld/v1/entities/{entityId}/attrs" rest endpoint.
	 * 
	 * @param entityId
	 * @param payload  json ld message
	 * @return ResponseEntity object
	 */
	@PatchMapping("/{entityId}/attrs")
	public ResponseEntity<byte[]> updateEntity(ServerHttpRequest request, @PathVariable("entityId") String entityId,
			@RequestBody String payload) {
		// String resolved = contextResolver.resolveContext(payload);
		try {

			// String[] split =
			// request.getPath().toString().replace("/ngsi-ld/v1/entities/",
			// "").split("/attrs");

			logger.trace("update entity :: started");
			Map<String, Object> resolved = (Map<String, Object>) JsonLdProcessor
					.expand(HttpUtils.getAtContext(request), JsonUtils.fromString(payload), opts,
							AppConstants.ENTITY_UPDATE_PAYLOAD, HttpUtils.doPreflightCheck(request))
					.get(0);
			UpdateResult update = entityService.updateMessage(HttpUtils.getHeaders(request), entityId, resolved);
			logger.trace("update entity :: completed");
			if (update.getUpdateResult()) {
				return ResponseEntity.noContent().build();
			} else {
				return ResponseEntity.status(HttpStatus.MULTI_STATUS)
						.body(objectMapper.writeValueAsBytes(update.getAppendedJsonFields()));
			}
		} catch (ResponseException responseException) {
			logger.error("Exception :: ", responseException);
			return ResponseEntity.status(responseException.getHttpStatus())
					.body(new RestResponse(responseException).toJsonBytes());
		} catch (DateTimeParseException exception) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST)
					.body(new RestResponse(ErrorType.BadRequestData, "Failed to parse provided datetime field.")
							.toJsonBytes());
		} catch (JsonProcessingException exception) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST)
					.body(new RestResponse(ErrorType.BadRequestData, "There is an error in the provided json document")
							.toJsonBytes());
		} catch (Exception e) {
			logger.error("Exception :: ", e);
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
					.body(new RestResponse(ErrorType.InternalError, e.getLocalizedMessage()).toJsonBytes());
		}
	}

	/**
	 * Method(POST) for "/ngsi-ld/v1/entities/{entityId}/attrs" rest endpoint.
	 * 
	 * @param entityId
	 * @param payload  jsonld message
	 * @return ResponseEntity object
	 */
	@PostMapping("/{entityId}/attrs")
	public ResponseEntity<byte[]> appendEntity(ServerHttpRequest request, @PathVariable("entityId") String entityId,
			@RequestBody String payload, @RequestParam(required = false, name = "options") String options) {
		// String resolved = contextResolver.resolveContext(payload);
		try {
			// String[] split =
			// request.getPath().toString().replace("/ngsi-ld/v1/entities/",
			// "").split("/attrs");
			entityId = HttpUtils.denormalize(entityId);

			logger.trace("append entity :: started");
			Map<String, Object> resolved = (Map<String, Object>) JsonLdProcessor
					.expand(HttpUtils.getAtContext(request), JsonUtils.fromString(payload), opts,
							AppConstants.ENTITY_UPDATE_PAYLOAD, HttpUtils.doPreflightCheck(request))
					.get(0);
			AppendResult append = entityService.appendMessage(HttpUtils.getHeaders(request), entityId, resolved,
					options);
			logger.trace("append entity :: completed");
			if (append.getAppendResult()) {
				return ResponseEntity.noContent().build();
			} else {
				return ResponseEntity.status(HttpStatus.MULTI_STATUS)
						.body(objectMapper.writeValueAsBytes(append.getAppendedJsonFields()));
			}
		} catch (ResponseException responseException) {
			logger.error("Exception :: ", responseException);
			return ResponseEntity.status(responseException.getHttpStatus())
					.body(new RestResponse(responseException).toJsonBytes());
		} catch (DateTimeParseException exception) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST)
					.body(new RestResponse(ErrorType.BadRequestData, "Failed to parse provided datetime field.")
							.toJsonBytes());
		} catch (JsonProcessingException exception) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST)
					.body(new RestResponse(ErrorType.BadRequestData, "There is an error in the provided json document")
							.toJsonBytes());
		} catch (Exception exception) {
			logger.error("Exception :: ", exception);
			exception.printStackTrace();
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
					.body(new RestResponse(ErrorType.InternalError, exception.getLocalizedMessage()).toJsonBytes());
		}
	}

	/**
	 * Method(PATCH) for "/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}" rest
	 * endpoint.
	 * 
	 * @param entityId
	 * @param attrId
	 * @param payload
	 * @return
	 */
	@PatchMapping("/{entityId}/attrs/{attrId}")
	public ResponseEntity<byte[]> partialUpdateEntity(ServerHttpRequest request,
			@PathVariable("entityId") String entityId, @PathVariable("attrId") String attrId,
			@RequestBody String payload) {
		try {
			// String[] split =
			// request.getPath().toString().replace("/ngsi-ld/v1/entities/",
			// "").split("/attrs/");
			Object jsonPayload = JsonUtils.fromString(payload);
			List<Object> atContext = HttpUtils.getAtContext(request);
			logger.trace("partial-update entity :: started");
			Map<String, Object> expandedPayload = (Map<String, Object>) JsonLdProcessor.expand(atContext, jsonPayload,
					opts, AppConstants.ENTITY_ATTRS_UPDATE_PAYLOAD, HttpUtils.doPreflightCheck(request)).get(0);
			Context context = JsonLdProcessor.getCoreContextClone();
			context = context.parse(atContext, true);
			if (jsonPayload instanceof Map) {
				Object payloadContext = ((Map<String, Object>) jsonPayload).get(JsonLdConsts.CONTEXT);
				context = context.parse(payloadContext, true);
			}
			String expandedAttrib = ParamsResolver.expandAttribute(attrId, context);

			UpdateResult update = entityService.partialUpdateEntity(HttpUtils.getHeaders(request), entityId,
					expandedAttrib, expandedPayload);
			logger.trace("partial-update entity :: completed");
			if (update.getStatus()) {
				return ResponseEntity.noContent().build();
			} else {
				return ResponseEntity.status(HttpStatus.BAD_REQUEST).build();
			}
			/*
			 * There is no 207 multi status response in the Partial Attribute Update
			 * operation. Section 6.7.3.1 else { return
			 * ResponseEntity.status(HttpStatus.MULTI_STATUS).body(update.
			 * getAppendedJsonFields()); }
			 */
		} catch (ResponseException responseException) {
			logger.error("Exception :: ", responseException);
			return ResponseEntity.status(responseException.getHttpStatus())
					.body(new RestResponse(responseException).toJsonBytes());
		} catch (DateTimeParseException exception) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST)
					.body(new RestResponse(ErrorType.BadRequestData, "Failed to parse provided datetime field.")
							.toJsonBytes());
		} catch (JsonProcessingException exception) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST)
					.body(new RestResponse(ErrorType.BadRequestData, "There is an error in the provided json document")
							.toJsonBytes());
		} catch (Exception exception) {
			logger.error("Exception :: ", exception);
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
					.body(new RestResponse(ErrorType.InternalError, exception.getLocalizedMessage()).toJsonBytes());
		}
	}

	/**
	 * Method(DELETE) for "/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}" rest
	 * endpoint.
	 * 
	 * @param entityId
	 * @param attrId
	 * @return
	 */
	@DeleteMapping("/**")
	public ResponseEntity<byte[]> deleteAttribute(ServerHttpRequest request,
			@RequestParam(value = "datasetId", required = false) String datasetId,
			@RequestParam(value = "deleteAll", required = false) String deleteAll) {
		try {
			String path = request.getPath().toString().replace("/ngsi-ld/v1/entities/", "");
			if (path.contains("/attrs/")) {
				String[] split = path.split("/attrs/");
				String attrId = HttpUtils.denormalize(split[1]);
				String entityId = HttpUtils.denormalize(split[0]);
				ValidateURI.validateUri(entityId);
				logger.trace("delete attribute :: started");
				Validator.validate(request.getQueryParams());
				Context context = JsonLdProcessor.getCoreContextClone();
				context = context.parse(HttpUtils.getAtContext(request), true);
				String expandedAttrib = ParamsResolver.expandAttribute(attrId, context);
				entityService.deleteAttribute(HttpUtils.getHeaders(request), entityId, expandedAttrib, datasetId,
						deleteAll);
				logger.trace("delete attribute :: completed");
				return ResponseEntity.noContent().build();
			} else {
				return deleteEntity(request);
			}
		} catch (ResponseException responseException) {
			logger.error("Exception :: ", responseException);
			return ResponseEntity.status(responseException.getHttpStatus())
					.body(new RestResponse(responseException).toJsonBytes());
		} catch (DateTimeParseException exception) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST)
					.body(new RestResponse(ErrorType.BadRequestData, "Failed to parse provided datetime field.")
							.toJsonBytes());
		} catch (JsonProcessingException exception) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST)
					.body(new RestResponse(ErrorType.BadRequestData, "There is an error in the provided json document")
							.toJsonBytes());
		} catch (Exception exception) {
			logger.error("Exception :: ", exception);
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
					.body(new RestResponse(ErrorType.InternalError, exception.getLocalizedMessage()).toJsonBytes());
		}
	}

	/**
	 * Method(DELETE) for "/ngsi-ld/v1/entities/{entityId}" rest endpoint.
	 * 
	 * @param entityId
	 * @return
	 */
	public ResponseEntity<byte[]> deleteEntity(ServerHttpRequest request) {
		try {
			String entityId = request.getPath().toString().replace("/ngsi-ld/v1/entities/", "");
			logger.trace("delete entity :: started");
			ValidateURI.validateUri(entityId);
			entityService.deleteEntity(HttpUtils.getHeaders(request), entityId);
			logger.trace("delete entity :: completed");
			return ResponseEntity.noContent().build();
		} catch (ResponseException responseException) {
			logger.error("Exception :: ", responseException);
			return ResponseEntity.status(responseException.getHttpStatus())
					.body(new RestResponse(responseException).toJsonBytes());
		} catch (DateTimeParseException exception) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST)
					.body(new RestResponse(ErrorType.BadRequestData, "Failed to parse provided datetime field.")
							.toJsonBytes());
		} catch (JsonProcessingException exception) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST)
					.body(new RestResponse(ErrorType.BadRequestData, "There is an error in the provided json document")
							.toJsonBytes());
		} catch (Exception exception) {
			logger.error("Exception :: ", exception);
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
					.body(new RestResponse(ErrorType.InternalError, exception.getLocalizedMessage()).toJsonBytes());
		}
	}
}
