package eu.neclab.ngsildbroker.entityhandler.controller;

import java.net.URI;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.Collections;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonParseException;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.AppendResult;
import eu.neclab.ngsildbroker.commons.datatypes.RestResponse;
import eu.neclab.ngsildbroker.commons.datatypes.UpdateResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.ldcontext.ContextResolverBasic;
import eu.neclab.ngsildbroker.commons.ngsiqueries.ParamsResolver;
import eu.neclab.ngsildbroker.commons.ngsiqueries.QueryParser;
import eu.neclab.ngsildbroker.commons.securityConfig.ResourceConfigDetails;
import eu.neclab.ngsildbroker.commons.securityConfig.SecurityConfig;
import eu.neclab.ngsildbroker.commons.stream.service.KafkaOps;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.entityhandler.config.EntityProducerChannel;
import eu.neclab.ngsildbroker.entityhandler.services.EntityService;

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

//	@Autowired
//	@Qualifier("emops")
//	KafkaOps kafkaOps;

	@Autowired
	@Qualifier("emconRes")
	ContextResolverBasic contextResolver;

	@Autowired
	@Qualifier("emparamsres")
	ParamsResolver paramsResolver;

	@SuppressWarnings("unused")
	// TODO check to remove ... never used
	private EntityProducerChannel producerChannel;

	@Autowired
	public EntityController(EntityProducerChannel producerChannel) {
		this.producerChannel = producerChannel;
	}

	private HttpUtils httpUtils;

	@PostConstruct
	private void setup() {
		this.httpUtils = HttpUtils.getInstance(contextResolver);
	}

	LocalDateTime start;
	LocalDateTime end;

	public EntityController() {
	}

	/**
	 * Method(POST) for "/ngsi-ld/v1/entities/" rest endpoint.
	 * 
	 * @param payload
	 *            jsonld message
	 * @return ResponseEntity object
	 */
	@PostMapping("/")
	public ResponseEntity<byte[]> createEntity(HttpServletRequest request,
			@RequestBody(required = false) String payload) {
		String result = null;
		try {
			HttpUtils.doPreflightCheck(request, payload);
			logger.trace("create entity :: started");
			String resolved = httpUtils.expandPayload(request, payload);
			entityService.validateEntity(resolved, request);

			result = entityService.createMessage(resolved);
			logger.trace("create entity :: completed");
			return ResponseEntity.status(HttpStatus.CREATED).header("location", AppConstants.ENTITES_URL + result)
					.body(new URI(result).toString().getBytes());
		} catch (ResponseException exception) {
			logger.error("Exception :: ", exception);
			exception.printStackTrace();
			return ResponseEntity.status(exception.getHttpStatus()).body(new RestResponse(exception).toJsonBytes());
		} catch(DateTimeParseException exception) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST)
					.body(new RestResponse(ErrorType.BadRequestData, "Failed to parse provided datetime field.").toJsonBytes());
		} catch(JsonParseException exception) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST)
					.body(new RestResponse(ErrorType.BadRequestData, "There is an error in the provided json document").toJsonBytes());
		} catch (Exception exception) {
			logger.error("Exception :: ", exception);
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
					.body(new RestResponse(ErrorType.InternalError, "Internal error").toJsonBytes());
		}
	}

	/**
	 * Method(PATCH) for "/ngsi-ld/v1/entities/{entityId}/attrs" rest endpoint.
	 * 
	 * @param entityId
	 * @param payload
	 *            json ld message
	 * @return ResponseEntity object
	 */
	@PatchMapping("/{entityId}/attrs")
	public ResponseEntity<byte[]> updateEntity(HttpServletRequest request, @PathVariable("entityId") String entityId,
			@RequestBody String payload) {
		// String resolved = contextResolver.resolveContext(payload);
		try {
			HttpUtils.doPreflightCheck(request, payload);
			logger.trace("update entity :: started");

			String resolved = httpUtils.expandPayload(request, payload);

			UpdateResult update = entityService.updateMessage(entityId, resolved);
			logger.trace("update entity :: completed");
			if (update.getUpdateResult()) {
				return ResponseEntity.noContent().build();
			} else {
				return ResponseEntity.status(HttpStatus.MULTI_STATUS).body(objectMapper.writeValueAsBytes(update.getAppendedJsonFields()));
			}
		} catch (ResponseException responseException) {
			logger.error("Exception :: ", responseException);
			return ResponseEntity.status(responseException.getHttpStatus()).body(new RestResponse(responseException).toJsonBytes());
		} catch(DateTimeParseException exception) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST)
					.body(new RestResponse(ErrorType.BadRequestData, "Failed to parse provided datetime field.").toJsonBytes());
		} catch(JsonParseException exception) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST)
					.body(new RestResponse(ErrorType.BadRequestData, "There is an error in the provided json document").toJsonBytes());
		} catch (Exception e) {
			logger.error("Exception :: ", e);
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
					.body(new RestResponse(ErrorType.InternalError, "Internal error").toJsonBytes());
		}
	}

	/**
	 * Method(POST) for "/ngsi-ld/v1/entities/{entityId}/attrs" rest endpoint.
	 * 
	 * @param entityId
	 * @param payload
	 *            jsonld message
	 * @return ResponseEntity object
	 */
	@PostMapping("/{entityId}/attrs")
	public ResponseEntity<byte[]> appendEntity(HttpServletRequest request, @PathVariable("entityId") String entityId,
			@RequestBody String payload, @RequestParam(required = false, name = "options") String options) {
		// String resolved = contextResolver.resolveContext(payload);
		try {
			HttpUtils.doPreflightCheck(request, payload);
			logger.trace("append entity :: started");
			String resolved = httpUtils.expandPayload(request, payload);

			AppendResult append = entityService.appendMessage(entityId, resolved, options);
			logger.trace("append entity :: completed");
			if (append.getAppendResult()) {
				return ResponseEntity.noContent().build();
			} else {
				return ResponseEntity.status(HttpStatus.MULTI_STATUS).body(objectMapper.writeValueAsBytes(append.getAppendedJsonFields()));
			}
		} catch (ResponseException responseException) {
			logger.error("Exception :: ", responseException);
			return ResponseEntity.status(responseException.getHttpStatus()).body(new RestResponse(responseException).toJsonBytes());
		} catch(DateTimeParseException exception) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST)
					.body(new RestResponse(ErrorType.BadRequestData, "Failed to parse provided datetime field.").toJsonBytes());
		} catch(JsonParseException exception) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST)
					.body(new RestResponse(ErrorType.BadRequestData, "There is an error in the provided json document").toJsonBytes());
		} catch (Exception exception) {
			logger.error("Exception :: ", exception);
			exception.printStackTrace();
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
					.body(new RestResponse(ErrorType.InternalError, "Internal error").toJsonBytes());
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
	public ResponseEntity<byte[]> partialUpdateEntity(HttpServletRequest request,
			@PathVariable("entityId") String entityId, @PathVariable("attrId") String attrId,
			@RequestBody String payload) {
		try {
			HttpUtils.doPreflightCheck(request, payload);
			logger.trace("partial-update entity :: started");

			String expandedPayload = httpUtils.expandPayload(request, payload);

			String expandedAttrib = paramsResolver.expandAttribute(attrId, payload, request);

			UpdateResult update = entityService.partialUpdateEntity(entityId, expandedAttrib, expandedPayload);
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
			return ResponseEntity.status(responseException.getHttpStatus()).body(new RestResponse(responseException).toJsonBytes());
		} catch(DateTimeParseException exception) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST)
					.body(new RestResponse(ErrorType.BadRequestData, "Failed to parse provided datetime field.").toJsonBytes());
		} catch(JsonParseException exception) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST)
					.body(new RestResponse(ErrorType.BadRequestData, "There is an error in the provided json document").toJsonBytes());
		} catch (Exception exception) {
			logger.error("Exception :: ", exception);
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
					.body(new RestResponse(ErrorType.InternalError, "Internal error").toJsonBytes());
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
	@DeleteMapping("/{entityId}/attrs/{attrId}")
	public ResponseEntity<byte[]> deleteAttribute(HttpServletRequest request, @PathVariable("entityId") String entityId,
			@PathVariable("attrId") String attrId) {
		try {
			logger.trace("delete attribute :: started");
			String expandedAttrib = paramsResolver.expandAttribute(attrId, HttpUtils.getAtContext(request));
			entityService.deleteAttribute(entityId, expandedAttrib);
			logger.trace("delete attribute :: completed");
			return ResponseEntity.noContent().build();
		} catch (ResponseException responseException) {
			logger.error("Exception :: ", responseException);
			return ResponseEntity.status(responseException.getHttpStatus()).body(new RestResponse(responseException).toJsonBytes());
		} catch(DateTimeParseException exception) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST)
					.body(new RestResponse(ErrorType.BadRequestData, "Failed to parse provided datetime field.").toJsonBytes());
		} catch(JsonParseException exception) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST)
					.body(new RestResponse(ErrorType.BadRequestData, "There is an error in the provided json document").toJsonBytes());
		} catch (Exception exception) {
			logger.error("Exception :: ", exception);
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
					.body(new RestResponse(ErrorType.InternalError, "Internal error").toJsonBytes());
		}
	}

	/**
	 * Method(DELETE) for "/ngsi-ld/v1/entities/{entityId}" rest endpoint.
	 * 
	 * @param entityId
	 * @return
	 */
	@DeleteMapping("/{entityId}")
	public ResponseEntity<byte[]> deleteEntity(@PathVariable("entityId") String entityId) {
		try {
			logger.trace("delete entity :: started");
			entityService.deleteEntity(entityId);
			logger.trace("delete entity :: completed");
			return ResponseEntity.noContent().build();
		} catch (ResponseException responseException) {
			logger.error("Exception :: ", responseException);
			return ResponseEntity.status(responseException.getHttpStatus()).body(new RestResponse(responseException).toJsonBytes());
		} catch(DateTimeParseException exception) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST)
					.body(new RestResponse(ErrorType.BadRequestData, "Failed to parse provided datetime field.").toJsonBytes());
		} catch(JsonParseException exception) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST)
					.body(new RestResponse(ErrorType.BadRequestData, "There is an error in the provided json document").toJsonBytes());
		} catch (Exception exception) {
			logger.error("Exception :: ", exception);
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
					.body(new RestResponse(ErrorType.InternalError, "Internal error").toJsonBytes());
		}
	}

}
