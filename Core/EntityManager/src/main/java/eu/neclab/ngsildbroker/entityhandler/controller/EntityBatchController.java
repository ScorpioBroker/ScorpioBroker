package eu.neclab.ngsildbroker.entityhandler.controller;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonParseException;
import com.github.jsonldjava.core.JsonLdError;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.AppendResult;
import eu.neclab.ngsildbroker.commons.datatypes.BatchFailure;
import eu.neclab.ngsildbroker.commons.datatypes.BatchResult;
import eu.neclab.ngsildbroker.commons.datatypes.RestResponse;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.entityhandler.services.EntityService;
import eu.neclab.ngsildbroker.entityhandler.validationutil.Validator;

@RestController
@RequestMapping("/ngsi-ld/v1/entityOperations")
public class EntityBatchController {

	@Autowired
	EntityService entityService;

	@Value("${batchoperations.maxnumber.create:-1}")
	int maxCreateBatch;
	@Value("${batchoperations.maxnumber.update:-1}")
	int maxUpdateBatch;
	@Value("${batchoperations.maxnumber.upsert:-1}")
	int maxUpsertBatch;
	@Value("${batchoperations.maxnumber.delete:-1}")
	int maxDeleteBatch;

	private JsonLdOptions opts = new JsonLdOptions(JsonLdOptions.JSON_LD_1_1);
	
	@Value("${ngsild.corecontext:https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld}")
	String coreContext;
	
	@PostConstruct
	public void init() {
		JsonLdProcessor.init(coreContext);
	}

	@PostMapping("/create")
	public ResponseEntity<byte[]> createMultiple(HttpServletRequest request, @RequestBody String payload) {

		List<Map<String, Object>> jsonPayload;
		try {
			jsonPayload = getJsonPayload(payload);
		} catch (Exception e) {
			ResponseException responseException;
			if (e instanceof ResponseException) {
				responseException = (ResponseException) e;
			} else {
				responseException = new ResponseException(ErrorType.BadRequestData, e.getMessage());
			}
			return ResponseEntity.status(responseException.getHttpStatus())
					.body(new RestResponse(responseException).toJsonBytes());
		}
		BatchResult result = new BatchResult();
		if (maxCreateBatch != -1 && jsonPayload.size() > maxCreateBatch) {
			ResponseException responseException = new ResponseException(ErrorType.RequestEntityTooLarge,
					"Maximum allowed number of entities for this operation is " + maxCreateBatch);
			return ResponseEntity.status(responseException.getHttpStatus())
					.body(new RestResponse(responseException).toJsonBytes());
		}
		List<Object> linkHeaders = HttpUtils.getAtContext(request);
		ArrayListMultimap<String, String> headers = HttpUtils.getHeaders(request);
		boolean preFlight;
		try {
			preFlight = HttpUtils.doPreflightCheck(request);
		} catch (ResponseException responseException) {
			return ResponseEntity.status(responseException.getHttpStatus())
					.body(new RestResponse(responseException).toJsonBytes());
		}
		for (Map<String, Object> entry : jsonPayload) {
			Map<String, Object> resolved;
			try {
				resolved = (Map<String, Object>) JsonLdProcessor
						.expand(linkHeaders, entry, opts, AppConstants.ENTITY_CREATE_PAYLOAD, preFlight).get(0);
			} catch (JsonLdError | ResponseException e) {
				RestResponse response;
				if (e instanceof ResponseException) {
					response = new RestResponse((ResponseException) e);
				} else {
					response = new RestResponse(ErrorType.BadRequestData, e.getLocalizedMessage());
				}
				result.addFail(new BatchFailure("FAILED TO PARSE BODY", response));
				continue;
			}
			try {
				result.addSuccess(entityService.createMessage(headers, resolved));
			} catch (Exception e) {
				RestResponse response;
				if (e instanceof ResponseException) {
					response = new RestResponse((ResponseException) e);
				} else {
					response = new RestResponse(ErrorType.InternalError, e.getLocalizedMessage());
				}
				String entityId = "NO ID PROVIDED";
				if (resolved.containsKey(NGSIConstants.JSON_LD_ID)) {
					entityId = (String) resolved.get(NGSIConstants.JSON_LD_ID);
				}
				result.addFail(new BatchFailure(entityId, response));
			}

		}
		return generateBatchResultReply(result, HttpStatus.CREATED);
	}

	private List<Map<String, Object>> getJsonPayload(String payload) throws ResponseException, IOException {
		Object jsonPayload = JsonUtils.fromString(payload);
		if (!(jsonPayload instanceof List)) {
			throw new ResponseException(ErrorType.InvalidRequest, "This interface only supports arrays of entities");
		}
		return (List<Map<String, Object>>) jsonPayload;
	}

	private ResponseEntity<byte[]> generateBatchResultReply(BatchResult result, HttpStatus okStatus) {
		HttpStatus status = HttpStatus.MULTI_STATUS;
		String body = DataSerializer.toJson(result);
		if (result.getFails().isEmpty()) {
			status = okStatus;
			body = null;
		}
		if (result.getSuccess().isEmpty()) {
			status = HttpStatus.BAD_REQUEST;
		}

		if (body == null) {
			return ResponseEntity.status(status).build();
		}
		return ResponseEntity.status(status).body(body.getBytes());
	}

	@PostMapping("/upsert")
	public ResponseEntity<byte[]> upsertMultiple(HttpServletRequest request, @RequestBody String payload,
			@RequestParam(required = false, name = "options") String options) {

		List<Map<String, Object>> jsonPayload;
		try {
			jsonPayload = getJsonPayload(payload);
		} catch (Exception e) {
			ResponseException responseException;
			if (e instanceof ResponseException) {
				responseException = (ResponseException) e;
			} else {
				responseException = new ResponseException(ErrorType.BadRequestData, e.getMessage());
			}
			return ResponseEntity.status(responseException.getHttpStatus())
					.body(new RestResponse(responseException).toJsonBytes());
		}
		BatchResult result = new BatchResult();
		if (maxCreateBatch != -1 && jsonPayload.size() > maxCreateBatch) {
			ResponseException responseException = new ResponseException(ErrorType.RequestEntityTooLarge,
					"Maximum allowed number of entities for this operation is " + maxCreateBatch);
			return ResponseEntity.status(responseException.getHttpStatus())
					.body(new RestResponse(responseException).toJsonBytes());
		}
		List<Object> linkHeaders = HttpUtils.getAtContext(request);
		ArrayListMultimap<String, String> headers = HttpUtils.getHeaders(request);
		boolean preFlight;
		try {
			preFlight = HttpUtils.doPreflightCheck(request);
		} catch (ResponseException responseException) {
			return ResponseEntity.status(responseException.getHttpStatus())
					.body(new RestResponse(responseException).toJsonBytes());
		}
		for (Map<String, Object> entry : jsonPayload) {
			Map<String, Object> resolved;
			try {
				resolved = (Map<String, Object>) JsonLdProcessor
						.expand(linkHeaders, entry, opts, AppConstants.ENTITY_CREATE_PAYLOAD, preFlight).get(0);
			} catch (JsonLdError | ResponseException e) {
				RestResponse response;
				if (e instanceof ResponseException) {
					response = new RestResponse((ResponseException) e);
				} else {
					response = new RestResponse(ErrorType.BadRequestData, e.getLocalizedMessage());
				}
				result.addFail(new BatchFailure("FAILED TO PARSE BODY", response));
				continue;
			}
			try {
				result.addSuccess(entityService.createMessage(headers, resolved));
			} catch (Exception e) {

				RestResponse response;
				String entityId;
				if (entry.containsKey(NGSIConstants.JSON_LD_ID)) {
					entityId = (String) entry.get(NGSIConstants.JSON_LD_ID);
				} else {
					result.addFail(new BatchFailure("NO ID PROVIDED",
							new RestResponse(ErrorType.BadRequestData, "No Entity Id provided")));
					continue;
				}
				if (e instanceof ResponseException) {

					ResponseException responseException = ((ResponseException) e);
					if (responseException.getHttpStatus().equals(HttpStatus.CONFLICT)) {
						try {
							AppendResult updateResult = entityService.appendMessage(headers, entityId, resolved,
									options);
							if (updateResult.getStatus()) {
								result.addSuccess(entityId);
							} else {
								result.addFail(new BatchFailure(entityId, new RestResponse(ErrorType.MultiStatus,
										JsonUtils.toPrettyString(updateResult.getJsonToAppend()) + " was not added")));
							}
						} catch (Exception e1) {
							if (e instanceof ResponseException) {
								response = new RestResponse((ResponseException) e1);
							} else {
								response = new RestResponse(ErrorType.InternalError, e1.getLocalizedMessage());
							}

							result.addFail(new BatchFailure(entityId, response));
						}

					} else {
						response = new RestResponse((ResponseException) e);
						result.addFail(new BatchFailure(entityId, response));
					}

				} else {
					response = new RestResponse(ErrorType.InternalError, e.getLocalizedMessage());
					result.addFail(new BatchFailure(entityId, response));
				}

			}
		}

		if (result.getFails().size() == 0) {// && EntityService.checkEntity == true) {
			return generateBatchResultReply(result, HttpStatus.CREATED);
		} else {
			return generateBatchResultReply(result, HttpStatus.NO_CONTENT);
		}

	}

	@PostMapping("/update")
	public ResponseEntity<byte[]> updateMultiple(HttpServletRequest request, @RequestBody String payload,
			@RequestParam(required = false, name = "options") String options) {

		List<Map<String, Object>> jsonPayload;
		try {
			jsonPayload = getJsonPayload(payload);
		} catch (Exception e) {
			ResponseException responseException;
			if (e instanceof ResponseException) {
				responseException = (ResponseException) e;
			} else {
				responseException = new ResponseException(ErrorType.BadRequestData, e.getMessage());
			}
			return ResponseEntity.status(responseException.getHttpStatus())
					.body(new RestResponse(responseException).toJsonBytes());
		}
		BatchResult result = new BatchResult();
		List<Object> linkHeaders = HttpUtils.getAtContext(request);
		ArrayListMultimap<String, String> headers = HttpUtils.getHeaders(request);
		boolean preFlight;
		try {
			preFlight = HttpUtils.doPreflightCheck(request);
		} catch (ResponseException responseException) {
			return ResponseEntity.status(responseException.getHttpStatus())
					.body(new RestResponse(responseException).toJsonBytes());
		}
		if (maxUpdateBatch != -1 && jsonPayload.size() > maxUpdateBatch) {
			ResponseException responseException = new ResponseException(ErrorType.RequestEntityTooLarge,
					"Maximum allowed number of entities for this operation is " + maxUpdateBatch);
			return ResponseEntity.status(responseException.getHttpStatus())
					.body(new RestResponse(responseException).toJsonBytes());
		}
		for (Map<String, Object> entry : jsonPayload) {
			String entityId = "NOT AVAILABLE";

			if (entry.containsKey(NGSIConstants.JSON_LD_ID)) {
				entityId = (String) entry.get(NGSIConstants.JSON_LD_ID);
			} else {
				result.addFail(new BatchFailure(entityId,
						new RestResponse(ErrorType.BadRequestData, "No Entity Id provided")));
				continue;
			}

			Map<String, Object> resolved;
			try {
				resolved = (Map<String, Object>) JsonLdProcessor
						.expand(linkHeaders, entry, opts, AppConstants.ENTITY_UPDATE_PAYLOAD, preFlight).get(0);
			} catch (JsonLdError | ResponseException e) {
				RestResponse response;
				if (e instanceof ResponseException) {
					response = new RestResponse((ResponseException) e);
				} else {
					response = new RestResponse(ErrorType.BadRequestData, e.getLocalizedMessage());
				}
				result.addFail(new BatchFailure("FAILED TO PARSE BODY", response));
				continue;
			}
			try {
				AppendResult updateResult = entityService.appendMessage(headers, entityId, resolved, options);
				if (updateResult.getStatus()) {
					result.addSuccess(entityId);
				} else {
					result.addFail(new BatchFailure(entityId, new RestResponse(ErrorType.MultiStatus,
							JsonUtils.toPrettyString(updateResult.getJsonToAppend()) + " was not added")));
				}
			} catch (Exception e) {

				RestResponse response;
				if (e instanceof ResponseException) {
					response = new RestResponse((ResponseException) e);
				} else {
					response = new RestResponse(ErrorType.InternalError, e.getLocalizedMessage());
				}

				result.addFail(new BatchFailure(entityId, response));
			}
		}
		return generateBatchResultReply(result, HttpStatus.NO_CONTENT);

	}

	@PostMapping("/delete")
	public ResponseEntity<byte[]> deleteMultiple(HttpServletRequest request, @RequestBody String payload)
			throws ResponseException {
		try {
//			String resolved = httpUtils.expandPayload(request, payload);
			// it's an array of uris which is not json-ld so no expanding here
			BatchResult result = entityService.deleteMultipleMessage(HttpUtils.getHeaders(request), payload);
			return generateBatchResultReply(result, HttpStatus.NO_CONTENT);
		} catch (ResponseException responseException) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(
					new RestResponse(ErrorType.InvalidRequest, "There is an error in the provided json").toJsonBytes());
		}
	}

}
