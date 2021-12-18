package eu.neclab.ngsildbroker.entityhandler.controller;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.server.reactive.ServerHttpRequest;
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

	@SuppressWarnings("unchecked")
	@PostMapping("/create")
	public ResponseEntity<byte[]> createMultiple(ServerHttpRequest request, @RequestBody String payload) {

		List<Map<String, Object>> jsonPayload;
		try {
			jsonPayload = getJsonPayload(payload);
		} catch (Exception exception) {
			return HttpUtils.handleControllerExceptions(exception);
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
			preFlight = HttpUtils.doPreflightCheck(request, linkHeaders);
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

	@SuppressWarnings("unchecked")
	private List<Map<String, Object>> getJsonPayload(String payload)
			throws ResponseException, JsonParseException, IOException {
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

	@SuppressWarnings("unchecked")
	@PostMapping("/upsert")
	public ResponseEntity<byte[]> upsertMultiple(ServerHttpRequest request, @RequestBody String payload,
			@RequestParam(required = false, name = "options") String options) {

		List<Map<String, Object>> jsonPayload;
		try {
			jsonPayload = getJsonPayload(payload);
		} catch (Exception exception) {
			return HttpUtils.handleControllerExceptions(exception);
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
			preFlight = HttpUtils.doPreflightCheck(request, linkHeaders);
		} catch (ResponseException responseException) {
			return ResponseEntity.status(responseException.getHttpStatus())
					.body(new RestResponse(responseException).toJsonBytes());
		}
		boolean insertedOneEntity = false;
		boolean appendedOneEntity = false;
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
				insertedOneEntity = true;
			} catch (Exception e) {

				RestResponse response;
				String entityId;
				if (resolved.containsKey(NGSIConstants.JSON_LD_ID)) {
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
								appendedOneEntity = true;
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

		boolean failedOnce = !result.getFails().isEmpty();
		HttpStatus status;
		if (failedOnce) {
			if (insertedOneEntity || appendedOneEntity) {
				status = HttpStatus.MULTI_STATUS;
			} else {
				status = HttpStatus.BAD_REQUEST;
			}
		} else {
			if (insertedOneEntity && appendedOneEntity) {
				status = HttpStatus.MULTI_STATUS;
			} else if (insertedOneEntity) {
				status = HttpStatus.CREATED;
			} else {
				status = HttpStatus.NO_CONTENT;
			}
		}
		return generateBatchResultReply(result, status);
	}

	@SuppressWarnings("unchecked")
	@PostMapping("/update")
	public ResponseEntity<byte[]> updateMultiple(ServerHttpRequest request, @RequestBody String payload,
			@RequestParam(required = false, name = "options") String options) {

		List<Map<String, Object>> jsonPayload;
		try {
			jsonPayload = getJsonPayload(payload);
		} catch (Exception exception) {
			return HttpUtils.handleControllerExceptions(exception);
		}
		BatchResult result = new BatchResult();
		List<Object> linkHeaders = HttpUtils.getAtContext(request);
		ArrayListMultimap<String, String> headers = HttpUtils.getHeaders(request);
		boolean preFlight;
		try {
			preFlight = HttpUtils.doPreflightCheck(request, linkHeaders);
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
		for (Map<String, Object> compactedEntry : jsonPayload) {
			String entityId = "NOT AVAILABLE";
			Map<String, Object> entry;
			try {
				entry = (Map<String, Object>) JsonLdProcessor
						.expand(linkHeaders, compactedEntry, opts, AppConstants.ENTITY_UPDATE_PAYLOAD, preFlight)
						.get(0);
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
			if (entry.containsKey(NGSIConstants.JSON_LD_ID)) {
				entityId = (String) entry.get(NGSIConstants.JSON_LD_ID);
			} else {
				result.addFail(new BatchFailure(entityId,
						new RestResponse(ErrorType.BadRequestData, "No Entity Id provided")));
				continue;
			}
			try {
				AppendResult updateResult = entityService.appendMessage(headers, entityId, entry, options);
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

	@SuppressWarnings("unchecked")
	@PostMapping("/delete")
	public ResponseEntity<byte[]> deleteMultiple(ServerHttpRequest request, @RequestBody String payload) {
		List<Object> jsonPayload;
		boolean atContextAllowed;
		List<Object> links = HttpUtils.getAtContext(request);
		try {

			Object obj = JsonUtils.fromString(payload);
			if (!(obj instanceof List)) {
				return HttpUtils.handleControllerExceptions(new ResponseException(ErrorType.InvalidRequest,
						"This interface only supports arrays of entities"));
			}
			jsonPayload = (List<Object>) obj;
			atContextAllowed = HttpUtils.doPreflightCheck(request, links);
		} catch (Exception exception) {
			return HttpUtils.handleControllerExceptions(exception);
		}

		ArrayListMultimap<String, String> headers = HttpUtils.getHeaders(request);
		BatchResult result = new BatchResult();
		for (Object entry : jsonPayload) {
			String entityId = "NO ENTITY ID FOUND";
			try {
				if (entry instanceof String) {
					entityId = (String) entry;
				} else {
					List<Object> resolved = JsonLdProcessor.expand(links, entry, opts,
							AppConstants.ENTITY_CREATE_PAYLOAD, atContextAllowed);
					entityId = (String) ((Map<String, Object>) resolved.get(0)).get(NGSIConstants.JSON_LD_ID);
				}
				if (entityService.deleteEntity(headers, entityId)) {
					result.addSuccess(entityId);
				} else {
					result.addFail(new BatchFailure(entityId, new RestResponse(ErrorType.InternalError, "")));
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

		// BatchResult result =
		// entityService.deleteMultipleMessage(HttpUtils.getHeaders(request), resolved);
		return generateBatchResultReply(result, HttpStatus.NO_CONTENT);

	}

}
