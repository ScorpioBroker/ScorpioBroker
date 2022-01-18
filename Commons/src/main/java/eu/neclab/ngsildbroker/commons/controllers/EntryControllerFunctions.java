package eu.neclab.ngsildbroker.commons.controllers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import com.github.jsonldjava.core.JsonLdConsts;
import com.github.jsonldjava.core.JsonLdError;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.RestResponse;
import eu.neclab.ngsildbroker.commons.datatypes.results.BatchFailure;
import eu.neclab.ngsildbroker.commons.datatypes.results.BatchResult;
import eu.neclab.ngsildbroker.commons.datatypes.results.UpdateResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.EntryCRUDService;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;

public interface EntryControllerFunctions {
	static JsonLdOptions opts = new JsonLdOptions(JsonLdOptions.JSON_LD_1_1);

	@SuppressWarnings("unchecked")
	public static ResponseEntity<String> updateMultiple(EntryCRUDService entityService, HttpServletRequest request,
			String payload, int maxUpdateBatch, String options, int payloadType) {
		String[] optionsArray = getOptionsArray(options);
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
					.body(new RestResponse(responseException).toJson());
		}
		if (maxUpdateBatch != -1 && jsonPayload.size() > maxUpdateBatch) {
			ResponseException responseException = new ResponseException(ErrorType.RequestEntityTooLarge,
					"Maximum allowed number of entities for this operation is " + maxUpdateBatch);
			return ResponseEntity.status(responseException.getHttpStatus())
					.body(new RestResponse(responseException).toJson());
		}
		for (Map<String, Object> compactedEntry : jsonPayload) {
			String entityId = "NOT AVAILABLE";
			Map<String, Object> entry;
			try {
				entry = (Map<String, Object>) JsonLdProcessor
						.expand(linkHeaders, compactedEntry, opts, payloadType, preFlight).get(0);
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
				//need to be check
//				AppendResult updateResult = entityService.appendToEntry(headers, entityId, entry, optionsArray);
//				if (updateResult.getStatus()) {
//					result.addSuccess(entityId);
//				} else {
//					result.addFail(new BatchFailure(entityId, new RestResponse(ErrorType.MultiStatus,
//							JsonUtils.toPrettyString(updateResult.getJsonToAppend()) + " was not added")));
//				}
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
	public static ResponseEntity<String> createMultiple(EntryCRUDService entityService, HttpServletRequest request,
			String payload, int maxCreateBatch, int payloadType) {

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
					.body(new RestResponse(responseException).toJson());
		}
		List<Object> linkHeaders = HttpUtils.getAtContext(request);
		ArrayListMultimap<String, String> headers = HttpUtils.getHeaders(request);
		boolean preFlight;
		try {
			preFlight = HttpUtils.doPreflightCheck(request, linkHeaders);
		} catch (ResponseException responseException) {
			return ResponseEntity.status(responseException.getHttpStatus())
					.body(new RestResponse(responseException).toJson());
		}
		for (Map<String, Object> entry : jsonPayload) {
			Map<String, Object> resolved;
			try {
				resolved = (Map<String, Object>) JsonLdProcessor
						.expand(linkHeaders, entry, opts, payloadType, preFlight).get(0);
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
				result.addSuccess(entityService.createEntry(headers, resolved));
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
	private static List<Map<String, Object>> getJsonPayload(String payload) throws ResponseException, IOException {
		Object jsonPayload = JsonUtils.fromString(payload);
		if (!(jsonPayload instanceof List)) {
			throw new ResponseException(ErrorType.InvalidRequest, "This interface only supports arrays of entities");
		}
		return (List<Map<String, Object>>) jsonPayload;
	}

	private static ResponseEntity<String> generateBatchResultReply(BatchResult result, HttpStatus okStatus) {
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
		return ResponseEntity.status(status).body(body);
	}

	@SuppressWarnings("unchecked")
	public static ResponseEntity<String> deleteMultiple(EntryCRUDService entityService, HttpServletRequest request,
			String payload, int payloadType) {
		List<Object> jsonPayload;
		boolean atContextAllowed;
		List<Object> links = HttpUtils.getAtContext(request);
		try {

			Object obj = JsonUtils.fromString(payload);
			if (!(obj instanceof List)) {
				return HttpUtils.handleControllerExceptions(new ResponseException(ErrorType.BadRequestData,
						"This interface only supports arrays of entities"));
			}
			jsonPayload = (List<Object>) obj;
			atContextAllowed = HttpUtils.doPreflightCheck(request, links);
		} catch (Exception exception) {
			return HttpUtils.handleControllerExceptions(exception);
		}
		if (jsonPayload.isEmpty()) {
			return HttpUtils.handleControllerExceptions(
					new ResponseException(ErrorType.BadRequestData, "An empty array is not allowed"));
		}
		ArrayListMultimap<String, String> headers = HttpUtils.getHeaders(request);
		BatchResult result = new BatchResult();
		for (Object entry : jsonPayload) {
			String entityId = "NO ENTITY ID FOUND";
			try {
				if (entry instanceof String) {
					entityId = (String) entry;
				} else {
					List<Object> resolved = JsonLdProcessor.expand(links, entry, opts, payloadType, atContextAllowed);
					entityId = (String) ((Map<String, Object>) resolved.get(0)).get(NGSIConstants.JSON_LD_ID);
				}
				if (entityService.deleteEntry(headers, entityId)) {
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
		return generateBatchResultReply(result, HttpStatus.NO_CONTENT);
	}

	@SuppressWarnings("unchecked")
	public static ResponseEntity<String> upsertMultiple(EntryCRUDService entityService, HttpServletRequest request,
			String payload, String options, int maxCreateBatch, int payloadType) {
		String[] optionsArray = getOptionsArray(options);
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
					.body(new RestResponse(responseException).toJson());
		}
		List<Object> linkHeaders = HttpUtils.getAtContext(request);
		ArrayListMultimap<String, String> headers = HttpUtils.getHeaders(request);
		boolean preFlight;
		try {
			preFlight = HttpUtils.doPreflightCheck(request, linkHeaders);
		} catch (ResponseException responseException) {
			return ResponseEntity.status(responseException.getHttpStatus())
					.body(new RestResponse(responseException).toJson());
		}
		boolean insertedOneEntity = false;
		boolean appendedOneEntity = false;
		for (Map<String, Object> entry : jsonPayload) {
			Map<String, Object> resolved;
			try {
				resolved = (Map<String, Object>) JsonLdProcessor
						.expand(linkHeaders, entry, opts, payloadType, preFlight).get(0);
			} catch (JsonLdError | ResponseException e) {
				RestResponse response;
				if (e instanceof ResponseException) {
					response = new RestResponse((ResponseException) e);
				} else {
					response = new RestResponse(ErrorType.BadRequestData, e.getLocalizedMessage());
				}
				String entityId = "No entity ID found";
				if (entry.containsKey(NGSIConstants.QUERY_PARAMETER_ID)) {
					entityId = (String) entry.get(NGSIConstants.QUERY_PARAMETER_ID);
				} else if (entry.containsKey(NGSIConstants.JSON_LD_ID)) {
					entityId = (String) entry.get(NGSIConstants.JSON_LD_ID);
				}
				result.addFail(new BatchFailure(entityId, response));
				continue;
			}
			try {
				result.addSuccess(entityService.createEntry(headers, resolved));
				insertedOneEntity = true;
			} catch (Exception e) {

				RestResponse response;
				String entityId;
				if (resolved.containsKey(NGSIConstants.JSON_LD_ID)) {
					entityId = (String) resolved.get(NGSIConstants.JSON_LD_ID);
				} else {
					result.addFail(new BatchFailure("NO ID PROVIDED",
							new RestResponse(ErrorType.BadRequestData, "No Entity Id provided")));
					continue;
				}
				if (e instanceof ResponseException) {

					ResponseException responseException = ((ResponseException) e);
					if (responseException.getHttpStatus().equals(HttpStatus.CONFLICT)) {
						try {
							//need to be check
//							AppendResult updateResult = entityService.appendToEntry(headers, entityId, resolved,
//									optionsArray);
//							if (updateResult.getStatus()) {
//								result.addSuccess(entityId);
//								appendedOneEntity = true;
//							} else {
//								result.addFail(new BatchFailure(entityId, new RestResponse(ErrorType.MultiStatus,
//										JsonUtils.toPrettyString(updateResult.getJsonToAppend()) + " was not added")));
//							}
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
		// for 1.5.1 update no more bad request on fail only multi status
		if (failedOnce) {
			if (insertedOneEntity || appendedOneEntity) {
				status = HttpStatus.MULTI_STATUS;
			} else {
				status = HttpStatus.BAD_REQUEST;
			}
		} else {
			if (insertedOneEntity && appendedOneEntity) {
				status = HttpStatus.MULTI_STATUS;
			} else {
				if (insertedOneEntity) {
					status = HttpStatus.CREATED;
				} else {
					status = HttpStatus.NO_CONTENT;
				}
			}
		}
		return generateBatchResultReply(result, status);
	}

	@SuppressWarnings("unchecked")
	public static ResponseEntity<String> updateEntry(EntryCRUDService entityService, HttpServletRequest request,
			String entityId, String payload, int payloadType, Logger logger) {
		try {
			logger.trace("update entry :: started");
			HttpUtils.validateUri(entityId);
			List<Object> contextHeaders = HttpUtils.getAtContext(request);
			boolean atContextAllowed = HttpUtils.doPreflightCheck(request, contextHeaders);
			List<Object> context = new ArrayList<Object>();
			context.addAll(contextHeaders);
			Map<String, Object> body = ((Map<String, Object>) JsonUtils.fromString(payload));
			Object bodyContext = body.get(JsonLdConsts.CONTEXT);
			Map<String, Object> resolved = (Map<String, Object>) JsonLdProcessor
					.expand(contextHeaders, body, opts, payloadType, atContextAllowed).get(0);
			if (bodyContext instanceof List) {
				context.addAll((List<Object>) bodyContext);
			} else {
				context.add(bodyContext);
			}

			UpdateResult update = entityService.updateEntry(HttpUtils.getHeaders(request), entityId, resolved);
			logger.trace("update entry :: completed");
			return HttpUtils.generateReply(request, update, context, AppConstants.UPDATE_REQUEST);
		} catch (Exception exception) {
			return HttpUtils.handleControllerExceptions(exception);
		}
	}

	public static ResponseEntity<String> createEntry(EntryCRUDService entityService, HttpServletRequest request,
			String payload, int payloadType, String baseUrl, Logger logger) {
		String result = null;
		try {
			logger.trace("create entity :: started");
			List<Object> contextHeaders = HttpUtils.getAtContext(request);
			boolean atContextAllowed = HttpUtils.doPreflightCheck(request, contextHeaders);
			if (payload == null || payload.isEmpty()) {
				return HttpUtils.handleControllerExceptions(
						new ResponseException(ErrorType.InvalidRequest, "You have to provide a valid payload"));
			}
			@SuppressWarnings("unchecked")
			Map<String, Object> resolved = (Map<String, Object>) JsonLdProcessor
					.expand(contextHeaders, JsonUtils.fromString(payload), opts, payloadType, atContextAllowed).get(0);
			result = entityService.createEntry(HttpUtils.getHeaders(request), resolved);
			logger.trace("create entity :: completed");
			return ResponseEntity.status(HttpStatus.CREATED).header("location", baseUrl + result).build();
		} catch (Exception exception) {
			return HttpUtils.handleControllerExceptions(exception);
		}
	}
	
	public static ResponseEntity<String> appendToEntry(EntryCRUDService entityService, HttpServletRequest request,
			String entityId, String payload, String options, int payloadType, Logger logger) {
		try {
			logger.trace("append entity :: started");
			String[] optionsArray = getOptionsArray(options);
			List<Object> contextHeaders = HttpUtils.getAtContext(request);			
			boolean atContextAllowed = HttpUtils.doPreflightCheck(request, contextHeaders);
			List<Object> context = new ArrayList<Object>();
			context.addAll(contextHeaders);
			Map<String, Object> body = ((Map<String, Object>) JsonUtils.fromString(payload));
			Object bodyContext = body.get(JsonLdConsts.CONTEXT);
			
			if (bodyContext instanceof List) {
				context.addAll((List<Object>) bodyContext);
			} else {
				context.add(bodyContext);
			}
			if (payload == null || payload.isEmpty()) {
				throw new ResponseException(ErrorType.InvalidRequest, "An empty payload is not allowed");
			}
			HttpUtils.validateUri(entityId);
			@SuppressWarnings("unchecked")
			Map<String, Object> resolved = (Map<String, Object>) JsonLdProcessor
					.expand(contextHeaders, JsonUtils.fromString(payload), opts, payloadType, atContextAllowed).get(0);
			UpdateResult update = entityService.appendToEntry(HttpUtils.getHeaders(request), entityId, resolved,
					optionsArray);
			logger.trace("append entity :: completed");
			
			logger.trace("update entry :: completed");
			return HttpUtils.generateReply(request, update, context, AppConstants.UPDATE_REQUEST);
//			if (append.getAppendResult()) {
//				return ResponseEntity.noContent().build();
//			} else {
//				return ResponseEntity.status(HttpStatus.MULTI_STATUS)
//						.body(JsonUtils.toPrettyString(append.getAppendedJsonFields()));
//			}
		} catch (Exception exception) {
			return HttpUtils.handleControllerExceptions(exception);
		}
	}

	private static String[] getOptionsArray(String options) {
		if (options == null) {
			return new String[0];
		} else {
			return options.split(",");
		}
	}

	public static ResponseEntity<String> deleteEntry(EntryCRUDService entityService, HttpServletRequest request,
			String entityId, Logger logger) {
		try {
			logger.trace("delete entity :: started");
			HttpUtils.validateUri(entityId);
			entityService.deleteEntry(HttpUtils.getHeaders(request), entityId);
			logger.trace("delete entity :: completed");
			return ResponseEntity.noContent().build();
		} catch (Exception exception) {
			return HttpUtils.handleControllerExceptions(exception);
		}
	}

}
