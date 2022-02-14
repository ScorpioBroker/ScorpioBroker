package eu.neclab.ngsildbroker.commons.controllers;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.http.HttpStatus;
import org.jboss.resteasy.reactive.RestResponse;
import org.jboss.resteasy.reactive.server.jaxrs.RestResponseBuilderImpl;
import org.slf4j.Logger;

import com.github.jsonldjava.core.JsonLdConsts;
import com.github.jsonldjava.core.JsonLdError;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.results.BatchFailure;
import eu.neclab.ngsildbroker.commons.datatypes.results.BatchResult;
import eu.neclab.ngsildbroker.commons.datatypes.results.UpdateResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.EntryCRUDService;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import io.vertx.core.http.HttpServerRequest;

public interface EntryControllerFunctions {
	static JsonLdOptions opts = new JsonLdOptions(JsonLdOptions.JSON_LD_1_1);

	@SuppressWarnings("unchecked")
	public static RestResponse<Object> updateMultiple(EntryCRUDService entityService, HttpServerRequest request,
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
			return HttpUtils.handleControllerExceptions(responseException);
		}
		if (maxUpdateBatch != -1 && jsonPayload.size() > maxUpdateBatch) {
			ResponseException responseException = new ResponseException(ErrorType.RequestEntityTooLarge,
					"Maximum allowed number of entities for this operation is " + maxUpdateBatch);
			return HttpUtils.handleControllerExceptions(responseException);
		}
		for (Map<String, Object> compactedEntry : jsonPayload) {
			String entityId = "NOT AVAILABLE";
			Map<String, Object> entry;
			try {
				entry = (Map<String, Object>) JsonLdProcessor
						.expand(linkHeaders, compactedEntry, opts, payloadType, preFlight).get(0);
			} catch (JsonLdError | ResponseException e) {
				eu.neclab.ngsildbroker.commons.datatypes.RestResponse response;
				if (e instanceof ResponseException) {
					response = new eu.neclab.ngsildbroker.commons.datatypes.RestResponse((ResponseException) e);
				} else {
					response = new eu.neclab.ngsildbroker.commons.datatypes.RestResponse(ErrorType.BadRequestData,
							e.getLocalizedMessage());
				}
				result.addFail(new BatchFailure("FAILED TO PARSE BODY", response));
				continue;
			}
			if (entry.containsKey(NGSIConstants.JSON_LD_ID)) {
				entityId = (String) entry.get(NGSIConstants.JSON_LD_ID);
			} else {
				result.addFail(new BatchFailure(entityId, new eu.neclab.ngsildbroker.commons.datatypes.RestResponse(
						ErrorType.BadRequestData, "No Entity Id provided")));
				continue;
			}
			try {

				UpdateResult updateResult = entityService.appendToEntry(headers, entityId, entry, optionsArray);
				if (updateResult.getNotUpdated().isEmpty()) {
					result.addSuccess(entityId);
				} else {
					result.addFail(new BatchFailure(entityId,
							new eu.neclab.ngsildbroker.commons.datatypes.RestResponse(ErrorType.MultiStatus,
									JsonUtils.toPrettyString(updateResult.getNotUpdated()) + " was not added")));
				}
			} catch (Exception e) {

				eu.neclab.ngsildbroker.commons.datatypes.RestResponse response;
				if (e instanceof ResponseException) {
					response = new eu.neclab.ngsildbroker.commons.datatypes.RestResponse((ResponseException) e);
				} else {
					response = new eu.neclab.ngsildbroker.commons.datatypes.RestResponse(ErrorType.InternalError,
							e.getLocalizedMessage());
				}

				result.addFail(new BatchFailure(entityId, response));
			}
		}
		return generateBatchResultReply(result, HttpStatus.SC_NO_CONTENT);
	}

	@SuppressWarnings("unchecked")
	public static RestResponse<Object> createMultiple(EntryCRUDService entityService, HttpServerRequest request,
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
			return HttpUtils.handleControllerExceptions(responseException);
		}
		List<Object> linkHeaders = HttpUtils.getAtContext(request);
		ArrayListMultimap<String, String> headers = HttpUtils.getHeaders(request);
		boolean preFlight;
		try {
			preFlight = HttpUtils.doPreflightCheck(request, linkHeaders);
		} catch (ResponseException responseException) {
			return HttpUtils.handleControllerExceptions(responseException);
		}
		for (Map<String, Object> entry : jsonPayload) {
			Map<String, Object> resolved;
			try {
				resolved = (Map<String, Object>) JsonLdProcessor
						.expand(linkHeaders, entry, opts, payloadType, preFlight).get(0);
			} catch (JsonLdError | ResponseException e) {
				eu.neclab.ngsildbroker.commons.datatypes.RestResponse response;
				if (e instanceof ResponseException) {
					response = new eu.neclab.ngsildbroker.commons.datatypes.RestResponse((ResponseException) e);
				} else {
					response = new eu.neclab.ngsildbroker.commons.datatypes.RestResponse(ErrorType.BadRequestData,
							e.getLocalizedMessage());
				}
				result.addFail(new BatchFailure("FAILED TO PARSE BODY", response));
				continue;
			}
			try {
				result.addSuccess(entityService.createEntry(headers, resolved));
			} catch (Exception e) {
				eu.neclab.ngsildbroker.commons.datatypes.RestResponse response;
				if (e instanceof ResponseException) {
					response = new eu.neclab.ngsildbroker.commons.datatypes.RestResponse((ResponseException) e);
				} else {
					response = new eu.neclab.ngsildbroker.commons.datatypes.RestResponse(ErrorType.InternalError,
							e.getLocalizedMessage());
				}
				String entityId = "NO ID PROVIDED";
				if (resolved.containsKey(NGSIConstants.JSON_LD_ID)) {
					entityId = (String) resolved.get(NGSIConstants.JSON_LD_ID);
				}
				result.addFail(new BatchFailure(entityId, response));
			}

		}
		return generateBatchResultReply(result, HttpStatus.SC_CREATED);
	}

	@SuppressWarnings("unchecked")
	private static List<Map<String, Object>> getJsonPayload(String payload) throws ResponseException, IOException {
		Object jsonPayload = JsonUtils.fromString(payload);
		if (!(jsonPayload instanceof List)) {
			throw new ResponseException(ErrorType.InvalidRequest, "This interface only supports arrays of entities");
		}
		return (List<Map<String, Object>>) jsonPayload;
	}

	private static RestResponse<Object> generateBatchResultReply(BatchResult result, int okStatus) {
		int status = HttpStatus.SC_MULTI_STATUS;
		String body = DataSerializer.toJson(result);
		if (result.getFails().isEmpty()) {
			status = okStatus;
			body = null;
		}
		if (result.getSuccess().isEmpty()) {
			status = HttpStatus.SC_BAD_REQUEST;
		}
		if (body == null) {
			return RestResponse.status(status);
		}
		return RestResponseBuilderImpl.create(status).entity(body).build();
	}

	@SuppressWarnings("unchecked")
	public static RestResponse<Object> deleteMultiple(EntryCRUDService entityService, HttpServerRequest request,
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
					result.addFail(new BatchFailure(entityId,
							new eu.neclab.ngsildbroker.commons.datatypes.RestResponse(ErrorType.InternalError, "")));
				}
			} catch (Exception e) {
				eu.neclab.ngsildbroker.commons.datatypes.RestResponse response;
				if (e instanceof ResponseException) {
					response = new eu.neclab.ngsildbroker.commons.datatypes.RestResponse((ResponseException) e);
				} else {
					response = new eu.neclab.ngsildbroker.commons.datatypes.RestResponse(ErrorType.InternalError,
							e.getLocalizedMessage());
				}
				result.addFail(new BatchFailure(entityId, response));
			}
		}
		return generateBatchResultReply(result, HttpStatus.SC_NO_CONTENT);
	}

	@SuppressWarnings("unchecked")
	public static RestResponse<Object> upsertMultiple(EntryCRUDService entityService, HttpServerRequest request,
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
			return HttpUtils.handleControllerExceptions(responseException);
		}
		List<Object> linkHeaders = HttpUtils.getAtContext(request);
		ArrayListMultimap<String, String> headers = HttpUtils.getHeaders(request);
		boolean preFlight;
		try {
			preFlight = HttpUtils.doPreflightCheck(request, linkHeaders);
		} catch (ResponseException responseException) {
			return HttpUtils.handleControllerExceptions(responseException);
		}
		boolean insertedOneEntity = false;
		boolean appendedOneEntity = false;
		boolean replace = true;
		if (ArrayUtils.contains(optionsArray, NGSIConstants.UPDATE_OPTION)) {
			replace = false;
		}
		for (Map<String, Object> entry : jsonPayload) {
			Map<String, Object> resolved;
			try {
				resolved = (Map<String, Object>) JsonLdProcessor
						.expand(linkHeaders, entry, opts, payloadType, preFlight).get(0);
			} catch (JsonLdError | ResponseException e) {
				eu.neclab.ngsildbroker.commons.datatypes.RestResponse response;
				if (e instanceof ResponseException) {
					response = new eu.neclab.ngsildbroker.commons.datatypes.RestResponse((ResponseException) e);
				} else {
					response = new eu.neclab.ngsildbroker.commons.datatypes.RestResponse(ErrorType.BadRequestData,
							e.getLocalizedMessage());
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
				String entityId;
				if (resolved.containsKey(NGSIConstants.JSON_LD_ID)) {
					entityId = (String) resolved.get(NGSIConstants.JSON_LD_ID);
				} else {
					result.addFail(new BatchFailure("NO ID PROVIDED",
							new eu.neclab.ngsildbroker.commons.datatypes.RestResponse(ErrorType.BadRequestData,
									"No Entity Id provided")));
					continue;
				}
				eu.neclab.ngsildbroker.commons.datatypes.RestResponse response;
				if (e instanceof ResponseException) {
					ResponseException responseException = ((ResponseException) e);
					if (responseException.getHttpStatus().code() == HttpStatus.SC_CONFLICT) {
						if (replace) {
							try {
								entityService.deleteEntry(headers, (String) resolved.get(NGSIConstants.JSON_LD_ID));
								result.addSuccess(entityService.createEntry(headers, resolved));
								insertedOneEntity = true;
							} catch (Exception e1) {
								if (e1 instanceof ResponseException) {
									response = new eu.neclab.ngsildbroker.commons.datatypes.RestResponse(
											(ResponseException) e1);
								} else {
									response = new eu.neclab.ngsildbroker.commons.datatypes.RestResponse(
											ErrorType.InternalError, e1.getLocalizedMessage());
								}
								result.addFail(new BatchFailure(entityId, response));// TODO Auto-generated catch block
							}
						} else {

							try {
								UpdateResult updateResult = entityService.appendToEntry(headers, entityId, resolved,
										new String[0]);
								if (updateResult.getNotUpdated().isEmpty()) {
									result.addSuccess(entityId);
									appendedOneEntity = true;
								} else {
									result.addFail(new BatchFailure(entityId,
											new eu.neclab.ngsildbroker.commons.datatypes.RestResponse(
													ErrorType.MultiStatus,
													JsonUtils.toPrettyString(updateResult.getNotUpdated())
															+ " was not added")));
								}
							} catch (Exception e1) {
								if (e1 instanceof ResponseException) {
									response = new eu.neclab.ngsildbroker.commons.datatypes.RestResponse(
											(ResponseException) e1);
								} else {
									response = new eu.neclab.ngsildbroker.commons.datatypes.RestResponse(
											ErrorType.InternalError, e1.getLocalizedMessage());
								}

								result.addFail(new BatchFailure(entityId, response));
							}
						}
					} else {
						response = new eu.neclab.ngsildbroker.commons.datatypes.RestResponse((ResponseException) e);
						result.addFail(new BatchFailure(entityId, response));
					}

				} else {
					response = new eu.neclab.ngsildbroker.commons.datatypes.RestResponse(ErrorType.InternalError,
							e.getLocalizedMessage());
					result.addFail(new BatchFailure(entityId, response));
				}
			}
		}

		boolean failedOnce = !result.getFails().isEmpty();
		int status;
		// for 1.5.1 update no more bad request on fail only multi status
		if (failedOnce) {
			if (insertedOneEntity || appendedOneEntity) {
				status = HttpStatus.SC_MULTI_STATUS;
			} else {
				status = HttpStatus.SC_BAD_REQUEST;
			}
		} else {
			if (insertedOneEntity && appendedOneEntity) {
				status = HttpStatus.SC_MULTI_STATUS;
			} else {
				if (insertedOneEntity) {
					status = HttpStatus.SC_CREATED;
				} else {
					status = HttpStatus.SC_NO_CONTENT;
				}
			}
		}
		return generateBatchResultReply(result, status);
	}

	@SuppressWarnings("unchecked")
	public static org.jboss.resteasy.reactive.RestResponse<Object> updateEntry(EntryCRUDService entityService,
			HttpServerRequest request, String entityId, String payload, int payloadType, Logger logger) {
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

	public static RestResponse<Object> createEntry(EntryCRUDService entityService, HttpServerRequest request,
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
			return RestResponse.created(new URI(baseUrl + result));
		} catch (Exception exception) {
			return HttpUtils.handleControllerExceptions(exception);
		}
	}

	@SuppressWarnings("unchecked")
	public static org.jboss.resteasy.reactive.RestResponse<Object> appendToEntry(EntryCRUDService entityService,
			HttpServerRequest request, String entityId, String payload, String options, int payloadType,
			Logger logger) {
		try {
			logger.trace("append entity :: started");
			String[] optionsArray = getOptionsArray(options);
			List<Object> contextHeaders = HttpUtils.getAtContext(request);
			boolean atContextAllowed = HttpUtils.doPreflightCheck(request, contextHeaders);
			List<Object> context = new ArrayList<Object>();
			context.addAll(contextHeaders);
			if (payload == null || payload.isEmpty()) {
				throw new ResponseException(ErrorType.InvalidRequest, "An empty payload is not allowed");
			}
			Map<String, Object> body = ((Map<String, Object>) JsonUtils.fromString(payload));
			Object bodyContext = body.get(JsonLdConsts.CONTEXT);

			if (bodyContext instanceof List) {
				context.addAll((List<Object>) bodyContext);
			} else {
				context.add(bodyContext);
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

	public static RestResponse<Object> deleteEntry(EntryCRUDService entityService, HttpServerRequest request,
			String entityId, Logger logger) {
		try {
			logger.trace("delete entity :: started");
			HttpUtils.validateUri(entityId);
			entityService.deleteEntry(HttpUtils.getHeaders(request), entityId);
			logger.trace("delete entity :: completed");
			return RestResponse.noContent();
		} catch (Exception exception) {
			return HttpUtils.handleControllerExceptions(exception);
		}
	}

}
