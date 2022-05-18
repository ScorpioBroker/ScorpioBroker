package eu.neclab.ngsildbroker.commons.controllers;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.http.HttpStatus;
import org.jboss.resteasy.reactive.RestResponse;
import org.jboss.resteasy.reactive.server.jaxrs.RestResponseBuilderImpl;
import org.slf4j.Logger;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParseException;
import com.github.jsonldjava.core.JsonLdConsts;
import com.github.jsonldjava.core.JsonLdError;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.ArrayListMultimap;

import com.google.common.collect.Lists;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.NGSIRestResponse;
import eu.neclab.ngsildbroker.commons.datatypes.results.BatchFailure;
import eu.neclab.ngsildbroker.commons.datatypes.results.BatchResult;
import eu.neclab.ngsildbroker.commons.datatypes.results.UpdateResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.EntryCRUDService;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.mutiny.tuples.Tuple3;
import io.smallrye.mutiny.unchecked.Unchecked;
import io.vertx.core.http.HttpServerRequest;

public interface EntryControllerFunctions {
	static JsonLdOptions opts = new JsonLdOptions(JsonLdOptions.JSON_LD_1_1);

	@SuppressWarnings("unchecked")
	public static Uni<RestResponse<Object>> updateMultiple(EntryCRUDService entityService, HttpServerRequest request,
			String payload, int maxUpdateBatch, String options, int payloadType) {
		List<Map<String, Object>> jsonPayload;
		try {
			jsonPayload = getJsonPayload(payload);
		} catch (Exception exception) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(exception));
		}
		if (maxUpdateBatch != -1 && jsonPayload.size() > maxUpdateBatch) {
			ResponseException responseException = new ResponseException(ErrorType.RequestEntityTooLarge,
					"Maximum allowed number of entities for this operation is " + maxUpdateBatch);
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(responseException));
		}

		return HttpUtils.getAtContext(request).onItem().transformToMulti(t -> {
			boolean preFlight;

			try {
				preFlight = HttpUtils.doPreflightCheck(request, t);
			} catch (ResponseException e) {
				return Multi.createFrom().failure(e);
			}
			Multi<Tuple3<List<Object>, Map<String, Object>, Boolean>> tmpResult = Multi.createFrom()
					.items(jsonPayload.parallelStream()).onItem().transform(i -> Tuple3.of(t, i, preFlight));
			return tmpResult;
		}).onItem().transform(Unchecked.function(t -> {
			return (Map<String, Object>) JsonLdProcessor
					.expand(t.getItem1(), t.getItem2(), opts, payloadType, t.getItem3()).get(0);
		})).onItem().transformToUni(t -> {
			ArrayListMultimap<String, String> headers = HttpUtils.getHeaders(request);
			entityService.appendToEntry(headers, entityId, entry, optionsArray);
			return entityService.createEntry(headers, t).onItem()
					.transform(i -> Tuple2.of(new BatchFailure("FAILED TO PARSE BODY", null), i)).onFailure()
					.recoverWithItem(e -> {
						NGSIRestResponse response;
						if (e instanceof ResponseException) {
							response = new NGSIRestResponse((ResponseException) e);
						} else {
							response = new NGSIRestResponse(ErrorType.InternalError, e.getLocalizedMessage());
						}
						String entityId = "NO ID PROVIDED";
						if (t.containsKey(NGSIConstants.JSON_LD_ID)) {
							entityId = (String) t.get(NGSIConstants.JSON_LD_ID);
						}
						return Tuple2.of(new BatchFailure(entityId, response), null);
					});
		}).collectFailures().concatenate().collect().asList().onItem().transform(t -> {
			BatchResult result = new BatchResult();
			t.forEach(i -> {
				if (i.getItem2() != null) {
					result.addSuccess(i.getItem2());
				} else {
					result.addFail(i.getItem1());
				}
			});
			return generateBatchResultReply(result, HttpStatus.SC_CREATED);
		});

		List<Map<String, Object>> jsonPayload;
		String[] optionsArray = getOptionsArray(options);
		try {
			jsonPayload = getJsonPayload(payload);
		} catch (Exception exception) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(exception));
		}
		BatchResult result = new BatchResult();
		List<Object> linkHeaders = HttpUtils.getAtContext(request);
		ArrayListMultimap<String, String> headers = HttpUtils.getHeaders(request);
		boolean preFlight;
		try {
			preFlight = HttpUtils.doPreflightCheck(request, linkHeaders);
		} catch (ResponseException responseException) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(responseException));
		}
		if (maxUpdateBatch != -1 && jsonPayload.size() > maxUpdateBatch) {
			ResponseException responseException = new ResponseException(ErrorType.RequestEntityTooLarge,
					"Maximum allowed number of entities for this operation is " + maxUpdateBatch);
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(responseException));
		}
		for (Map<String, Object> compactedEntry : jsonPayload) {
			String entityId = "NOT AVAILABLE";
			Map<String, Object> entry;
			try {
				entry = (Map<String, Object>) JsonLdProcessor
						.expand(linkHeaders, compactedEntry, opts, payloadType, preFlight).get(0);
			} catch (JsonLdError | ResponseException e) {
				eu.neclab.ngsildbroker.commons.datatypes.NGSIRestResponse response;
				if (e instanceof ResponseException) {
					response = new eu.neclab.ngsildbroker.commons.datatypes.NGSIRestResponse((ResponseException) e);
				} else {
					response = new eu.neclab.ngsildbroker.commons.datatypes.NGSIRestResponse(ErrorType.BadRequestData,
							e.getLocalizedMessage());
				}
				result.addFail(new BatchFailure("FAILED TO PARSE BODY", response));
				continue;
			}
			if (entry.containsKey(NGSIConstants.JSON_LD_ID)) {
				entityId = (String) entry.get(NGSIConstants.JSON_LD_ID);
			} else {
				result.addFail(new BatchFailure(entityId, new eu.neclab.ngsildbroker.commons.datatypes.NGSIRestResponse(
						ErrorType.BadRequestData, "No Entity Id provided")));
				continue;
			}
			try {

				UpdateResult updateResult = null;// entityService.appendToEntry(headers, entityId, entry, optionsArray);
				if (updateResult.getNotUpdated().isEmpty()) {
					result.addSuccess(entityId);
				} else {
					result.addFail(new BatchFailure(entityId,
							new eu.neclab.ngsildbroker.commons.datatypes.NGSIRestResponse(ErrorType.MultiStatus,
									JsonUtils.toPrettyString(updateResult.getNotUpdated()) + " was not added")));
				}
			} catch (Exception e) {

				eu.neclab.ngsildbroker.commons.datatypes.NGSIRestResponse response;
				if (e instanceof ResponseException) {
					response = new eu.neclab.ngsildbroker.commons.datatypes.NGSIRestResponse((ResponseException) e);
				} else {
					response = new eu.neclab.ngsildbroker.commons.datatypes.NGSIRestResponse(ErrorType.InternalError,
							e.getLocalizedMessage());
				}

				result.addFail(new BatchFailure(entityId, response));
			}
		}
		return generateBatchResultReply(result, HttpStatus.SC_NO_CONTENT);
	}

	@SuppressWarnings("unchecked")
	public static Uni<RestResponse<Object>> createMultiple(EntryCRUDService entityService, HttpServerRequest request,
			String payload, int maxCreateBatch, int payloadType) {
		List<Map<String, Object>> jsonPayload;
		try {
			jsonPayload = getJsonPayload(payload);
		} catch (Exception exception) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(exception));
		}
		if (maxCreateBatch != -1 && jsonPayload.size() > maxCreateBatch) {
			ResponseException responseException = new ResponseException(ErrorType.RequestEntityTooLarge,
					"Maximum allowed number of entities for this operation is " + maxCreateBatch);
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(responseException));
		}
		return HttpUtils.getAtContext(request).onItem().transformToMulti(t -> {
			boolean preFlight;

			try {
				preFlight = HttpUtils.doPreflightCheck(request, t);
			} catch (ResponseException e) {
				return Multi.createFrom().failure(e);
			}
			Multi<Tuple3<List<Object>, Map<String, Object>, Boolean>> tmpResult = Multi.createFrom()
					.items(jsonPayload.parallelStream()).onItem().transform(i -> Tuple3.of(t, i, preFlight));
			return tmpResult;
		}).onItem().transform(Unchecked.function(t -> {
			return (Map<String, Object>) JsonLdProcessor
					.expand(t.getItem1(), t.getItem2(), opts, payloadType, t.getItem3()).get(0);
		})).onItem().transformToUni(t -> {
			ArrayListMultimap<String, String> headers = HttpUtils.getHeaders(request);
			return entityService.createEntry(headers, t).onItem()
					.transform(i -> Tuple2.of(new BatchFailure("dummy", null), i)).onFailure().recoverWithItem(e -> {
						NGSIRestResponse response;
						if (e instanceof ResponseException) {
							response = new NGSIRestResponse((ResponseException) e);
						} else {
							response = new NGSIRestResponse(ErrorType.InternalError, e.getLocalizedMessage());
						}
						String entityId = "NO ID PROVIDED";
						if (t.containsKey(NGSIConstants.JSON_LD_ID)) {
							entityId = (String) t.get(NGSIConstants.JSON_LD_ID);
						}
						return Tuple2.of(new BatchFailure(entityId, response), null);
					});
		}).collectFailures().concatenate().collect().asList().onItem().transform(t -> {
			BatchResult result = new BatchResult();
			t.forEach(i -> {
				if (i.getItem2() != null) {
					result.addSuccess(i.getItem2());
				} else {
					result.addFail(i.getItem1());
				}
			});
			return generateBatchResultReply(result, HttpStatus.SC_CREATED);
		});
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
			RestResponse.status(status);
		}
		return RestResponseBuilderImpl.create(status).entity(body).build();
	}

	@SuppressWarnings("unchecked")
	public static Uni<RestResponse<Object>> deleteMultiple(EntryCRUDService entityService, HttpServerRequest request,
			String payload, int payloadType) {

		return HttpUtils.getAtContext(request).onItem().transformToMulti(t -> {
			Object obj = JsonUtils.fromString(payload);
			if (!(obj instanceof List)) {
				return Multi.createFrom().failure(new ResponseException(ErrorType.BadRequestData,
						"This interface only supports arrays of entities"));

			}
			List<Object> jsonPayload = (List<Object>) obj;
			boolean atContextAllowed = HttpUtils.doPreflightCheck(request, t);
			if (jsonPayload.isEmpty()) {
				return Multi.createFrom()
						.failure(new ResponseException(ErrorType.BadRequestData, "An empty array is not allowed"));

			}
			ArrayListMultimap<String, String> headers = HttpUtils.getHeaders(request);
			return Multi.createFrom().items(jsonPayload.parallelStream()).onItem().transform(t2 -> {
				String entityId = "NO ENTITY ID FOUND";
				if (t2 instanceof String) {
					entityId = (String) t2;
				} else {
					List<Object> resolved = JsonLdProcessor.expand(t, t2, opts, payloadType, atContextAllowed);
					entityId = (String) ((Map<String, Object>) resolved.get(0)).get(NGSIConstants.JSON_LD_ID);
				}

				return Tuple2.of(entityId, headers);
			});
		}).onItem().transformToUni(t -> entityService.deleteEntry(t.getItem2(), t.getItem1()).onItem().transform(i -> {
			if (i) {
				return Tuple2.of(new BatchFailure("dummy", null), t.getItem1());
			} else {
				return Tuple2.of(new BatchFailure(t.getItem1(), new NGSIRestResponse(ErrorType.InternalError, "")),
						null);
			}
		}).onFailure().recoverWithItem(e -> {
			NGSIRestResponse response;
			if (e instanceof ResponseException) {
				response = new NGSIRestResponse((ResponseException) e);
			} else {
				response = new NGSIRestResponse(ErrorType.InternalError, e.getLocalizedMessage());
			}

			return Tuple2.of(new BatchFailure(t.getItem1(), response), null);
		})).collectFailures().concatenate().collect().asList().onItem().transform(t -> {
			BatchResult result = new BatchResult();
			t.forEach(i -> {
				if (i.getItem2() != null) {
					result.addSuccess((String) i.getItem2());
				} else {
					result.addFail(i.getItem1());
				}
			});
			return generateBatchResultReply(result, HttpStatus.SC_CREATED);
		});
	}

	@SuppressWarnings("unchecked")
	public static Uni<RestResponse<Object>> upsertMultiple(EntryCRUDService entityService, HttpServerRequest request,
			String payload, String options, int maxCreateBatch, int payloadType) {
		String[] optionsArray = getOptionsArray(options);
		List<Map<String, Object>> jsonPayload;
		try {
			jsonPayload = getJsonPayload(payload);
		} catch (Exception exception) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(exception));
		}
		BatchResult result = new BatchResult();
		if (maxCreateBatch != -1 && jsonPayload.size() > maxCreateBatch) {
			ResponseException responseException = new ResponseException(ErrorType.RequestEntityTooLarge,
					"Maximum allowed number of entities for this operation is " + maxCreateBatch);
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(responseException));
		}
		List<Object> linkHeaders = HttpUtils.getAtContext(request);
		ArrayListMultimap<String, String> headers = HttpUtils.getHeaders(request);
		boolean preFlight;
		try {
			preFlight = HttpUtils.doPreflightCheck(request, linkHeaders);
		} catch (ResponseException responseException) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(responseException));
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
				eu.neclab.ngsildbroker.commons.datatypes.NGSIRestResponse response;
				if (e instanceof ResponseException) {
					response = new eu.neclab.ngsildbroker.commons.datatypes.NGSIRestResponse((ResponseException) e);
				} else {
					response = new eu.neclab.ngsildbroker.commons.datatypes.NGSIRestResponse(ErrorType.BadRequestData,
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
//				result.addSuccess(entityService.createEntry(headers, resolved));
				insertedOneEntity = true;
			} catch (Exception e) {
				String entityId;
				if (resolved.containsKey(NGSIConstants.JSON_LD_ID)) {
					entityId = (String) resolved.get(NGSIConstants.JSON_LD_ID);
				} else {
					result.addFail(new BatchFailure("NO ID PROVIDED",
							new eu.neclab.ngsildbroker.commons.datatypes.NGSIRestResponse(ErrorType.BadRequestData,
									"No Entity Id provided")));
					continue;
				}
				eu.neclab.ngsildbroker.commons.datatypes.NGSIRestResponse response;
				if (e instanceof ResponseException) {
					ResponseException responseException = ((ResponseException) e);
					if (responseException.getHttpStatus().code() == HttpStatus.SC_CONFLICT) {
						if (replace) {
							try {
								entityService.deleteEntry(headers, (String) resolved.get(NGSIConstants.JSON_LD_ID));
								// result.addSuccess(entityService.createEntry(headers, resolved));
								insertedOneEntity = true;
							} catch (Exception e1) {
								if (e1 instanceof ResponseException) {
									response = new eu.neclab.ngsildbroker.commons.datatypes.NGSIRestResponse(
											(ResponseException) e1);
								} else {
									response = new eu.neclab.ngsildbroker.commons.datatypes.NGSIRestResponse(
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
											new eu.neclab.ngsildbroker.commons.datatypes.NGSIRestResponse(
													ErrorType.MultiStatus,
													JsonUtils.toPrettyString(updateResult.getNotUpdated())
															+ " was not added")));
								}
							} catch (Exception e1) {
								if (e1 instanceof ResponseException) {
									response = new eu.neclab.ngsildbroker.commons.datatypes.NGSIRestResponse(
											(ResponseException) e1);
								} else {
									response = new eu.neclab.ngsildbroker.commons.datatypes.NGSIRestResponse(
											ErrorType.InternalError, e1.getLocalizedMessage());
								}

								result.addFail(new BatchFailure(entityId, response));
							}
						}
					} else {
						response = new eu.neclab.ngsildbroker.commons.datatypes.NGSIRestResponse((ResponseException) e);
						result.addFail(new BatchFailure(entityId, response));
					}

				} else {
					response = new eu.neclab.ngsildbroker.commons.datatypes.NGSIRestResponse(ErrorType.InternalError,
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
	public static Uni<RestResponse<Object>> updateEntry(EntryCRUDService entityService, HttpServerRequest request,
			String entityId, String payload, int payloadType, Logger logger) {
		logger.trace("update entry :: started");
		return Uni.combine().all().unis(HttpUtils.validateUri(entityId), HttpUtils.getAtContext(request)).asTuple()
				.onItem().transformToUni(t -> {
					List<Object> contextHeaders = t.getItem2();
					boolean atContextAllowed;
					try {
						atContextAllowed = HttpUtils.doPreflightCheck(request, contextHeaders);
					} catch (ResponseException e) {
						return Uni.createFrom().failure(e);
					}
					List<Object> context = new ArrayList<Object>();
					context.addAll(contextHeaders);
					Object bodyContext;
					Map<String, Object> body;
					try {
						body = ((Map<String, Object>) JsonUtils.fromString(payload));
					} catch (IOException e) {
						return Uni.createFrom().failure(e);
					}
					bodyContext = body.get(JsonLdConsts.CONTEXT);
					Map<String, Object> resolvedBody;
					try {
						resolvedBody = (Map<String, Object>) JsonLdProcessor
								.expand(contextHeaders, body, opts, payloadType, atContextAllowed).get(0);
					} catch (JsonLdError | ResponseException e) {
						return Uni.createFrom().failure(e);
					}

					if (bodyContext instanceof List) {
						context.addAll((List<Object>) bodyContext);
					} else {
						context.add(bodyContext);
					}
					return Uni.createFrom().item(Tuple2.of(resolvedBody, context));
				}).onItem()
				.transformToUni(resolved -> entityService
						.updateEntry(HttpUtils.getHeaders(request), entityId, resolved.getItem1()).onItem()
						.transformToUni(updateResult -> {
							logger.trace("update entry :: completed");
							return HttpUtils.generateReply(request, updateResult, resolved.getItem2(),
									AppConstants.UPDATE_REQUEST);
						}))
				.onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);
	}

	@SuppressWarnings("unchecked")
	public static Uni<RestResponse<Object>> createEntry(EntryCRUDService entityService, HttpServerRequest request,
			String payload, int payloadType, String baseUrl, Logger logger) {
		logger.trace("create entity :: started");
		return HttpUtils.getAtContext(request).onItem().transformToUni(t -> {
			boolean atContextAllowed;
			try {
				atContextAllowed = HttpUtils.doPreflightCheck(request, t);
			} catch (ResponseException e1) {
				return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e1));
			}
			if (payload == null || payload.isEmpty()) {
				return Uni.createFrom().item(HttpUtils.handleControllerExceptions(
						new ResponseException(ErrorType.InvalidRequest, "You have to provide a valid payload")));
			}
			Map<String, Object> resolved;
			try {
				resolved = (Map<String, Object>) JsonLdProcessor
						.expand(t, JsonUtils.fromString(payload), opts, payloadType, atContextAllowed).get(0);
			} catch (Exception e) {
				return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
			}
			return entityService.createEntry(HttpUtils.getHeaders(request), resolved).onItem().transform(entityId -> {
				logger.trace("create entity :: completed");
				try {
					return RestResponse.created(new URI(baseUrl + t));
				} catch (URISyntaxException e) {
					return HttpUtils.handleControllerExceptions(e);
				}
			});
		}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);

	}

	@SuppressWarnings("unchecked")
	public static Uni<RestResponse<Object>> appendToEntry(EntryCRUDService entityService, HttpServerRequest request,
			String entityId, String payload, String options, int payloadType, Logger logger) {
		return HttpUtils.getAtContext(request).onItem().transformToUni(t -> {
			logger.trace("append entity :: started");
			String[] optionsArray = getOptionsArray(options);
			// try {
			boolean atContextAllowed;
			try {
				atContextAllowed = HttpUtils.doPreflightCheck(request, t);
			} catch (ResponseException e1) {
				return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e1));
			}
			List<Object> context = new ArrayList<Object>();
			context.addAll(t);
			if (payload == null || payload.isEmpty()) {
				return Uni.createFrom().item(HttpUtils.handleControllerExceptions(
						new ResponseException(ErrorType.InvalidRequest, "An empty payload is not allowed")));
			}
			Map<String, Object> body;
			try {
				body = ((Map<String, Object>) JsonUtils.fromString(payload));
			} catch (Exception e) {
				return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
			}
			Object bodyContext = body.get(JsonLdConsts.CONTEXT);

			if (bodyContext instanceof List) {
				context.addAll((List<Object>) bodyContext);
			} else {
				context.add(bodyContext);
			}
			Map<String, Object> resolved;
			try {
				HttpUtils.validateUri(entityId);
				resolved = (Map<String, Object>) JsonLdProcessor
						.expand(t, JsonUtils.fromString(payload), opts, payloadType, atContextAllowed).get(0);
			} catch (Exception e) {
				return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
			}
			return entityService.appendToEntry(HttpUtils.getHeaders(request), entityId, resolved, optionsArray).onItem()
					.transformToUni(tResult -> {
						return HttpUtils.generateReply(request, tResult, context, AppConstants.UPDATE_REQUEST);
					});

		}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);
	}

	private static String[] getOptionsArray(String options) {
		if (options == null) {
			return new String[0];
		} else {
			return options.split(",");
		}
	}

	public static Uni<RestResponse<Object>> deleteEntry(EntryCRUDService entityService, HttpServerRequest request,
			String entityId, Logger logger) {
		return HttpUtils.validateUri(entityId).onItem().transformToUni(t -> {
			return entityService.deleteEntry(HttpUtils.getHeaders(request), entityId).onItem().transform(t2 -> {
				logger.trace("delete entity :: completed");
				return RestResponse.noContent();
			});
		}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);
	}

}
