package eu.neclab.ngsildbroker.commons.controllers;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.github.jsonldjava.core.JsonLdConsts;
import com.github.jsonldjava.core.JsonLdError;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.ArrayListMultimap;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.http.HttpStatus;
import org.jboss.resteasy.reactive.RestResponse;
import org.jboss.resteasy.reactive.server.jaxrs.RestResponseBuilderImpl;
import org.slf4j.Logger;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.NGSIRestResponse;
import eu.neclab.ngsildbroker.commons.datatypes.results.BatchFailure;
import eu.neclab.ngsildbroker.commons.datatypes.results.BatchResult;
import eu.neclab.ngsildbroker.commons.datatypes.results.CreateResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.EntryCRUDService;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.mutiny.tuples.Tuple3;
import io.smallrye.mutiny.tuples.Tuple4;
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

		return HttpUtils.getAtContext(request).onItem().transformToMulti(Unchecked.function(t -> {
			boolean preFlight = HttpUtils.doPreflightCheck(request, t);
			return Multi.createFrom().items(jsonPayload.parallelStream()).onItem()
					.transform(i -> Tuple3.of(preFlight, t, i));
		})).onItem().transformToUni(tupleItem -> {
			String entityId;
			if (tupleItem.getItem3().containsKey(NGSIConstants.JSON_LD_ID)) {
				entityId = (String) tupleItem.getItem3().get(NGSIConstants.JSON_LD_ID);
			} else if (tupleItem.getItem3().containsKey(NGSIConstants.QUERY_PARAMETER_ID)) {
				entityId = (String) tupleItem.getItem3().get(NGSIConstants.QUERY_PARAMETER_ID);
			} else {
				return Uni.createFrom().item(Tuple2.of(
						new BatchFailure("NO ID PROVIDED",
								new NGSIRestResponse(
										new ResponseException(ErrorType.BadRequestData, "No Entity Id provided"))),
						null));
			}
			Map<String, Object> entry;
			try {
				entry = (Map<String, Object>) JsonLdProcessor.expand((List<Object>) tupleItem.getItem2(),
						tupleItem.getItem3(), opts, payloadType, tupleItem.getItem1()).get(0);
			} catch (Exception e1) {
				NGSIRestResponse response;
				if (e1 instanceof ResponseException) {
					response = new NGSIRestResponse((ResponseException) e1);
				} else {
					response = new NGSIRestResponse(ErrorType.InvalidRequest,
							"failed to expand payload because: " + e1.getCause().getLocalizedMessage());
				}
				return Uni.createFrom().item(Tuple2.of(new BatchFailure(entityId, response), null));
			}

			return entityService.appendToEntry(HttpUtils.getHeaders(request), entityId, entry, getOptionsArray(options))
					.onItem().transform(Unchecked.function(i -> {
						if (i.getNotUpdated().isEmpty()) {
							return Tuple2.of(new BatchFailure("", null), entityId);
						} else {
							return Tuple2.of(
									new BatchFailure(entityId,
											new NGSIRestResponse(ErrorType.MultiStatus,
													JsonUtils.toPrettyString(i.getNotUpdated()) + " was not added")),
									null);
						}
					}));
		}).collectFailures().concatenate().collect().asList().onItem().transform(t -> {
			BatchResult result = new BatchResult();
			t.forEach(i -> {
				if (i.getItem2() != null) {
					result.addSuccess((String) i.getItem2());
				} else {
					result.addFail(i.getItem1());
				}
			});
			return generateBatchResultReply(result, HttpStatus.SC_NO_CONTENT);
		}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);

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
		}).onItem().transformToUni(itemTuple -> {
			Map<String, Object> expanded;
			String entityId;
			if (itemTuple.getItem2().containsKey(NGSIConstants.JSON_LD_ID)) {
				entityId = (String) itemTuple.getItem2().get(NGSIConstants.JSON_LD_ID);
			} else if (itemTuple.getItem2().containsKey(NGSIConstants.QUERY_PARAMETER_ID)) {
				entityId = (String) itemTuple.getItem2().get(NGSIConstants.QUERY_PARAMETER_ID);
			} else {
				entityId = "NO ID PROVIDED";
			}
			try {
				expanded = (Map<String, Object>) JsonLdProcessor
						.expand(itemTuple.getItem1(), itemTuple.getItem2(), opts, payloadType, itemTuple.getItem3())
						.get(0);
			} catch (Exception e1) {
				NGSIRestResponse response;
				if (e1 instanceof ResponseException) {
					response = new NGSIRestResponse((ResponseException) e1);
				} else {
					response = new NGSIRestResponse(ErrorType.InvalidRequest,
							"failed to expand payload because: " + e1.getCause().getLocalizedMessage());
				}
				return Uni.createFrom().item(Tuple2.of(new BatchFailure(entityId, response), null));
			}
			ArrayListMultimap<String, String> headers = HttpUtils.getHeaders(request);
			return entityService.createEntry(headers, expanded).onItem()
					.transform(i -> Tuple2.of(new BatchFailure("dummy", null), i)).onFailure().recoverWithItem(e -> {
						NGSIRestResponse response;
						if (e instanceof ResponseException) {
							response = new NGSIRestResponse((ResponseException) e);
						} else {
							response = new NGSIRestResponse(ErrorType.InternalError, e.getLocalizedMessage());
						}

						return Tuple2.of(new BatchFailure(entityId, response), null);
					});
		}).collectFailures().concatenate().collect().asList().onItemOrFailure().transform((results, fails) -> {
			if (results == null) {
				return HttpUtils.handleControllerExceptions(fails);
			}
			BatchResult result = new BatchResult();
			results.forEach(i -> {
				if (i.getItem2() != null) {
					result.addSuccess(((CreateResult) i.getItem2()).getEntityId());
				} else {
					result.addFail(i.getItem1());
				}
			});
			System.out.println(fails);
			return generateBatchResultReply(result, HttpStatus.SC_CREATED);
		});

//				t -> {
//			BatchResult result = new BatchResult();
//			t.forEach(i -> {
//				if (i.getItem2() != null) {
//					result.addSuccess(i.getItem2().getEntityId());
//				} else {
//					result.addFail(i.getItem1());
//				}
//			});
//			return generateBatchResultReply(result, HttpStatus.SC_CREATED);
//		});
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
		String body = result.toJsonString();
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

		try {
			getJsonPayload(payload);
		} catch (Exception exception) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(exception));
		}
		return HttpUtils.getAtContext(request).onItem().transformToMulti(Unchecked.function(t -> {
			Object obj = JsonUtils.fromString(payload);
			if (!(obj instanceof List)) {
				return Multi.createFrom().failure(new ResponseException(ErrorType.BadRequestData,
						"This interface only supports arrays of entities"));

			}
			List<Object> jsonPayload = (List<Object>) obj;
			boolean atContextAllowed;
			try {
				atContextAllowed = HttpUtils.doPreflightCheck(request, t);
			} catch (ResponseException e) {
				return Multi.createFrom().failure(e);
			}
			if (jsonPayload.isEmpty()) {
				return Multi.createFrom()
						.failure(new ResponseException(ErrorType.BadRequestData, "An empty array is not allowed"));

			}

			ArrayListMultimap<String, String> headers = HttpUtils.getHeaders(request);
			return Multi.createFrom().items(jsonPayload.parallelStream()).onItem().transform(Unchecked.function(t2 -> {
				String entityId = "NO ENTITY ID FOUND";
				if (t2 instanceof String) {
					entityId = (String) t2;
				} else {
					List<Object> resolved = JsonLdProcessor.expand(t, t2, opts, payloadType, atContextAllowed);
					entityId = (String) ((Map<String, Object>) resolved.get(0)).get(NGSIConstants.JSON_LD_ID);
				}

				return Tuple2.of(entityId, headers);
			}));
		})).onItem().transformToUni(t -> entityService.deleteEntry(t.getItem2(), t.getItem1()).onItem().transform(i -> {
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
			return generateBatchResultReply(result, HttpStatus.SC_NO_CONTENT);
		}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);
	}

	@SuppressWarnings("unchecked")
	public static Uni<RestResponse<Object>> upsertMultiple(EntryCRUDService entityService, HttpServerRequest request,
			String payload, String options, int maxCreateBatch, int payloadType) {
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
		boolean replace;

		String[] optionsArray = getOptionsArray(options);
		if (ArrayUtils.contains(optionsArray, NGSIConstants.UPDATE_OPTION)) {
			replace = false;
		} else {
			replace = true;
		}
		return HttpUtils.getAtContext(request).onItem().transformToMulti(Unchecked.function(t -> {
			boolean preFlight = HttpUtils.doPreflightCheck(request, t);
			return Multi.createFrom().items(jsonPayload.parallelStream()).onItem()
					.transform(i -> Tuple3.of(preFlight, t, i));
		})).onItem().transformToUni(Unchecked.function(tt -> {
			String entityIdTmp;
			if (tt.getItem3().containsKey(NGSIConstants.JSON_LD_ID)) {
				entityIdTmp = (String) tt.getItem3().get(NGSIConstants.JSON_LD_ID);
			} else if (tt.getItem3().containsKey(NGSIConstants.QUERY_PARAMETER_ID)) {
				entityIdTmp = (String) tt.getItem3().get(NGSIConstants.QUERY_PARAMETER_ID);
			} else {
				entityIdTmp = "NO ID PROVIDED";
			}
			Map<String, Object> expanded = null;
			try {
				expanded = (Map<String, Object>) JsonLdProcessor
						.expand((List<Object>) tt.getItem2(), tt.getItem3(), opts, payloadType, tt.getItem1()).get(0);
			} catch (Exception e) {
				NGSIRestResponse response;
				if (e instanceof ResponseException) {
					response = new NGSIRestResponse((ResponseException) e);
				} else {
					response = new NGSIRestResponse(ErrorType.InternalError, e.getLocalizedMessage());
				}
				return Uni.createFrom().item(Tuple3.of(new BatchFailure(entityIdTmp, response), null, false));
			}
			Tuple4<Map<String, Object>, Object, ArrayListMultimap<String, String>, String[]> t = Tuple4.of(expanded,
					tt.getItem2(), HttpUtils.getHeaders(request), getOptionsArray(options));

			Map<String, Object> entry = expanded;// t.getItem1();
			String entityId;
			if (entry.containsKey(NGSIConstants.JSON_LD_ID)) {
				entityId = (String) entry.get(NGSIConstants.JSON_LD_ID);
			} else {
				return Uni.createFrom()
						.failure(new ResponseException(ErrorType.BadRequestData, "No Entity Id provided"));
			}
			if (replace) {
				return entityService.deleteEntry(t.getItem3(), entityId).onFailure().recoverWithUni(e -> {
					NGSIRestResponse response;
					if (e instanceof ResponseException) {
						response = new NGSIRestResponse((ResponseException) e);
						if (response.getStatus().equals(HttpResponseStatus.NOT_FOUND)) {
							return Uni.createFrom().item(false);
						}
					}
					return Uni.createFrom().failure(e);
				}).onItem().transformToUni(t2 -> {
					return entityService.createEntry(t.getItem3(), t.getItem1()).onItem().transform(i -> {
						boolean inserted;
						if (t2) {
							inserted = false;
						} else {
							inserted = true;
						}
						return Tuple3.of(new BatchFailure("", null), entityId, inserted);
					});
				}).onFailure().recoverWithItem(e -> {
					NGSIRestResponse response;
					if (e instanceof ResponseException) {
						response = new NGSIRestResponse((ResponseException) e);
					} else {
						response = new NGSIRestResponse(ErrorType.InternalError, e.getLocalizedMessage());
					}
					return Tuple3.of(new BatchFailure(entityId, response), null, false);
				});
			} else {
				return entityService.appendToEntry(t.getItem3(), entityId, entry, t.getItem4()).onItem()
						.transform(Unchecked.function(i -> {
							if (i.getNotUpdated().isEmpty()) {
								return Tuple3.of(new BatchFailure("", null), entityId, false);
							} else {
								return Tuple3.of(
										new BatchFailure(entityId, new NGSIRestResponse(ErrorType.MultiStatus,
												JsonUtils.toPrettyString(i.getNotUpdated()) + " was not added")),
										null, false);
							}
						})).onFailure().recoverWithUni(e -> {
							NGSIRestResponse response;
							if (e instanceof ResponseException) {
								response = new NGSIRestResponse((ResponseException) e);
								if (response.getStatus().equals(HttpResponseStatus.NOT_FOUND)) {
									return entityService.createEntry(t.getItem3(), entry).onItem()
											.transform(i -> Tuple3.of(new BatchFailure("", null), entityId, true))
											.onFailure().recoverWithItem(e1 -> {
												NGSIRestResponse response1;
												if (e1 instanceof ResponseException) {
													response1 = new NGSIRestResponse((ResponseException) e1);
												} else {
													response1 = new NGSIRestResponse(ErrorType.InternalError,
															e1.getLocalizedMessage());
												}
												return Tuple3.of(new BatchFailure(entityId, response1), null, false);
											});
								}
							} else {
								response = new NGSIRestResponse(ErrorType.InternalError, e.getLocalizedMessage());
							}
							return Uni.createFrom().item(Tuple3.of(new BatchFailure(entityId, response), null, false));
						});
			}
		})).collectFailures().concatenate().collect().asList().onItem().transform(t -> {
			BatchResult result = new BatchResult();
			boolean insertedOneEntity = false;
			boolean appendedOneEntity = false;
			for (Tuple3<BatchFailure, ? extends Object, Boolean> i : t) {
				if (i.getItem2() != null) {
					if (i.getItem3()) {
						insertedOneEntity = true;
					} else {
						appendedOneEntity = true;
					}
					result.addSuccess((String) i.getItem2());
				} else {
					result.addFail(i.getItem1());
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
		});
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
			return entityService.createEntry(HttpUtils.getHeaders(request), resolved).onItem()
					.transform(createResult -> {
						logger.trace("create entity :: completed");
						String entityId = createResult.getEntityId();
						try {
							if (createResult.isCreatedOrUpdated()) {
								return RestResponse.created(new URI(baseUrl + entityId));
							} else {
								return RestResponse.noContent();
							}
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
