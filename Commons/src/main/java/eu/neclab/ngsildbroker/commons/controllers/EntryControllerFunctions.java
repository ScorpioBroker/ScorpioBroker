package eu.neclab.ngsildbroker.commons.controllers;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.github.jsonldjava.core.JsonLdConsts;
import com.github.jsonldjava.core.JsonLdError;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.net.HttpHeaders;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.http.HttpStatus;
import org.jboss.resteasy.reactive.RestResponse;
import org.jboss.resteasy.reactive.server.jaxrs.RestResponseBuilderImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.BatchInfo;
import eu.neclab.ngsildbroker.commons.datatypes.results.BatchResult;
import eu.neclab.ngsildbroker.commons.datatypes.results.CRUDSuccess;
import eu.neclab.ngsildbroker.commons.datatypes.results.NGSILDOperationResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.EntryCRUDService;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.mutiny.tuples.Tuple3;
import io.smallrye.mutiny.tuples.Tuple4;
import io.vertx.core.http.HttpServerRequest;

public interface EntryControllerFunctions {
	static JsonLdOptions opts = new JsonLdOptions(JsonLdOptions.JSON_LD_1_1);

	static Logger logger = LoggerFactory.getLogger(EntryControllerFunctions.class);

	@SuppressWarnings("unchecked")
	public static Uni<RestResponse<Object>> updateMultiple(EntryCRUDService entityService, HttpServerRequest request,
			String payload, int maxUpdateBatch, String options, int payloadType, Random random) {
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
		BatchInfo batchInfo = new BatchInfo(random.nextInt(), jsonPayload.size());

		return HttpUtils.getAtContext(request).onItem().transformToMulti(t -> {
			boolean preFlight;
			try {
				preFlight = HttpUtils.doPreflightCheck(request, t);
			} catch (ResponseException e) {
				return Multi.createFrom().failure(e);
			}
			return Multi.createFrom().items(jsonPayload.parallelStream()).onItem()
					.transform(i -> Tuple3.of(preFlight, t, i));
		}).onItem().transformToUni(tupleItem -> {
			String entityId;
			if (tupleItem.getItem3().containsKey(NGSIConstants.JSON_LD_ID)) {
				entityId = (String) tupleItem.getItem3().get(NGSIConstants.JSON_LD_ID);
			} else if (tupleItem.getItem3().containsKey(NGSIConstants.QUERY_PARAMETER_ID)) {
				entityId = (String) tupleItem.getItem3().get(NGSIConstants.QUERY_PARAMETER_ID);
			} else {
				return entityService.sendFail(batchInfo).onItem()
						.transformToUni(
								t -> Uni.createFrom()
										.item(Tuple2.of(
												new BatchFailure("NO ID PROVIDED",
														new NGSIRestResponse(new ResponseException(
																ErrorType.BadRequestData, "No Entity Id provided"))),
												null)));

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
				return entityService.sendFail(batchInfo).onItem().transformToUni(
						t -> Uni.createFrom().item(Tuple2.of(new BatchFailure(entityId, response), null)));
			}

			return entityService
					.appendToEntry(HttpUtils.getHeaders(request), entityId, entry, getOptionsArray(options), batchInfo)
					.onFailure().recoverWithItem(t -> new UpdateResult()).onItem().transformToUni(i -> {
						if (i.getNotUpdated().isEmpty()) {
							return Uni.createFrom().item(Tuple2.of(new BatchFailure("", null), entityId));
						} else {
							return entityService.sendFail(batchInfo).onItem().transformToUni(t -> {
								try {
									return Uni.createFrom().item(Tuple2.of(
											new BatchFailure(entityId, new NGSIRestResponse(ErrorType.MultiStatus,
													JsonUtils.toPrettyString(i.getNotUpdated()) + " was not added")),
											null));
								} catch (Exception e) {
									return Uni.createFrom().item(Tuple2.of(new BatchFailure(entityId,
											new NGSIRestResponse(ErrorType.MultiStatus, e.getLocalizedMessage())),
											null));
								}
							});
						}
					});
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
			String payload, int maxCreateBatch, int payloadType, Random random) {
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
		BatchInfo batchInfo = new BatchInfo(random.nextInt(), jsonPayload.size());
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
			try {
				expanded = (Map<String, Object>) JsonLdProcessor
						.expand(itemTuple.getItem1(), itemTuple.getItem2(), opts, payloadType, itemTuple.getItem3())
						.get(0);
			} catch (Exception e) {
				return entityService.sendFail(batchInfo).onItem().transformToUni(t -> Uni.createFrom().failure(e));
			}
			ArrayListMultimap<String, String> headers = HttpUtils.getHeaders(request);
			List<Object> context = itemTuple.getItem1();
			Object bodyContext = itemTuple.getItem2().get(JsonLdConsts.CONTEXT);
			if (bodyContext instanceof List) {
				context.addAll((List<Object>) bodyContext);
			} else {
				context.add(bodyContext);
			}
			return entityService.createEntry(headers, expanded, context, batchInfo).onFailure().recoverWithUni(e -> {
				return entityService.sendFail(batchInfo).onItem().transformToUni(v -> Uni.createFrom().failure(e));
			});
		}).collectFailures().concatenate().collect().asList().onItemOrFailure().transform((results, fails) -> {
			
			if (results == null) {
				return HttpUtils.handleControllerExceptions(fails);
			}			
			
			return generateBatchResultReply(results, HttpStatus.SC_CREATED);
			

//			List<NGSILDOperationResult> result = Lists.newArrayList();
//			if(fails != null) {
//				
//				CompositeException exceptions = (CompositeException) fails;
//				exceptions.getCauses();	
//			}
//			
//			if (results == null) {
//				return HttpUtils.handleControllerExceptions(fails);
//			}
//			results.forEach(i -> {
//				if (i.getSuccesses()!=null && !i.getSuccesses().isEmpty() ) {
//					result.addSuccess(((CreateResult) i.getItem2()).getEntityId());
//				} else {
//					result.addFail(i.getItem1());
//				}
//			});
//			return generateBatchResultReply(result, HttpStatus.SC_CREATED);
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

	private static RestResponse<Object> generateBatchResultReply(List<NGSILDOperationResult> results, int okStatus) {
		List<ResponseException> resFailures = new ArrayList<>();
		List<String> resSuccesses = new ArrayList<>();

		int httpStatus = okStatus;

		for (NGSILDOperationResult i : results) {
			List<ResponseException> failures = i.getFailures();
			List<CRUDSuccess> successes = i.getSuccesses();
			if (failures.isEmpty()) {
				resSuccesses.add(i.getEntityId());
			} else if (successes.isEmpty() && failures.size() == 1) {
				failures.addAll(i.getFailures());
				httpStatus = HttpStatus.SC_MULTI_STATUS;
			} else {
				failures.addAll(i.getFailures());
				httpStatus = HttpStatus.SC_MULTI_STATUS;
			}
		}

		String body = null;

		try {
			if (httpStatus == okStatus) {
				body = JsonUtils.toPrettyString(resSuccesses);
			} else {
				Map<String, Object> resMap = Maps.newHashMap();
				resMap.put("success", resSuccesses);
				resMap.put("errors", resFailures);
				body = JsonUtils.toPrettyString(resMap);
			}
		} catch (IOException e) {
			logger.error("Failed to generate reply body for batch operation.", e);
		}

		return RestResponseBuilderImpl.create(httpStatus).entity(body)
				.header(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON).build();
	}

	@SuppressWarnings("unchecked")
	public static Uni<RestResponse<Object>> deleteMultiple(EntryCRUDService entityService, HttpServerRequest request,
			String payload, int payloadType, Random random) {
		BatchInfo batchInfo;
		try {
			batchInfo = new BatchInfo(random.nextInt(), getJsonPayload(payload).size());
		} catch (Exception exception) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(exception));
		}
		return HttpUtils.getAtContext(request).onItem().transformToMulti(t -> {
			Object obj;
			try {
				obj = JsonUtils.fromString(payload);
			} catch (Exception e) {
				return Multi.createFrom().failure(e);
			}
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
			return Multi.createFrom().items(jsonPayload.parallelStream()).onItem().transformToUni(t2 -> {
				String entityId = "NO ENTITY ID FOUND";
				if (t2 instanceof String) {
					entityId = (String) t2;
				} else {
					List<Object> resolved;
					try {
						resolved = JsonLdProcessor.expand(t, t2, opts, payloadType, atContextAllowed);
						entityId = (String) ((Map<String, Object>) resolved.get(0)).get(NGSIConstants.JSON_LD_ID);
					} catch (Exception e) {
						return Uni.createFrom().failure(e);
					}
				}
				return Uni.createFrom().item(Tuple3.of(entityId, headers, t));
			}).concatenate();
		}).onItem().transformToUni(
				t -> entityService.deleteEntry(t.getItem2(), t.getItem1(), t.getItem3()).onItem().transform(i -> {
					return i;
				})).collectFailures().concatenate().collect().asList().onItem().transform(results -> {
					return generateBatchResultReply(results, HttpStatus.SC_NO_CONTENT);
				}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);
	}

	@SuppressWarnings("unchecked")
	public static Uni<RestResponse<Object>> upsertMultiple(EntryCRUDService entityService, HttpServerRequest request,
			String payload, String options, int maxCreateBatch, int payloadType, Random random) {
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
		BatchInfo batchInfo = new BatchInfo(random.nextInt(), jsonPayload.size());
		return HttpUtils.getAtContext(request).onItem().transformToMulti(t -> {
			boolean preFlight;
			try {
				preFlight = HttpUtils.doPreflightCheck(request, t);
			} catch (ResponseException e) {
				return Multi.createFrom().failure(e);
			}
			return Multi.createFrom().items(jsonPayload.parallelStream()).onItem()
					.transform(i -> Tuple3.of(preFlight, t, i));
		}).onItem().transformToUni(tt -> {
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
				return entityService.sendFail(batchInfo).onItem().transformToUni(t -> Uni.createFrom().failure(e));
			}
			Tuple4<Map<String, Object>, Object, ArrayListMultimap<String, String>, String[]> t = Tuple4.of(expanded,
					tt.getItem2(), HttpUtils.getHeaders(request), getOptionsArray(options));

			Map<String, Object> entry = expanded;// t.getItem1();
			String entityId;
			if (entry.containsKey(NGSIConstants.JSON_LD_ID)) {
				entityId = (String) entry.get(NGSIConstants.JSON_LD_ID);
			} else {
				return entityService.sendFail(batchInfo).onItem().transformToUni(v -> Uni.createFrom()
						.failure(new ResponseException(ErrorType.BadRequestData, "No Entity Id provided")));
			}
			if (replace) {
				return entityService.deleteEntry(t.getItem3(), entityId, tt.getItem2()).onFailure()
						.recoverWithUni(e -> {
							return Uni.createFrom().failure(e);
						}).onItem().transformToUni(t2 -> {
							return entityService.createEntry(t.getItem3(), t.getItem1(), tt.getItem2(), batchInfo)
									.onItem().transform(i -> {
										return i;
									});
						}).onFailure().recoverWithUni(e -> {
							return entityService.sendFail(batchInfo).onItem()
									.transformToUni(v -> Uni.createFrom().failure(e));
						});
			} else {
				return entityService
						.appendToEntry(t.getItem3(), entityId, entry, t.getItem4(), tt.getItem2(), batchInfo).onItem()
						.transform(i -> {
							List<ResponseException> failures = i.getFailures();
							if (failures != null && !failures.isEmpty()) {
								for (ResponseException p : failures) {
									if (p.getErrorCode() == ErrorType.NotFound.getCode()) {
										entityService.createEntry(t.getItem3(), t.getItem1(), tt.getItem2(), batchInfo)
												.onItem().transform(i2 -> {
													return i2;
												});
										break;
									}
								}
							}
							return i;
						}).onFailure().recoverWithUni(e -> {
							return entityService.sendFail(batchInfo).onItem()
									.transformToUni(v -> Uni.createFrom().failure(e));
						});
			}
		}).collectFailures().concatenate().collect().asList().onItem().transform(results -> {
			return generateBatchResultReply(results, HttpStatus.SC_CREATED);
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

					Map<String, Object> body;
					try {
						body = ((Map<String, Object>) JsonUtils.fromString(payload));
					} catch (IOException e) {
						return Uni.createFrom().failure(e);
					}

					Map<String, Object> resolvedBody;
					try {
						resolvedBody = (Map<String, Object>) JsonLdProcessor
								.expand(contextHeaders, body, opts, payloadType, atContextAllowed).get(0);
					} catch (JsonLdError | ResponseException e) {
						return Uni.createFrom().failure(e);
					}
					Object bodyContext = body.get(JsonLdConsts.CONTEXT);
					if (bodyContext instanceof List) {
						context.addAll((List<Object>) bodyContext);
					} else {
						context.add(bodyContext);
					}
					return Uni.createFrom().item(Tuple2.of(resolvedBody, context));
				}).onItem()
				.transformToUni(resolved -> entityService
						.updateEntry(HttpUtils.getHeaders(request), entityId, resolved.getItem1(), resolved.getItem2())
						.onItem().transformToUni(updateResult -> {
							logger.trace("update entry :: completed");
							return HttpUtils.generateUpdateResultResponse(updateResult);
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
			Object originalPayload = JsonUtils.fromString(payload);
			List<Object> atContext;

			Map<String, Object> resolved;
			try {
				resolved = (Map<String, Object>) JsonLdProcessor
						.expand(t, originalPayload, opts, payloadType, atContextAllowed).get(0);
			} catch (Exception e) {
				return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
			}
			if (atContextAllowed) {
				Object payloadAtContext = ((Map<String, Object>) originalPayload).get(NGSIConstants.JSON_LD_CONTEXT);
				if (payloadAtContext == null) {
					atContext = Lists.newArrayList();
				} else if (payloadAtContext instanceof List) {
					atContext = (List<Object>) payloadAtContext;
				} else {
					atContext = Lists.newArrayList();
					atContext.add(payloadAtContext);
				}
			} else {
				atContext = t;
			}
			return entityService.createEntry(HttpUtils.getHeaders(request), resolved, atContext).onItem()
					.transform(operationResult -> {
						logger.trace("create entity :: completed");
						List<ResponseException> fails = operationResult.getFailures();
						List<CRUDSuccess> successes = operationResult.getSuccesses();
						if (fails.isEmpty()) {
							try {
								return RestResponse.created(new URI(baseUrl + operationResult.getEntityId()));
							} catch (URISyntaxException e) {
								return HttpUtils.handleControllerExceptions(e);
							}
						} else if (successes.isEmpty() && fails.size() == 1) {
							return HttpUtils.handleControllerExceptions(fails.get(0));
						} else {
							return new RestResponseBuilderImpl<Object>().status(207)
									.type(AppConstants.NGB_APPLICATION_JSON)
									.entity(JsonUtils.toPrettyString(operationResult.getJson())).build();
						}
					});
		}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);

	}

	@SuppressWarnings("unchecked")
	public static Uni<RestResponse<Object>> appendToEntry(EntryCRUDService entityService, HttpServerRequest request,
			String entityId, String payload, String options, int payloadType, Logger logger) {
		return Uni.combine().all().unis(HttpUtils.validateUri(entityId), HttpUtils.getAtContext(request)).asTuple()
				.onItem().transformToUni(t -> {
					logger.trace("append entity :: started");
					List<Object> contextHeaders = t.getItem2();
					String[] optionsArray = getOptionsArray(options);
					// try {
					boolean atContextAllowed;
					try {
						atContextAllowed = HttpUtils.doPreflightCheck(request, contextHeaders);
					} catch (ResponseException e1) {
						return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e1));
					}
					List<Object> context = new ArrayList<Object>();
					context.addAll(contextHeaders);
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
						resolved = (Map<String, Object>) JsonLdProcessor.expand(contextHeaders,
								JsonUtils.fromString(payload), opts, payloadType, atContextAllowed).get(0);
					} catch (Exception e) {
						return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
					}
					return entityService
							.appendToEntry(HttpUtils.getHeaders(request), entityId, resolved, optionsArray, context)
							.onItem().transformToUni(tResult -> {
								return HttpUtils.generateUpdateResultResponse(tResult);
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
		return Uni.combine().all().unis(HttpUtils.validateUri(entityId), HttpUtils.getAtContext(request)).asTuple()
				.onItem().transformToUni(t -> {
					return entityService.deleteEntry(HttpUtils.getHeaders(request), entityId, t.getItem2()).onItem()
							.transformToUni(t2 -> {
								logger.trace("delete entity :: completed");
								return HttpUtils.generateUpdateResultResponse(t2);
							});
				}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);
	}

}
