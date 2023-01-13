package eu.neclab.ngsildbroker.commons.controllers;

import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.Maps;
import com.google.common.net.HttpHeaders;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.BatchInfo;
import eu.neclab.ngsildbroker.commons.datatypes.results.CRUDSuccess;
import eu.neclab.ngsildbroker.commons.datatypes.results.NGSILDOperationResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.entityhandler.services.EntityService;
import io.smallrye.mutiny.CompositeException;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.mutiny.tuples.Tuple3;
import io.vertx.core.http.HttpServerRequest;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.http.HttpStatus;
import org.jboss.resteasy.reactive.RestResponse;
import org.jboss.resteasy.reactive.server.jaxrs.RestResponseBuilderImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

public interface EntryControllerFunctions {
	JsonLdOptions opts = new JsonLdOptions(JsonLdOptions.JSON_LD_1_1);

	Logger logger = LoggerFactory.getLogger(EntryControllerFunctions.class);

	@SuppressWarnings("unchecked")
	static Uni<RestResponse<Object>> updateMultiple(EntityService entityService, HttpServerRequest request,
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

		return Uni.createFrom().item(HttpUtils.getAtContext(request)).onItem().transformToMulti(t -> {
			boolean preFlight;
			try {
				preFlight = HttpUtils.doPreflightCheck(request, t);
			} catch (ResponseException e) {
				return Multi.createFrom().failure(e);
			}
			return Multi.createFrom().items(jsonPayload.parallelStream()).onItem()
					.transform(i -> Tuple3.of(preFlight, t, i));
		}).onItem().transformToUni(tupleItem -> {
			if (!tupleItem.getItem3().containsKey(NGSIConstants.JSON_LD_ID)
					&& !tupleItem.getItem3().containsKey(NGSIConstants.QUERY_PARAMETER_ID)) {
				return Uni.createFrom().failure(new Throwable("No id provided"));
			}
			Map<String, Object> entry;
			Context context;
			try {
				context = HttpUtils.getContextFromPayload(tupleItem.getItem3(), tupleItem.getItem2(),
						tupleItem.getItem1());
			} catch (ResponseException e) {
				return Uni.createFrom().failure(e);
			}
			try {
				entry = (Map<String, Object>) JsonLdProcessor
						.expand(tupleItem.getItem2(), tupleItem.getItem3(), opts, payloadType, tupleItem.getItem1())
						.get(0);
			} catch (Exception e1) {
				return Uni.createFrom().failure(e1);
			}

			return Uni.createFrom().item(Tuple3.of(context, entry, tupleItem.getItem1()));
		}).collectFailures().concatenate().collect().asList().onItemOrFailure().transformToUni((tuple3s, fails) -> {
			if (fails instanceof CompositeException) {
				// these are possible jsonld expansion error
				CompositeException compFail = (CompositeException) fails;

				for (Throwable fail : compFail.getCauses()) {
					// TODO collect failures from expanding here
				}
			} else {
				// unexpected failure abort
				return Uni.createFrom().item(HttpUtils.handleControllerExceptions(fails));

			}
			return entityService.updateMultipleEntry(HttpUtils.getTenant(request), tuple3s).onItem()
					.transform(HttpUtils::generateBatchResult);
		});

	}

	@SuppressWarnings("unchecked")
	static Uni<RestResponse<Object>> createMultiple(EntityService entityService, HttpServerRequest request,
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

		String tenant = HttpUtils.getTenant(request);
		return Uni.createFrom().item(HttpUtils.getAtContext(request)).onItem().transformToMulti(t -> {
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
			Context context;
			try {
				context = HttpUtils.getContextFromPayload(itemTuple.getItem2(), itemTuple.getItem1(),
						itemTuple.getItem3());
			} catch (ResponseException e) {
				return Uni.createFrom().failure(e);
			}
			try {
				expanded = (Map<String, Object>) JsonLdProcessor
						.expand(context, itemTuple.getItem2(), opts, payloadType, itemTuple.getItem3()).get(0);
			} catch (Exception e) {
				return Uni.createFrom().failure(e);
			}

			return Uni.createFrom().item(Tuple2.of(context, expanded));
		}).collectFailures().concatenate().collect().asList().onItemOrFailure().transformToUni((results, fails) -> {
			if (fails instanceof CompositeException) {
				// these are possible jsonld expansion error
				CompositeException compFail = (CompositeException) fails;

				for (Throwable fail : compFail.getCauses()) {
					// TODO collect failures from expanding here
				}
			} else {
				// unexpected failure abort
				return Uni.createFrom().item(HttpUtils.handleControllerExceptions(fails));

			}
			/// context
			return entityService.createMultipleEntry(tenant, results).onItem()
					.transform(HttpUtils::generateBatchResult);
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
	static Uni<RestResponse<Object>> deleteMultiple(EntityService entityService, HttpServerRequest request,
			String payload, int payloadType, Random random) {
		return Uni.createFrom().item(HttpUtils.getAtContext(request)).onItem().transformToMulti(t -> {
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

			return Multi.createFrom().items(jsonPayload.parallelStream()).onItem().transformToUni(t2 -> {
				String entityId = "NO ENTITY ID FOUND";
				if (t2 instanceof String) {
					entityId = (String) t2;
				} else {
					List<Object> resolved;
					try {
						resolved = JsonLdProcessor.expand(t, t2, opts, payloadType, atContextAllowed);
						if (((Map<String, Object>) resolved.get(0)).containsKey(NGSIConstants.JSON_LD_ID))
							entityId = (String) ((Map<String, Object>) resolved.get(0)).get(NGSIConstants.JSON_LD_ID);
					} catch (Exception e) {
						return Uni.createFrom().failure(e);
					}
				}
				return Uni.createFrom().item(entityId);
			}).concatenate();
		}).collect().asList().onItemOrFailure().transformToUni((ids, fail) -> entityService
				.deleteMultipleEntry(HttpUtils.getTenant(request), ids, new BatchInfo(random.nextInt(), ids.size()))
				.onItem().transform(HttpUtils::generateBatchResult));
	}

	@SuppressWarnings("unchecked")
	static Uni<RestResponse<Object>> upsertMultiple(EntityService entityService, HttpServerRequest request,
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
		return Uni.createFrom().item(HttpUtils.getAtContext(request)).onItem().transformToMulti(t -> {
			boolean preFlight;
			try {
				preFlight = HttpUtils.doPreflightCheck(request, t);
			} catch (ResponseException e) {
				return Multi.createFrom().failure(e);
			}
			return Multi.createFrom().items(jsonPayload.parallelStream()).onItem()
					.transform(i -> Tuple3.of(preFlight, t, i));
		}).onItem().transformToUni(tt -> {
			Context context;
			try {
				context = HttpUtils.getContextFromPayload(tt.getItem3(), tt.getItem2(), tt.getItem1());
			} catch (ResponseException e) {
				return Uni.createFrom().failure(e);
			}

			Map<String, Object> expanded;
			try {
				expanded = (Map<String, Object>) JsonLdProcessor
						.expand(tt.getItem2(), tt.getItem3(), opts, payloadType, tt.getItem1()).get(0);
			} catch (Exception e) {
				return entityService.sendFail(batchInfo).onItem().transformToUni(t -> Uni.createFrom().failure(e));
			}

			Map<String, Object> entry = expanded;// t.getItem1();
			String entityId;
			if (entry.containsKey(NGSIConstants.JSON_LD_ID)) {
				entityId = (String) entry.get(NGSIConstants.JSON_LD_ID);
			} else {
				return Uni.createFrom().failure(new Throwable("No Entity Id provided"));
			}

			return Uni.createFrom().item(Tuple3.of(context, entityId, expanded));
		}).collectFailures().concatenate().collect().asList().onItem().transformToUni(results -> {
			String tenant = HttpUtils.getTenant(request);
			List<String> toBeDeleted = new ArrayList<>();
			List<Tuple2<Context, Map<String, Object>>> toBeCreated = new ArrayList<>();
			List<Tuple3<Context, Map<String, Object>, Boolean>> toBeAppend = new ArrayList<>();
			for (Tuple3<Context, String, Map<String, Object>> tuple : results) {
				// Tuple< Context, id, payload>
				if (replace) {
					toBeDeleted.add(tuple.getItem2());
					toBeCreated.add(Tuple2.of(tuple.getItem1(), tuple.getItem3()));
				} else
					toBeAppend.add(Tuple3.of(tuple.getItem1(), tuple.getItem3(), true));
			}
			if (!toBeDeleted.isEmpty()) {
				return entityService.deleteMultipleEntry(tenant, toBeDeleted, batchInfo).onItem()
						.transformToUni(t -> entityService.createMultipleEntry(tenant, toBeCreated)).onItem()
						.transform(HttpUtils::generateBatchResult);
			} else {
				return entityService.updateMultipleEntry(tenant, toBeAppend).onItem()
						.transform(HttpUtils::generateBatchResult);
			}
		});
	}

	// public static Uni<RestResponse<Object>> updateEntry(EntryCRUDService
	// entityService, HttpServerRequest request,
//                                                        String entityId, String payload, int payloadType, Logger logger) throws ResponseException {
//        logger.trace("update entry :: started");
//        return Uni.combine().all().unis(HttpUtils.validateUri(entityId), HttpUtils.getAtContext(request)).asTuple()
//                .onItem().transformToUni(t -> {
//                    List<Object> contextHeaders = t.getItem2();
//                    boolean atContextAllowed;
//                    try {
//                        atContextAllowed = HttpUtils.doPreflightCheck(request, contextHeaders);
//                    } catch (ResponseException e) {
//                        return Uni.createFrom().failure(e);
//                    }
//
//                    Map<String, Object> body;
//                    try {
//                        body = ((Map<String, Object>) JsonUtils.fromString(payload));
//                    } catch (IOException e) {
//                        return Uni.createFrom().failure(e);
//                    }
//
//                    Map<String, Object> resolvedBody;
//                    Context context = null;
//                    try {
//                        context = HttpUtils.getContextFromPayload(body, contextHeaders, atContextAllowed);
//                    } catch (ResponseException e) {
//                        return Uni.createFrom().failure(e);
//                    }
//                    try {
//                        resolvedBody = (Map<String, Object>) JsonLdProcessor
//                                .expand(context, body, opts, payloadType, atContextAllowed).get(0);
//                        return Uni.createFrom().item(Tuple2.of(resolvedBody, context));
//                    } catch (JsonLdError | ResponseException e) {
//                        return Uni.createFrom().failure(e);
//                    }
//
//
//                })
//                .onItem()
//                .transform(resolved -> entityService
//                        .updateEntry(HttpUtils.getTenant(request), entityId, resolved.getItem1(), resolved.getItem2())
//                        .onItem().transformToUni(updateResult -> {
//                            logger.trace("update entry :: completed");
//                            return HttpUtils.generateUpdateResultResponse(updateResult);
//                        }))
//                .onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);
//    }

//    @SuppressWarnings({"unchecked", "unchecked"})
//    public static Uni<RestResponse<Object>> createEntry(EntryCRUDService entityService, HttpServerRequest request,
//                                                        String payload, int payloadType, String baseUrl, Logger logger) {
//        logger.trace("create entity :: started");
//        return HttpUtils.getAtContext(request).onItem().transformToUni(t -> {
//            boolean atContextAllowed;
//            try {
//                atContextAllowed = HttpUtils.doPreflightCheck(request, t);
//            } catch (ResponseException e1) {
//                return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e1));
//            }
//            if (payload == null || payload.isEmpty()) {
//                return Uni.createFrom().item(HttpUtils.handleControllerExceptions(
//                        new ResponseException(ErrorType.InvalidRequest, "You have to provide a valid payload")));
//            }
//            Map<String, Object> originalPayload;
//            try {
//                originalPayload = (Map<String, Object>) JsonUtils.fromString(payload);
//            } catch (Exception e) {
//                return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
//            }
//            Map<String, Object> resolved;
//            Context context;
//            try {
//                context = HttpUtils.getContextFromPayload(originalPayload, t, atContextAllowed);
//            } catch (ResponseException e) {
//                return Uni.createFrom().failure(e);
//            }
//            try {
//                resolved = (Map<String, Object>) JsonLdProcessor
//                        .expand(context, originalPayload, opts, payloadType, atContextAllowed).get(0);
//            } catch (Exception e) {
//                return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
//            }
//
//            return entityService.createEntry(HttpUtils.getTenant(request), resolved, context).onItem()
//                    .transform(operationResult -> {
//                        logger.trace("create entity :: completed");
//                        List<ResponseException> fails = operationResult.getFailures();
//                        List<CRUDSuccess> successes = operationResult.getSuccesses();
//                        if (fails.isEmpty()) {
//                            try {
//                                return RestResponse.created(new URI(baseUrl + operationResult.getEntityId()));
//                            } catch (URISyntaxException e) {
//                                return HttpUtils.handleControllerExceptions(e);
//                            }
//                        } else if (successes.isEmpty() && fails.size() == 1) {
//                            return HttpUtils.handleControllerExceptions(fails.get(0));
//                        } else {
//                            try {
//                                return new RestResponseBuilderImpl<Object>().status(207)
//                                        .type(AppConstants.NGB_APPLICATION_JSON)
//                                        .entity(JsonUtils.toPrettyString(operationResult.getJson())).build();
//                            } catch (Exception e) {
//                                return HttpUtils.handleControllerExceptions(e);
//                            }
//                        }
//                    });
//        }).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);
//
//    }

//    @SuppressWarnings("unchecked")
//    public static Uni<RestResponse<Object>> appendToEntry(EntryCRUDService entityService, HttpServerRequest request,
//                                                          String entityId, String payload, String options, int payloadType, Logger logger) {
//        return Uni.combine().all().unis(HttpUtils.validateUri(entityId), HttpUtils.getAtContext(request)).asTuple()
//                .onItem().transformToUni(t -> {
//                    logger.trace("append entity :: started");
//                    List<Object> contextHeaders = t.getItem2();
//                    String[] optionsArray = getOptionsArray(options);
//                    // try {
//                    boolean atContextAllowed;
//                    try {
//                        atContextAllowed = HttpUtils.doPreflightCheck(request, contextHeaders);
//                    } catch (ResponseException e1) {
//                        return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e1));
//                    }
//                    if (payload == null || payload.isEmpty()) {
//                        return Uni.createFrom().item(HttpUtils.handleControllerExceptions(
//                                new ResponseException(ErrorType.InvalidRequest, "An empty payload is not allowed")));
//                    }
//                    Map<String, Object> body;
//                    try {
//                        body = ((Map<String, Object>) JsonUtils.fromString(payload));
//                    } catch (Exception e) {
//                        return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
//                    }
//                    Context context = null;
//                    try {
//                        context = HttpUtils.getContextFromPayload(body, contextHeaders, atContextAllowed);
//                    } catch (ResponseException e) {
//                        return Uni.createFrom().failure(e);
//                    }
//                    Map<String, Object> resolved;
//                    try {
//                        resolved = (Map<String, Object>) JsonLdProcessor
//                                .expand(context, JsonUtils.fromString(payload), opts, payloadType, atContextAllowed)
//                                .get(0);
//                    } catch (Exception e) {
//                        return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
//                    }
//                    return entityService
//                            .appendToEntry(HttpUtils.getTenant(request), entityId, resolved, optionsArray, context)
//                            .onItem().transformToUni(tResult -> {
//                                return HttpUtils.generateUpdateResultResponse(tResult);
//                            });
//
//                }).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);
//    }

	private static String[] getOptionsArray(String options) {
		if (options == null) {
			return new String[0];
		} else {
			return options.split(",");
		}
	}

//    public static Uni<RestResponse<Object>> deleteEntry(EntryCRUDService entityService, HttpServerRequest request,
//                                                        String entityId, Logger logger) {
//        return Uni.combine().all().unis(HttpUtils.validateUri(entityId), HttpUtils.getAtContext(request)).asTuple()
//                .onItem().transformToUni(t -> {
//                    Context context = JsonLdProcessor.getCoreContextClone();
//                    if (t.getItem2() != null && !t.getItem2().isEmpty()) {
//                        context.parse(t.getItem2(), true);
//                    }
//                    return entityService.deleteEntry(HttpUtils.getTenant(request), entityId, context).onItem()
//                            .transformToUni(t2 -> {
//                                logger.trace("delete entity :: completed");
//                                return HttpUtils.generateUpdateResultResponse(t2);
//                            });
//                }).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);
//}

}
