package eu.neclab.ngsildbroker.commons.tools;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import com.fasterxml.jackson.core.JsonParseException;
import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLdError;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.datatypes.RestResponse;
import eu.neclab.ngsildbroker.commons.datatypes.results.AppendResult;
import eu.neclab.ngsildbroker.commons.datatypes.results.BatchFailure;
import eu.neclab.ngsildbroker.commons.datatypes.results.BatchResult;
import eu.neclab.ngsildbroker.commons.datatypes.results.QueryResult;
import eu.neclab.ngsildbroker.commons.datatypes.results.UpdateResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.EntityQueryService;
import eu.neclab.ngsildbroker.commons.interfaces.EntryCRUDService;
import eu.neclab.ngsildbroker.commons.ngsiqueries.ParamsResolver;
import eu.neclab.ngsildbroker.commons.ngsiqueries.QueryParser;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;

public class ControllerFunctions {
	private static JsonLdOptions opts = new JsonLdOptions(JsonLdOptions.JSON_LD_1_1);

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
						.expand(linkHeaders, compactedEntry, opts, payloadType, preFlight)
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
				AppendResult updateResult = entityService.appendToEntry(headers, entityId, entry, optionsArray);
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
	private static List<Map<String, Object>> getJsonPayload(String payload)
			throws ResponseException, JsonParseException, IOException {
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
				return HttpUtils.handleControllerExceptions(new ResponseException(ErrorType.InvalidRequest,
						"This interface only supports arrays of entities"));
			}
			jsonPayload = (List<Object>) obj;
			atContextAllowed = HttpUtils.doPreflightCheck(request, links);
		} catch (Exception exception) {
			return HttpUtils.handleControllerExceptions(exception);
		}
		if (jsonPayload.isEmpty()) {
			return HttpUtils.handleControllerExceptions(
					new ResponseException(ErrorType.InvalidRequest, "An empty array is not allowed"));
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
							payloadType, atContextAllowed);
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
							AppendResult updateResult = entityService.appendToEntry(headers, entityId, resolved,
									optionsArray);
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

	public static ResponseEntity<String> updateEntry(EntryCRUDService entityService, HttpServletRequest request,
			String entityId, String payload, int payloadType, Logger logger) {
		try {
			logger.trace("update entry :: started");
			List<Object> contextHeaders = HttpUtils.getAtContext(request);
			boolean atContextAllowed = HttpUtils.doPreflightCheck(request, contextHeaders);
			@SuppressWarnings("unchecked")
			Map<String, Object> resolved = (Map<String, Object>) JsonLdProcessor.expand(contextHeaders,
					JsonUtils.fromString(payload), opts, payloadType, atContextAllowed).get(0);
			UpdateResult update = entityService.updateEntry(HttpUtils.getHeaders(request), entityId, resolved);
			logger.trace("update entry :: completed");
			if (update.getUpdateResult()) {
				return ResponseEntity.noContent().build();
			} else {
				return ResponseEntity.status(HttpStatus.MULTI_STATUS)
						.body(JsonUtils.toPrettyString(update.getAppendedJsonFields()));
			}
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

			@SuppressWarnings("unchecked")
			Map<String, Object> resolved = (Map<String, Object>) JsonLdProcessor.expand(contextHeaders,
					JsonUtils.fromString(payload), opts, payloadType, atContextAllowed).get(0);
			result = entityService.createEntry(HttpUtils.getHeaders(request), resolved);
			logger.trace("create entity :: completed");
			return ResponseEntity.status(HttpStatus.CREATED).header("location", baseUrl + result)
					.build();
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
			@SuppressWarnings("unchecked")
			Map<String, Object> resolved = (Map<String, Object>) JsonLdProcessor.expand(contextHeaders,
					JsonUtils.fromString(payload), opts, payloadType, atContextAllowed).get(0);
			AppendResult append = entityService.appendToEntry(HttpUtils.getHeaders(request), entityId, resolved,
					optionsArray);
			logger.trace("append entity :: completed");
			if (append.getAppendResult()) {
				return ResponseEntity.noContent().build();
			} else {
				return ResponseEntity.status(HttpStatus.MULTI_STATUS)
						.body(JsonUtils.toPrettyString(append.getAppendedJsonFields()));
			}
		} catch (Exception exception) {
			return HttpUtils.handleControllerExceptions(exception);
		}
	}

	private static String[] getOptionsArray(String options) {
		if(options == null) {
			return new String[0];
		}else {
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

	// these are known structures in try catch. failed parsing would rightfully
	// result in an error
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static ResponseEntity<String> postQuery(EntityQueryService queryService, HttpServletRequest request,
			String payload, Integer limit, Integer offset, String qToken, List<String> options, boolean count,
			int defaultLimit, int payloadType) {
		try {
			List<Object> linkHeaders = HttpUtils.getAtContext(request);
			boolean atContextAllowed = HttpUtils.doPreflightCheck(request, linkHeaders);
			Map<String, Object> rawPayload = (Map<String, Object>) JsonUtils.fromString(payload);
			// String expandedPayload = "";// HttpUtils.expandPayload(request, payload,
			// AppConstants.BATCH_URL_ID);
			Map<String, Object> queries = (Map<String, Object>) JsonLdProcessor
					.expand(linkHeaders, rawPayload, opts, payloadType, atContextAllowed).get(0);

			if (rawPayload.containsKey(NGSIConstants.JSON_LD_CONTEXT)) {
				linkHeaders.add((String) rawPayload.get(NGSIConstants.JSON_LD_CONTEXT));
			}
			Context context = JsonLdProcessor.getCoreContextClone();

			context = context.parse(linkHeaders, true);

			QueryParams params = new QueryParams();
			if (limit == null) {
				limit = defaultLimit;
			}
			if (offset == null) {
				offset = 0;
			}
			boolean typeProvided = false;
			for (Entry<String, Object> entry : queries.entrySet()) {
				switch (entry.getKey()) {
				case NGSIConstants.NGSI_LD_ATTRS:
					List<Map<String, String>> attrs = (List<Map<String, String>>) entry.getValue();
					StringBuilder builder = new StringBuilder();
					for (Map<String, String> attr : attrs) {
						builder.append(ParamsResolver.expandAttribute(attr.get(NGSIConstants.JSON_LD_VALUE), context));
						builder.append(',');
					}
					params.setAttrs(builder.substring(0, builder.length() - 1));
					break;
				case NGSIConstants.NGSI_LD_ENTITIES:
					List<Map<String, String>> entities = new ArrayList<Map<String, String>>();
					for (Map<String, Object> entry2 : (List<Map<String, Object>>) entry.getValue()) {
						HashMap<String, String> temp = new HashMap<String, String>();
						for (Entry<String, Object> entry3 : entry2.entrySet()) {
							if (entry3.getValue() instanceof String) {
								temp.put(entry3.getKey(), (String) entry3.getValue());
							} else {
								Object tempItem = ((List) entry3.getValue()).get(0);
								if (tempItem instanceof String) {
									temp.put(entry3.getKey(), (String) tempItem);
								} else {
									temp.put(entry3.getKey(), (String) ((List<Map<String, Object>>) entry3.getValue())
											.get(0).get(NGSIConstants.JSON_LD_VALUE));
								}
							}
						}
						entities.add(temp);
					}
					params.setEntities(entities);
					break;
				case NGSIConstants.NGSI_LD_GEO_QUERY:
					Map<String, Object> geoQuery = ((List<Map<String, Object>>) entry.getValue()).get(0);
					params.setCoordinates(protectGeoProp(geoQuery));
					params.setGeometry((String) getValue(geoQuery.get(NGSIConstants.NGSI_LD_GEOMETRY)));
					if (geoQuery.containsKey(NGSIConstants.NGSI_LD_GEOPROPERTY)) {
						params.setGeoproperty((String) getValue(geoQuery.get(NGSIConstants.NGSI_LD_GEOPROPERTY)));
					}
					params.setGeorel(
							QueryParser.parseGeoRel((String) getValue(geoQuery.get(NGSIConstants.NGSI_LD_GEO_REL))));
					break;
				case NGSIConstants.NGSI_LD_QUERY:
					params.setQ(QueryParser.parseQuery((String) getValue(entry.getValue()), context).toSql(false));
					break;
				case NGSIConstants.JSON_LD_TYPE:
					if (entry.getValue() instanceof List) {
						if (((List) entry.getValue()).get(0).toString()
								.equals(NGSIConstants.NGSI_LD_DEFAULT_PREFIX + NGSIConstants.QUERY_TYPE)) {
							typeProvided = true;
							break;
						}
					}
					throw new ResponseException(ErrorType.BadRequestData, "Type has to be Query for this operation");
				default:
					throw new ResponseException(ErrorType.BadRequestData, entry.getKey() + " is an unknown entry");
				}
			}
			if (!typeProvided) {
				throw new ResponseException(ErrorType.BadRequestData,
						"No type provided. Type has to be Query for this operation");
			}
			params.setKeyValues((options != null && options.contains(NGSIConstants.QUERY_PARAMETER_OPTIONS_KEYVALUES)));
			params.setIncludeSysAttrs(
					(options != null && options.contains(NGSIConstants.QUERY_PARAMETER_OPTIONS_SYSATTRS)));
			return generateReply(request, queryService.getData(params, payload, linkHeaders, limit, offset, qToken,
					false, count, HttpUtils.getHeaders(request), true), true, count, context, linkHeaders);

		} catch (Exception exception) {
			return HttpUtils.handleControllerExceptions(exception);
		}
	}

	private static ResponseEntity<String> generateReply(HttpServletRequest request, QueryResult qResult,
			boolean forceArray, boolean count, Context context, List<Object> contextLinks) throws ResponseException {
		String nextLink = HttpUtils.generateNextLink(request, qResult);
		String prevLink = HttpUtils.generatePrevLink(request, qResult);
		ArrayList<Object> additionalLinks = new ArrayList<Object>();
		if (nextLink != null) {
			additionalLinks.add(nextLink);
		}
		if (prevLink != null) {
			additionalLinks.add(prevLink);
		}
		ArrayListMultimap<String, String> additionalHeaders = ArrayListMultimap.create();
		if (count == true) {
			additionalHeaders.put(NGSIConstants.COUNT_HEADER_RESULT, String.valueOf(qResult.getCount()));
		}

		if (!additionalLinks.isEmpty()) {
			for (Object entry : additionalLinks) {
				additionalHeaders.put(HttpHeaders.LINK, (String) entry);
			}
		}
		return HttpUtils.generateReply(request, "[" + String.join(",", qResult.getDataString()) + "]",
				additionalHeaders, context, contextLinks, forceArray, AppConstants.QUERY_ENDPOINT);
	}

	private static String protectGeoProp(Map<String, Object> value) throws ResponseException {
		Object potentialStringValue = value.get(NGSIConstants.JSON_LD_VALUE);
		if (potentialStringValue != null) {
			return (String) potentialStringValue;
		}

		Map<String, Object> compactedFull = JsonLdProcessor.compact(value, JsonLdProcessor.getCoreContextClone(), opts);
		compactedFull.remove(NGSIConstants.JSON_LD_CONTEXT);
		String geoType = (String) compactedFull.get(NGSIConstants.QUERY_PARAMETER_GEOMETRY);
		// This is needed because one context could map from type which wouldn't work
		// with the used context.
		// Used context is needed because something could map point
		// This is not good but new geo type will come so this can go away at some time
		if (geoType == null) {
			compactedFull = JsonLdProcessor.compact(value, JsonLdProcessor.getCoreContextClone(), opts);
			compactedFull.remove(NGSIConstants.JSON_LD_CONTEXT);
			geoType = (String) compactedFull.get(NGSIConstants.GEO_JSON_TYPE);

		}
		@SuppressWarnings("rawtypes")
		// this is fine we check types later on
		List geoValues = (List) compactedFull.get(NGSIConstants.GEO_JSON_COORDINATES);
		Object entry1, entry2;
		switch (geoType) {
		case NGSIConstants.GEO_TYPE_POINT:
			// nothing to be done here point is ok like this
			entry1 = geoValues.get(0);
			entry2 = geoValues.get(1);
			if ((!(entry1 instanceof Double) && !(entry1 instanceof Integer))
					|| (!(entry2 instanceof Double) && !(entry2 instanceof Integer))) {
				throw new ResponseException(ErrorType.BadRequestData, "Provided coordinate entry is not a float value");
			}
			break;
		case NGSIConstants.GEO_TYPE_LINESTRING:
			ArrayList<Object> containerList = new ArrayList<Object>();
			for (int i = 0; i < geoValues.size(); i += 2) {
				ArrayList<Object> container = new ArrayList<Object>();
				entry1 = geoValues.get(i);
				entry2 = geoValues.get(i + 1);
				if ((!(entry1 instanceof Double) && !(entry1 instanceof Integer))
						|| (!(entry2 instanceof Double) && !(entry2 instanceof Integer))) {
					throw new ResponseException(ErrorType.BadRequestData,
							"Provided coordinate entry is not a float value");
				}
				container.add(entry1);
				container.add(entry2);
				containerList.add(container);
			}
			compactedFull.put(NGSIConstants.GEO_JSON_COORDINATES, containerList);
			break;

		case NGSIConstants.GEO_TYPE_POLYGON:
			ArrayList<Object> topLevelContainerList = new ArrayList<Object>();
			ArrayList<Object> polyContainerList = new ArrayList<Object>();
			if (!geoValues.get(0).equals(geoValues.get(geoValues.size() - 2))
					|| !geoValues.get(1).equals(geoValues.get(geoValues.size() - 1))) {
				throw new ResponseException(ErrorType.BadRequestData, "Polygon does not close");
			}
			for (int i = 0; i < geoValues.size(); i += 2) {
				ArrayList<Object> container = new ArrayList<Object>();
				entry1 = geoValues.get(i);
				entry2 = geoValues.get(i + 1);
				if ((!(entry1 instanceof Double) && !(entry1 instanceof Integer))
						|| (!(entry2 instanceof Double) && !(entry2 instanceof Integer))) {
					throw new ResponseException(ErrorType.BadRequestData,
							"Provided coordinate entry is not a float value");
				}
				container.add(entry1);
				container.add(entry2);
				polyContainerList.add(container);
			}
			topLevelContainerList.add(polyContainerList);
			compactedFull.put(NGSIConstants.GEO_JSON_COORDINATES, topLevelContainerList);
			break;
		case NGSIConstants.GEO_TYPE_MULTI_POLYGON:
			ArrayList<Object> multiTopLevelContainerList = new ArrayList<Object>();
			ArrayList<Object> multiMidLevelContainerList = new ArrayList<Object>();
			ArrayList<Object> multiPolyContainerList = new ArrayList<Object>();
			for (int i = 0; i < geoValues.size(); i += 2) {
				ArrayList<Object> container = new ArrayList<Object>();
				entry1 = geoValues.get(i);
				entry2 = geoValues.get(i + 1);
				if ((!(entry1 instanceof Double) && !(entry1 instanceof Integer))
						|| (!(entry2 instanceof Double) && !(entry2 instanceof Integer))) {
					throw new ResponseException(ErrorType.BadRequestData,
							"Provided coordinate entry is not a float value");
				}
				container.add(entry1);
				container.add(entry2);
				multiPolyContainerList.add(container);
			}
			multiMidLevelContainerList.add(multiPolyContainerList);
			multiTopLevelContainerList.add(multiMidLevelContainerList);

			compactedFull.put(NGSIConstants.GEO_JSON_COORDINATES, multiTopLevelContainerList);
			break;

		default:
			break;
		}
		String protectedValue;
		try {
			protectedValue = JsonUtils.toString(compactedFull);
		} catch (IOException e) {
			throw new ResponseException(ErrorType.BadRequestData, "Failed to handle provided coordinates");
		}
		return protectedValue;
	}

	// known structure from json ld lib
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static Object getValue(Object original) {
		if (original instanceof List) {
			original = ((List) original).get(0);
		}
		return ((Map<String, Object>) original).get(NGSIConstants.JSON_LD_VALUE);

	}
	
}
