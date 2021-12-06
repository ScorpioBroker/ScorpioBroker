package eu.neclab.ngsildbroker.commons.ngsiqueries;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLdOptions;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.EntityInfo;
import eu.neclab.ngsildbroker.commons.datatypes.GeoqueryRel;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

@Component
public class ParamsResolver {

	private final static Logger logger = LogManager.getLogger(ParamsResolver.class);

	private JsonLdOptions opts = new JsonLdOptions(JsonLdOptions.JSON_LD_1_1);

	public QueryParams getQueryParamsFromUriQuery(Map<String, String[]> ngsildQueryParams, Context context)
			throws ResponseException {
		return this.getQueryParamsFromUriQuery(ngsildQueryParams, context, false);
	}

//TODO REWORK THIS COMPLETELY 
	public List<QueryParams> getQueryParamsFromSubscription(Subscription subscription) {
//TODO check if this can be changed now since a list of entityinfos is in queryparam
		ArrayList<QueryParams> result = new ArrayList<QueryParams>();
		for (EntityInfo entityInfo : subscription.getEntities()) {
			QueryParams temp = new QueryParams();
			// String type = null, id = null, idPattern = null;
			List<Map<String, String>> entities = new ArrayList<Map<String, String>>();
			if (subscription.getNotification().getAttributeNames() != null
					&& !subscription.getNotification().getAttributeNames().isEmpty()) {
				temp.setAttrs(String.join(",", subscription.getNotification().getAttributeNames()));
			}
			HashMap<String, String> temp1 = new HashMap<String, String>();
			temp1.put(NGSIConstants.JSON_LD_TYPE, entityInfo.getType());
			if (entityInfo.getId() != null) {
				temp1.put(NGSIConstants.JSON_LD_ID, entityInfo.getId().toString());
			}
			if (entityInfo.getIdPattern() != null) {
				temp1.put(NGSIConstants.NGSI_LD_ID_PATTERN, entityInfo.getIdPattern());
			}
			entities.add(temp1);
			temp.setEntities(entities);
			if (subscription.getLdGeoQuery() != null) {
				temp.setGeometry(subscription.getLdGeoQuery().getGeometry().name());
				temp.setGeoproperty(subscription.getLdGeoQuery().getGeoProperty());
				temp.setGeorel(new GeoqueryRel(subscription.getLdGeoQuery().getGeoRelation()));
				StringBuilder builder = new StringBuilder();
				List<Double> coordinates = subscription.getLdGeoQuery().getCoordinates();
				for (int i = 0; i < coordinates.size(); i += 2) {
					builder.append("[");
					builder.append(coordinates.get(i));
					builder.append(",");
					builder.append(coordinates.get(i + 1));
					builder.append("]");
				}
				String coordinatesString;
				switch (temp.getGeometry().toLowerCase()) {
				case "polygon":
					coordinatesString = "[[" + builder.toString() + "]]";
					break;
				case "linestring":
					coordinatesString = "[" + builder.toString() + "]";
					break;
				case "point":
				default:
					coordinatesString = builder.toString();
					break;
				}
				temp.setCoordinates(coordinatesString);
			}
			if (subscription.getLdQuery() != null && !subscription.getLdQuery().isEmpty()) {
				temp.setQ(subscription.getLdQuery());
			}
			result.add(temp);
		}

		return result;
	}

	// new simplified format
	public QueryParams getQueryParamsFromUriQuery(Map<String, String[]> ngsildQueryParams, Context context,
			boolean temporalEntityFormat) throws ResponseException {
		logger.trace("call getStorageManagerJsonQuery method ::");
		try {
			QueryParams qp = new QueryParams();
			Iterator<String> it = ngsildQueryParams.keySet().iterator();
			String id = null, type = null, idPattern = null;
			while (it.hasNext()) {
				String queryParameter = it.next();
				String queryValue = ngsildQueryParams.get(queryParameter)[0];
				logger.debug("Query parameter:" + queryParameter + ", value=" + queryValue);
				GeoqueryRel geoqueryTokens;
				switch (queryParameter) {
				case NGSIConstants.QUERY_PARAMETER_ID:
					id = queryValue;
					break;
				case NGSIConstants.QUERY_PARAMETER_IDPATTERN:
					idPattern = queryValue;
					break;
				case NGSIConstants.QUERY_PARAMETER_TYPE:
					queryValue = expandQueryValues(context, queryValue);
					type = queryValue;
					break;
				case NGSIConstants.QUERY_PARAMETER_ATTRS:
					queryValue = expandQueryValues(context, queryValue);
					qp.setAttrs(queryValue);
					break;
				case NGSIConstants.QUERY_PARAMETER_GEOREL:
					String georel = queryValue;
					String geometry = "";
					String coordinates = "";
					String geoproperty = "";
					if (ngsildQueryParams.get(NGSIConstants.QUERY_PARAMETER_GEOMETRY) != null)
						geometry = ngsildQueryParams.get(NGSIConstants.QUERY_PARAMETER_GEOMETRY)[0];
					if (ngsildQueryParams.get(NGSIConstants.QUERY_PARAMETER_COORDINATES) != null)
						coordinates = ngsildQueryParams.get(NGSIConstants.QUERY_PARAMETER_COORDINATES)[0];
					if (ngsildQueryParams.get(NGSIConstants.QUERY_PARAMETER_GEOPROPERTY) != null) {
						geoproperty = ngsildQueryParams.get(NGSIConstants.QUERY_PARAMETER_GEOPROPERTY)[0];
						geoproperty = expandAttribute(geoproperty, context);
					} else {
						geoproperty = NGSIConstants.QUERY_PARAMETER_DEFAULT_GEOPROPERTY;
					}

					geoqueryTokens = QueryParser.parseGeoRel(georel);
					logger.debug("  Geoquery term georelOp: " + geoqueryTokens.getGeorelOp());

					if (geoqueryTokens.getGeorelOp().isEmpty() || geometry.isEmpty() || coordinates.isEmpty()) {
						throw new ResponseException(ErrorType.BadRequestData,
								"Georel detected but georel, geometry or coordinates are empty!");
					}
					if (!AppConstants.NGB_ALLOWED_GEOM_LIST.contains(geometry.toUpperCase())) {
						throw new ResponseException(ErrorType.BadRequestData,
								" geometry detected, Bad geometry!" + geometry);
					}
					validateCoordinates(coordinates);
					GeoqueryRel gr = new GeoqueryRel();
					gr.setGeorelOp(geoqueryTokens.getGeorelOp());
					gr.setDistanceType(geoqueryTokens.getDistanceType());
					gr.setDistanceValue(geoqueryTokens.getDistanceValue());

					qp.setGeorel(gr);
					qp.setGeometry(geometry);
					qp.setCoordinates(coordinates);
					qp.setGeoproperty(geoproperty);
					break;
				case NGSIConstants.QUERY_PARAMETER_TIMEREL:
					String timerel = queryValue;
					String timeAt = "";
					String timeproperty = "";
					String endTimeAt = "";
					if (ngsildQueryParams.get(NGSIConstants.QUERY_PARAMETER_TIME) != null)
						timeAt = ngsildQueryParams.get(NGSIConstants.QUERY_PARAMETER_TIME)[0];
					if (ngsildQueryParams.get(NGSIConstants.QUERY_PARAMETER_TIMEPROPERTY) != null) {
						timeproperty = ngsildQueryParams.get(NGSIConstants.QUERY_PARAMETER_TIMEPROPERTY)[0];
						timeproperty = expandAttribute(timeproperty, context);
					} else {
						timeproperty = NGSIConstants.QUERY_PARAMETER_DEFAULT_TIMEPROPERTY;
					}
					if (ngsildQueryParams.get(NGSIConstants.QUERY_PARAMETER_ENDTIME) != null)
						endTimeAt = ngsildQueryParams.get(NGSIConstants.QUERY_PARAMETER_ENDTIME)[0];

					if (timeAt.isEmpty()) {
						throw new ResponseException(ErrorType.BadRequestData, "Time is empty");
					}
					if (timerel.equals(NGSIConstants.TIME_REL_BETWEEN) && endTimeAt.isEmpty()) {
						throw new ResponseException(ErrorType.BadRequestData,
								"Timerel is between but endTime is empty");
					}

					qp.setTimerel(timerel);
					qp.setTimeAt(timeAt);
					qp.setTimeproperty(timeproperty);
					qp.setEndTimeAt(endTimeAt);
					break;
				case NGSIConstants.QUERY_PARAMETER_QUERY:
					qp.setQ(QueryParser.parseQuery(queryValue, context).toSql(temporalEntityFormat));
					break;
				case NGSIConstants.QUERY_PARAMETER_OPTIONS:
					List<String> options = Arrays.asList(queryValue.split(","));
					qp.setIncludeSysAttrs(options.contains(NGSIConstants.QUERY_PARAMETER_OPTIONS_SYSATTRS));
					qp.setKeyValues(options.contains(NGSIConstants.QUERY_PARAMETER_OPTIONS_KEYVALUES));
					qp.setTemporalValues(options.contains(NGSIConstants.QUERY_PARAMETER_OPTIONS_TEMPORALVALUES));
					break;
				}
			}
			List<Map<String, String>> entities = new ArrayList<Map<String, String>>();
			HashMap<String, String> temp = new HashMap<String, String>();
			if (id != null) {
				temp.put(NGSIConstants.JSON_LD_ID, id);
			}
			if (type != null) {
				temp.put(NGSIConstants.JSON_LD_TYPE, type);
			}
			if (idPattern != null) {
				temp.put(NGSIConstants.NGSI_LD_ID_PATTERN, idPattern);
			}
			if (id != null || idPattern != null || type != null) {
				entities.add(temp);
			}
			qp.setEntities(entities);
			return qp;
		} catch (ResponseException e) {
			throw e; // rethrow response exception object
		}
		// return null;
	}

	private void validateCoordinates(String coordinates) throws ResponseException {
		if (!coordinates.matches(
				"^\\[*(\\[\\s*[-+]?(180(\\.0+)?|((1[0-7]\\d)|([1-9]?\\d))(\\.\\d+)?)(,\\d)?,[-+]?([1-8]?\\d(\\.\\d+)?|90(\\.0+)?)\\],?)+\\]*$")) {
			throw new ResponseException(ErrorType.BadRequestData, "coordinates are not valid");
		}

	}

	public String expandQueryValues(Context context, String queryValue) throws ResponseException {
		String[] temp = queryValue.split(",");
		StringBuilder builder = new StringBuilder();
		for (String element : temp) {
			builder.append(expandAttribute(element.trim(), context));
			builder.append(",");
		}
		return builder.substring(0, builder.length() - 1);
	}

	public String expandAttribute(String attribute, Context context) throws ResponseException {
		logger.trace("resolveQueryLdContext():: started");
		return context.expandIri(attribute, false, true, null, null);
		// process reserved attributes
		/*
		 * switch (attribute) { case NGSIConstants.QUERY_PARAMETER_ID: return
		 * NGSIConstants.JSON_LD_ID; case NGSIConstants.QUERY_PARAMETER_TYPE: return
		 * NGSIConstants.JSON_LD_TYPE; case NGSIConstants.QUERY_PARAMETER_CREATED_AT:
		 * return NGSIConstants.NGSI_LD_CREATED_AT; case
		 * NGSIConstants.QUERY_PARAMETER_MODIFIED_AT: return
		 * NGSIConstants.NGSI_LD_MODIFIED_AT; case
		 * NGSIConstants.QUERY_PARAMETER_OBSERVED_AT: return
		 * NGSIConstants.NGSI_LD_OBSERVED_AT; case
		 * NGSIConstants.QUERY_PARAMETER_LOCATION: return
		 * NGSIConstants.NGSI_LD_LOCATION; case
		 * NGSIConstants.QUERY_PARAMETER_OBSERVATION_SPACE: return
		 * NGSIConstants.NGSI_LD_OBSERVATION_SPACE; case
		 * NGSIConstants.QUERY_PARAMETER_OPERATION_SPACE: return
		 * NGSIConstants.NGSI_LD_OPERATION_SPACE; }
		 * 
		 * // custom attributes String attributeResolved = attribute;
		 * logger.debug("link: " + linkHeaders); String jsonLdAttribute =
		 * getJsonLdAttribute(attribute, linkHeaders); logger.debug("jsonLdAttribute: "
		 * + jsonLdAttribute); // LocalDateTime start = LocalDateTime.now(); String
		 * jsonLdAttributeResolved; try { jsonLdAttributeResolved =
		 * JsonUtils.toString(JsonLdProcessor.expand(linkHeaders,
		 * JsonUtils.fromString(jsonLdAttribute), opts, AppConstants.ATTRIBUTE_PAYLOAD,
		 * true).get(0)); } catch (JsonLdError | IOException | ResponseException e) {
		 * return ""; } // contextResolver.expand(jsonLdAttribute, context, false, //
		 * AppConstants.INTERNAL_CALL_ID); // LocalDateTime end = LocalDateTime.now();
		 * logger.debug("jsonLdAttributeResolved: " + jsonLdAttributeResolved);
		 * JsonParser parser = new JsonParser(); JsonElement jsonTree =
		 * parser.parse(jsonLdAttributeResolved); if (jsonTree.isJsonObject()) {
		 * JsonObject jsonObject = jsonTree.getAsJsonObject(); if
		 * (jsonObject.entrySet().size() > 0) attributeResolved =
		 * jsonObject.entrySet().iterator().next().getKey(); }
		 * logger.trace("resolveQueryLdContext():: completed"); return
		 * attributeResolved;
		 */
	}

	/*
	 * private String getJsonLdAttribute(String attribute, List<Object> context) {
	 * logger.trace("getJsonLdAttribute():: started"); String jsonString = null; try
	 * { JsonNode rootNode = objectMapper.createObjectNode(); // if (context !=
	 * null) { // ArrayNode contextNode = objectMapper.valueToTree(context); //
	 * ((ObjectNode) rootNode).putArray("@context").addAll(contextNode); // } //
	 * cant be in here like that ((ObjectNode) rootNode).put(attribute, "");
	 * jsonString =
	 * objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(rootNode); }
	 * catch (JsonProcessingException e) { logger.error("Exception ::", e);
	 * e.printStackTrace(); } logger.trace("getJsonLdAttribute():: completed");
	 * return jsonString; }
	 */

}
