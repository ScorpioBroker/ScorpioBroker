package eu.neclab.ngsildbroker.commons.ngsiqueries;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.util.MultiValueMap;

import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.utils.JsonUtils;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.EntityInfo;
import eu.neclab.ngsildbroker.commons.datatypes.GeoqueryRel;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;

public class ParamsResolver {
	static HashSet<String> validParams = new HashSet<String>();
	static {
		validParams.add(NGSIConstants.QUERY_PARAMETER_TYPE);
		validParams.add(NGSIConstants.QUERY_PARAMETER_ID);
		validParams.add(NGSIConstants.QUERY_PARAMETER_IDPATTERN);
		validParams.add(NGSIConstants.QUERY_PARAMETER_ATTRS);
		validParams.add(NGSIConstants.QUERY_PARAMETER_QUERY);
		validParams.add(NGSIConstants.QUERY_PARAMETER_GEOREL);
		validParams.add(NGSIConstants.QUERY_PARAMETER_GEOMETRY);
		validParams.add(NGSIConstants.QUERY_PARAMETER_COORDINATES);
		validParams.add(NGSIConstants.QUERY_PARAMETER_GEOPROPERTY);
		validParams.add(NGSIConstants.QUERY_PARAMETER_TIMEPROPERTY);
		validParams.add(NGSIConstants.QUERY_PARAMETER_OFFSET);
		validParams.add(NGSIConstants.QUERY_PARAMETER_LIMIT);
		validParams.add(NGSIConstants.QUERY_PARAMETER_QTOKEN);
		validParams.add(NGSIConstants.QUERY_PARAMETER_OPTIONS);
		validParams.add(NGSIConstants.QUERY_PARAMETER_DETAILS);
		validParams.add(NGSIConstants.COUNT_HEADER_RESULT);
		validParams.add(NGSIConstants.QUERY_PARAMETER_TIMEREL);
		validParams.add(NGSIConstants.QUERY_PARAMETER_TIME);
		validParams.add(NGSIConstants.QUERY_PARAMETER_LAST_N);
	}

	private final static Logger logger = LogManager.getLogger(ParamsResolver.class);

//TODO REWORK THIS COMPLETELY 
	public static List<QueryParams> getQueryParamsFromSubscription(Subscription subscription) {
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
			if (subscription.getLdQuery() != null && !subscription.getLdQueryString().isEmpty()) {
				temp.setQ(subscription.getLdQueryString());
			}
			result.add(temp);
		}

		return result;
	}

	// new simplified format
	public static QueryParams getQueryParamsFromUriQuery(MultiValueMap<String, String> multiValueMap, Context context,
			boolean temporalEntityFormat, boolean typeRequired, int defaultLimit, int maxLimit)
			throws ResponseException {
		QueryParams qp = new QueryParams();
		Iterator<String> it = multiValueMap.keySet().iterator();
		String id = null, type = null, idPattern = null;
		String geometryProperty = null;
		HashSet<String> attrs = null;
		String timerel = null;
		String geometry = null;
		String coordinates = null;
		String geoproperty = null;
		String timeAt = null;
		String timeproperty = null;
		String endTimeAt = null;
		String georel = null;
		int limit = defaultLimit;
		int offset = 0;
		while (it.hasNext()) {
			String queryParameter = it.next();
			String queryValue = multiValueMap.getFirst(queryParameter);
			logger.debug("Query parameter:" + queryParameter + ", value=" + queryValue);
			switch (queryParameter) {
			case NGSIConstants.QUERY_PARAMETER_ID:
				id = queryValue;
				HttpUtils.validateUri(id);
				break;
			case NGSIConstants.QUERY_PARAMETER_IDPATTERN:
				idPattern = queryValue;
				break;
			case NGSIConstants.QUERY_PARAMETER_TYPE:
				type = String.join(",", expandQueryValues(context, queryValue));
				break;
			case NGSIConstants.QUERY_PARAMETER_ATTRS:
				attrs = expandQueryValues(context, queryValue);
				break;
			case NGSIConstants.QUERY_PARAMETER_GEOMETRY_PROPERTY:
				geometryProperty = expandQueryValues(context, queryValue).iterator().next();
				break;
			case NGSIConstants.QUERY_PARAMETER_GEOREL:
				georel = queryValue;
				break;
			case NGSIConstants.QUERY_PARAMETER_GEOMETRY:
				geometry = queryValue;
				break;
			case NGSIConstants.QUERY_PARAMETER_COORDINATES:
				coordinates = queryValue;
				break;
			case NGSIConstants.QUERY_PARAMETER_GEOPROPERTY:
				geoproperty = expandAttribute(queryValue, context);
				break;
			case NGSIConstants.QUERY_PARAMETER_TIMEREL:
				timerel = queryValue;
				break;
			case NGSIConstants.QUERY_PARAMETER_TIME:
				timeAt = queryValue;
				break;
			case NGSIConstants.QUERY_PARAMETER_TIMEPROPERTY:
				timeproperty = expandAttribute(queryValue, context);
				break;
			case NGSIConstants.QUERY_PARAMETER_ENDTIME:
				endTimeAt = queryValue;
				break;
			case NGSIConstants.QUERY_PARAMETER_QUERY:
				qp.setQ(QueryParser.parseQuery(queryValue, context).toSql(temporalEntityFormat));
				break;
			case NGSIConstants.QUERY_PARAMETER_SCOPE_QUERY:
				qp.setScopeQ(QueryParser.parseScopeQuery(queryValue).toSql());
				break;
			case NGSIConstants.QUERY_PARAMETER_OPTIONS:
				List<String> options = Arrays.asList(queryValue.split(","));
				qp.setIncludeSysAttrs(options.contains(NGSIConstants.QUERY_PARAMETER_OPTIONS_SYSATTRS));
				qp.setKeyValues(options.contains(NGSIConstants.QUERY_PARAMETER_OPTIONS_KEYVALUES));
				qp.setTemporalValues(options.contains(NGSIConstants.QUERY_PARAMETER_OPTIONS_TEMPORALVALUES));
				break;
			case NGSIConstants.QUERY_PARAMETER_LIMIT:
				limit = Integer.parseInt(queryValue);
				if (limit > maxLimit) {
					throw new ResponseException(ErrorType.TooManyResults, "Limit exceeds max limit of " + maxLimit);
				}
				break;
			case NGSIConstants.QUERY_PARAMETER_OFFSET:
				offset = Integer.parseInt(queryValue);
				if (offset < 0) {
					throw new ResponseException(ErrorType.InvalidRequest, "Offset can not be smaller than 0");
				}
				break;
			case NGSIConstants.QUERY_PARAMETER_LAST_N:
				qp.setLastN(Integer.parseInt(queryValue));
				break;
			case NGSIConstants.QUERY_PARAMETER_DETAILS:
				// nothing to do here this is handled outside tbc!
				break;
			case NGSIConstants.QUERY_PARAMETER_COUNT:
				qp.setCountResult(true);
				break;
			case NGSIConstants.QUERY_PARAMETER_CSF:
				qp.setCsf(QueryParser.parseQuery(queryValue, context).toSql(temporalEntityFormat));
				break;
			default:
				throw new ResponseException(ErrorType.BadRequestData, queryParameter + " is unknown");
			}

		}
		if (attrs != null) {
			if (geometryProperty != null) {
				attrs.add(geometryProperty);
			}
			qp.setAttrs(String.join(",", attrs));
		}
		qp.setLimit(limit);
		qp.setOffSet(offset);
		handleGeoQuery(georel, geoproperty, coordinates, geometry, qp);
		handleTimeQuery(timerel, timeAt, timeproperty, endTimeAt, qp);
		List<Map<String, String>> entities = new ArrayList<Map<String, String>>();
		HashMap<String, String> temp = new HashMap<String, String>();
		if (typeRequired && type == null && attrs == null) {
			throw new ResponseException(ErrorType.BadRequestData, "Missing mandatory minimum parameter "
					+ NGSIConstants.QUERY_PARAMETER_TYPE + " or " + NGSIConstants.QUERY_PARAMETER_ATTRS);
		}

		if (!qp.getCountResult() && limit == 0) {
			throw new ResponseException(ErrorType.BadRequestData, "limit can only be 0 if count is activated");
		}
		if (qp.getGeometry() != null && !qp.getGeometry().isEmpty()) {
			if (!NGSIConstants.ALLOWED_GEOMETRIES.contains(qp.getGeometry())) {
				throw new ResponseException(ErrorType.BadRequestData, "Invalid geometry provided");
			}
		}
		if (qp.getGeorel() != null && qp.getGeorel().getGeorelOp() != null && !qp.getGeorel().getGeorelOp().isEmpty()) {
			if (!NGSIConstants.ALLOWED_GEOREL.contains(qp.getGeorel().getGeorelOp())) {
				throw new ResponseException(ErrorType.BadRequestData, "Invalid georel provided");
			}
		}
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

	}

	@SuppressWarnings("unchecked")
	private static void validateCoordinates(List<Object> coordinateList, String type) throws ResponseException {
		try {
			switch (type) {
			case NGSIConstants.GEO_TYPE_POINT:
				if (coordinateList.size() != 2) {
					throw new ResponseException(ErrorType.BadRequestData, "points have to be 2 entries");
				}
				break;
			case NGSIConstants.GEO_TYPE_LINESTRING:
				if (coordinateList.size() < 2) {
					throw new ResponseException(ErrorType.BadRequestData,
							"Linestring has to be an array of at least 2 points");
				}
				for (Object entry : coordinateList) {
					if (!(entry instanceof List)) {
						throw new ResponseException(ErrorType.BadRequestData,
								"Linestring has to be an array of at least 2 points");
					}
					validateCoordinates((List<Object>) entry, NGSIConstants.GEO_TYPE_POINT);
				}
				break;
			case NGSIConstants.GEO_TYPE_POLYGON:
				boolean error = false;
				if (coordinateList.size() == 1 && (coordinateList.get(0) instanceof List)) {
					coordinateList = (List<Object>) coordinateList.get(0);
					if (coordinateList.size() > 1 && (coordinateList.get(0) instanceof List)) {
						if (coordinateList.size() < 4
								|| !coordinateList.get(0).equals(coordinateList.get(coordinateList.size() - 1))) {
							throw new ResponseException(ErrorType.BadRequestData,
									"Polygons have to be an array of array with at least 3 points and have to close");
						}
						for (Object entry : coordinateList) {
							if (!(entry instanceof List)) {
								throw new ResponseException(ErrorType.BadRequestData,
										"Polygons have to be an array of array with at least 3 points and have to close");
							}
							validateCoordinates((List<Object>) entry, NGSIConstants.GEO_TYPE_POINT);
						}
					} else {
						error = true;
					}
				} else {
					error = true;
				}
				if (error) {
					throw new ResponseException(ErrorType.BadRequestData,
							"Polygons have to be an array of array with at least 3 points and have to close");
				}
				break;
			default:
				throw new ResponseException(ErrorType.BadRequestData, "Unsupported type");
			}
		} catch (Exception e) {
			if (e instanceof ResponseException) {
				throw e;
			}
			logger.error("failed to parse coordinates", e);
			throw new ResponseException(ErrorType.BadRequestData, "coordinates are not valid");

		}

	}

	private static HashSet<String> expandQueryValues(Context context, String queryValue) throws ResponseException {
		HashSet<String> result = new HashSet<String>();
		String[] temp = queryValue.split(",");
		for (String element : temp) {
			result.add(expandAttribute(element.trim(), context));
		}
		return result;
	}

	public static String expandAttribute(String attribute, Context context) throws ResponseException {
		logger.trace("resolveQueryLdContext():: started");
		return context.expandIri(attribute, false, true, null, null);
	}

	private static void handleTimeQuery(String timerel, String timeAt, String timeproperty, String endTimeAt,
			QueryParams qp) throws ResponseException {
		if (timerel == null && timeAt == null && timeproperty == null && endTimeAt == null) {
			return;
		}
		if (timeproperty == null) {
			timeproperty = NGSIConstants.QUERY_PARAMETER_DEFAULT_TIMEPROPERTY;
		}
		if (timeAt == null || timeAt.isEmpty()) {
			throw new ResponseException(ErrorType.BadRequestData, "Time is empty");
		}
		if (timerel.equals(NGSIConstants.TIME_REL_BETWEEN) && (endTimeAt == null || endTimeAt.isEmpty())) {
			throw new ResponseException(ErrorType.BadRequestData, "Timerel is between but endTime is empty");
		}

		qp.setTimerel(timerel);
		qp.setTimeAt(timeAt);
		qp.setTimeproperty(timeproperty);
		qp.setEndTimeAt(endTimeAt);
	}

	@SuppressWarnings("unchecked")
	private static void handleGeoQuery(String georel, String geoproperty, String coordinates, String geometry,
			QueryParams qp) throws ResponseException {
		if (georel == null && geoproperty == null && coordinates == null && geometry == null) {
			return;
		}
		if (geoproperty == null) {
			geoproperty = NGSIConstants.QUERY_PARAMETER_DEFAULT_GEOPROPERTY;
		}
		GeoqueryRel geoqueryTokens = QueryParser.parseGeoRel(georel);
		logger.debug("  Geoquery term georelOp: " + geoqueryTokens.getGeorelOp());
		if (geoqueryTokens.getGeorelOp().isEmpty() || geometry == null || geometry.isEmpty() || coordinates == null
				|| coordinates.isEmpty()) {
			throw new ResponseException(ErrorType.BadRequestData,
					"Georel detected but georel, geometry or coordinates are empty!");
		}
		if (!AppConstants.NGB_ALLOWED_GEOM_LIST.contains(geometry.toUpperCase())) {
			throw new ResponseException(ErrorType.BadRequestData, " geometry detected, Bad geometry!" + geometry);
		}
		try {
			validateCoordinates((List<Object>) JsonUtils.fromString(coordinates), geometry);
		} catch (IOException e) {
			throw new ResponseException(ErrorType.BadRequestData, "Failed to parse coordinates");
		}
		GeoqueryRel gr = new GeoqueryRel();
		gr.setGeorelOp(geoqueryTokens.getGeorelOp());
		gr.setDistanceType(geoqueryTokens.getDistanceType());
		gr.setDistanceValue(geoqueryTokens.getDistanceValue());

		qp.setGeorel(gr);
		qp.setGeometry(geometry);
		qp.setCoordinates(coordinates);
		qp.setGeoproperty(geoproperty);
	}
}
