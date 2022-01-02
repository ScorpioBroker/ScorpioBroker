package eu.neclab.ngsildbroker.commons.ngsiqueries;

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

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.EntityInfo;
import eu.neclab.ngsildbroker.commons.datatypes.GeoqueryRel;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

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
			if (subscription.getLdQuery() != null && !subscription.getLdQuery().isEmpty()) {
				temp.setQ(subscription.getLdQuery());
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
		int limit = defaultLimit;
		int offset = 0;
		while (it.hasNext()) {
			String queryParameter = it.next();
			String queryValue = multiValueMap.getFirst(queryParameter);
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
				type = String.join(",", expandQueryValues(context, queryValue));
				break;
			case NGSIConstants.QUERY_PARAMETER_ATTRS:
				attrs = expandQueryValues(context, queryValue);
				break;
			case NGSIConstants.QUERY_PARAMETER_GEOMETRY_PROPERTY:
				geometryProperty = expandQueryValues(context, queryValue).iterator().next();
				break;
			case NGSIConstants.QUERY_PARAMETER_GEOREL:
				String georel = queryValue;
				String geometry = "";
				String coordinates = "";
				String geoproperty = "";
				if (multiValueMap.get(NGSIConstants.QUERY_PARAMETER_GEOMETRY) != null)
					geometry = multiValueMap.getFirst(NGSIConstants.QUERY_PARAMETER_GEOMETRY);
				if (multiValueMap.getFirst(NGSIConstants.QUERY_PARAMETER_COORDINATES) != null)
					coordinates = multiValueMap.getFirst(NGSIConstants.QUERY_PARAMETER_COORDINATES);
				if (multiValueMap.get(NGSIConstants.QUERY_PARAMETER_GEOPROPERTY) != null) {
					geoproperty = multiValueMap.getFirst(NGSIConstants.QUERY_PARAMETER_GEOPROPERTY);
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
				if (multiValueMap.get(NGSIConstants.QUERY_PARAMETER_TIME) != null)
					timeAt = multiValueMap.getFirst(NGSIConstants.QUERY_PARAMETER_TIME);
				if (multiValueMap.get(NGSIConstants.QUERY_PARAMETER_TIMEPROPERTY) != null) {
					timeproperty = multiValueMap.getFirst(NGSIConstants.QUERY_PARAMETER_TIMEPROPERTY);
					timeproperty = expandAttribute(timeproperty, context);
				} else {
					timeproperty = NGSIConstants.QUERY_PARAMETER_DEFAULT_TIMEPROPERTY;
				}
				if (multiValueMap.get(NGSIConstants.QUERY_PARAMETER_ENDTIME) != null)
					endTimeAt = multiValueMap.getFirst(NGSIConstants.QUERY_PARAMETER_ENDTIME);

				if (timeAt.isEmpty()) {
					throw new ResponseException(ErrorType.BadRequestData, "Time is empty");
				}
				if (timerel.equals(NGSIConstants.TIME_REL_BETWEEN) && endTimeAt.isEmpty()) {
					throw new ResponseException(ErrorType.BadRequestData, "Timerel is between but endTime is empty");
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
			case NGSIConstants.QUERYP_PARAMETER_COUNT:
				qp.setCountResult(true);

			default:
				throw new ResponseException(ErrorType.BadRequestData, queryParameter + " is unknown");
			}

		}
		if (attrs != null) {
			if (geometryProperty != null) {
				attrs.add(geometryProperty);
				qp.setAttrs(String.join(",", attrs));
			}
		}
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

	private static void validateCoordinates(String coordinates) throws ResponseException {
		if (!coordinates.matches(
				"^\\[*(\\[\\s*[-+]?(180(\\.0+)?|((1[0-7]\\d)|([1-9]?\\d))(\\.\\d+)?)(,\\d)?,[-+]?([1-8]?\\d(\\.\\d+)?|90(\\.0+)?)\\],?)+\\]*$")) {
			throw new ResponseException(ErrorType.BadRequestData, "coordinates are not valid");
		}

	}

	public static HashSet<String> expandQueryValues(Context context, String queryValue) throws ResponseException {
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
}
