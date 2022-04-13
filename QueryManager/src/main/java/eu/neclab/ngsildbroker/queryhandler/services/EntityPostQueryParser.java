package eu.neclab.ngsildbroker.queryhandler.services;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.PayloadQueryParamParser;
import eu.neclab.ngsildbroker.commons.ngsiqueries.ParamsResolver;
import eu.neclab.ngsildbroker.commons.ngsiqueries.QueryParser;

public class EntityPostQueryParser implements PayloadQueryParamParser {

	private static JsonLdOptions opts = new JsonLdOptions(JsonLdOptions.JSON_LD_1_1);

	@SuppressWarnings("unchecked")
	@Override
	public QueryParams parse(Map<String, Object> queries, Integer limit, Integer offset, int defaultLimit, int maxLimit,
			boolean count, List<String> options, Context context) throws ResponseException {
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
							Object tempItem = ((List<Object>) entry3.getValue()).get(0);
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
			case NGSIConstants.NGSI_LD_SCOPE_Q:
				params.setScopeQ(QueryParser.parseScopeQuery((String) getValue(entry.getValue())).toSql());
				break;
			case NGSIConstants.JSON_LD_TYPE:
				if (entry.getValue() instanceof List) {
					if (((List<String>) entry.getValue()).get(0)
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
		params.setLimit(limit);
		params.setOffSet(offset);
		params.setCountResult(count);
		return params;
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
