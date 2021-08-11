package eu.neclab.ngsildbroker.queryhandler.controller;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.GeoqueryRel;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.ldcontext.ContextResolverBasic;
import eu.neclab.ngsildbroker.commons.ngsiqueries.ParamsResolver;
import eu.neclab.ngsildbroker.commons.ngsiqueries.QueryParser;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.queryhandler.services.QueryService;

@RestController
@RequestMapping("/ngsi-ld/v1/entityOperations")
public class EntityOperationQueryController {

	@Autowired
	QueryService queryService;

	@Autowired
	@Qualifier("qmparamsResolver")
	ParamsResolver paramsResolver;

	@Autowired
	@Qualifier("qmconRes")
	ContextResolverBasic contextResolver;
	
	@Autowired
	QueryParser queryParser;

	@Autowired
	ObjectMapper objectMapper;

	private HttpUtils httpUtils;

	private final byte[] emptyResult1 = { '{', ' ', '}' };
	private final byte[] emptyResult2 = { '{', '}' };

	private Object defaultContext;

	private JsonLdOptions defaultOptions;
	public static Boolean countResult = false;

	@PostConstruct
	private void setup() {
		httpUtils = HttpUtils.getInstance(contextResolver);
	}

	@PostMapping("/query")
	public ResponseEntity<byte[]> postQuery(HttpServletRequest request, @RequestBody String payload)
			throws ResponseException {
		try {
			String expandedPayload = httpUtils.expandPayload(request, payload, AppConstants.BATCH_URL_ID);
			Map<String, Object> queries = (Map<String, Object>) JsonUtils.fromString(expandedPayload);
			List<Map<String, Object>> entities = null;
			QueryParams paramsTemplate = new QueryParams();
			StringBuilder templateString = new StringBuilder();
			for (Entry<String, Object> entry : queries.entrySet()) {
				switch (entry.getKey()) {
				case NGSIConstants.NGSI_LD_ATTRS:
					List<Map<String, String>> attrs =  (List<Map<String, String>>) entry.getValue();
					StringBuilder builder = new StringBuilder();
					for(Map<String, String> attr: attrs) {
							builder.append(paramsResolver.expandAttribute(attr.get(NGSIConstants.JSON_LD_VALUE), getAtContext()));
							builder.append(',');
					}
					paramsTemplate.setAttrs(builder.substring(0, builder.length() - 1));
					break;
				case NGSIConstants.NGSI_LD_ENTITIES:
					entities = (List<Map<String, Object>>) entry.getValue();
					break;
				case NGSIConstants.NGSI_LD_GEO_QUERY:
					Map<String, Object> geoQuery = (Map<String, Object>) entry.getValue();
					paramsTemplate.setCoordinates(protectGeoProp(geoQuery));
					paramsTemplate.setGeometry((String) getValue(geoQuery.get(NGSIConstants.NGSI_LD_GEOMETRY)));
					paramsTemplate.setGeoproperty((String) getValue(geoQuery.get(NGSIConstants.NGSI_LD_GEOPROPERTY)));
					paramsTemplate.setGeorel(queryParser.parseGeoRel((String) getValue(geoQuery.get(NGSIConstants.NGSI_LD_GEO_REL))));
					break;
				case NGSIConstants.NGSI_LD_QUERY:
					paramsTemplate.setQ(queryParser.parseQuery((String) getValue(entry.getValue()), getAtContext()).toSql(false));
					break;
				case NGSIConstants.JSON_LD_TYPE:

					break;

				default:
					break;
				}
				System.out.println(entry);
			}
//			queryService.getData(qp, rawQueryString, linkHeaders, limit, offset, qToken, showServices, countResult, check, headers)

		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ResponseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return null;
	}

	private String protectGeoProp(Map<String, Object> value) throws ResponseException {
		Object potentialStringValue = value.get(NGSIConstants.JSON_LD_VALUE);
		if (potentialStringValue != null) {
			return (String) potentialStringValue;
		}

		Map<String, Object> compactedFull = JsonLdProcessor.compact(value, defaultContext, defaultOptions);
		compactedFull.remove(NGSIConstants.JSON_LD_CONTEXT);
		String geoType = (String) compactedFull.get(NGSIConstants.GEO_JSON_TYPE);
		// This is needed because one context could map from type which wouldn't work
		// with the used context.
		// Used context is needed because something could map point
		// This is not good but new geo type will come so this can go away at some time
		if (geoType == null) {
			compactedFull = JsonLdProcessor.compact(value, defaultContext, defaultOptions);
			compactedFull.remove(NGSIConstants.JSON_LD_CONTEXT);
			geoType = (String) compactedFull.get(NGSIConstants.GEO_JSON_TYPE);

		}
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
			throw new ResponseException("Failed to handle provided coordinates");
		}
		return protectedValue;
	}

	private List<Object> getAtContext() {
		// TODO Auto-generated method stub
		return null;
	}
	private Object getValue(Object original) {
		if(original instanceof List) {
			original = ((List)original).get(0);
		}
		return ((Map<String,Object>) original).get(NGSIConstants.JSON_LD_VALUE);
		
	}
}
