package eu.neclab.ngsildbroker.queryhandler.controller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
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

	@Value("${defaultLimit}")
	int defaultLimit = 50;
	@Value("${maxLimit}")
	int maxLimit = 1000;

	@Autowired
	@Qualifier("qmconRes")
	ContextResolverBasic contextResolver;

	@Autowired
	QueryParser queryParser;

	@Autowired
	ObjectMapper objectMapper;

	private final static Logger logger = LoggerFactory.getLogger(EntityOperationQueryController.class);

	private HttpUtils httpUtils;

	private Object defaultContext = "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld";

	private JsonLdOptions defaultOptions = new JsonLdOptions();

	@PostConstruct
	private void setup() {
		httpUtils = HttpUtils.getInstance(contextResolver);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	// these are known structures in try catch. failed parsing would rightfully
	// result in an error
	@PostMapping("/query")
	public ResponseEntity<byte[]> postQuery(HttpServletRequest request, @RequestBody String payload,
			@RequestParam(value = "limit", required = false) Integer limit,
			@RequestParam(value = "offset", required = false) Integer offset,
			@RequestParam(value = "qtoken", required = false) String qToken,
			@RequestParam(name = "options", required = false) List<String> options,
			@RequestParam(value = "count", required = false, defaultValue = "false") boolean count)
			throws ResponseException {
		try {
			HttpUtils.doPreflightCheck(request, payload);
			Map<String, Object> rawPayload = (Map<String, Object>) JsonUtils.fromString(payload);
			String expandedPayload = httpUtils.expandPayload(request, payload, AppConstants.BATCH_URL_ID);
			Map<String, Object> queries = (Map<String, Object>) JsonUtils.fromString(expandedPayload);
			List<Object> linkHeaders = HttpUtils.parseLinkHeader(request, NGSIConstants.HEADER_REL_LDCONTEXT);
			if (rawPayload.containsKey(NGSIConstants.JSON_LD_CONTEXT)) {
				linkHeaders.add(rawPayload.get(NGSIConstants.JSON_LD_CONTEXT));
			}
			QueryParams params = new QueryParams();
			if (limit == null) {
				limit = defaultLimit;
			}
			if (offset == null) {
				offset = 0;
			}
			for (Entry<String, Object> entry : queries.entrySet()) {
				switch (entry.getKey()) {
				case NGSIConstants.NGSI_LD_ATTRS:
					List<Map<String, String>> attrs = (List<Map<String, String>>) entry.getValue();
					StringBuilder builder = new StringBuilder();
					for (Map<String, String> attr : attrs) {
						builder.append(
								paramsResolver.expandAttribute(attr.get(NGSIConstants.JSON_LD_VALUE), linkHeaders));
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
							queryParser.parseGeoRel((String) getValue(geoQuery.get(NGSIConstants.NGSI_LD_GEO_REL))));
					break;
				case NGSIConstants.NGSI_LD_QUERY:
					params.setQ(queryParser.parseQuery((String) getValue(entry.getValue()), linkHeaders).toSql(false));
					break;
				case NGSIConstants.JSON_LD_TYPE:
					if (!entry.getValue().toString().equals(NGSIConstants.QUERY_TYPE)) {
						throw new ResponseException(ErrorType.BadRequestData,
								"Type has to be Query for this operation");
					}
					break;

				default:
					throw new ResponseException(ErrorType.BadRequestData, entry.getKey() + " is an unknown entry");
				}
			}
			return QueryController.generateReply(httpUtils, request, queryService.getData(params, payload, linkHeaders,
					limit, offset, qToken, false, count, HttpUtils.getHeaders(request), true), true, count);

		} catch (IOException e) {
			logger.error("Failed to parse request data", e);
			throw new ResponseException(ErrorType.BadRequestData, "Failed to parse request data\n" + e.getMessage());
		}
	}

	private String protectGeoProp(Map<String, Object> value) throws ResponseException {
		Object potentialStringValue = value.get(NGSIConstants.JSON_LD_VALUE);
		if (potentialStringValue != null) {
			return (String) potentialStringValue;
		}

		Map<String, Object> compactedFull = JsonLdProcessor.compact(value, defaultContext, defaultOptions);
		compactedFull.remove(NGSIConstants.JSON_LD_CONTEXT);
		String geoType = (String) compactedFull.get(NGSIConstants.QUERY_PARAMETER_GEOMETRY);
		// This is needed because one context could map from type which wouldn't work
		// with the used context.
		// Used context is needed because something could map point
		// This is not good but new geo type will come so this can go away at some time
		if (geoType == null) {
			compactedFull = JsonLdProcessor.compact(value, defaultContext, defaultOptions);
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
			throw new ResponseException("Failed to handle provided coordinates");
		}
		return protectedValue;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	// known structure from json ld lib
	private Object getValue(Object original) {
		if (original instanceof List) {
			original = ((List) original).get(0);
		}
		return ((Map<String, Object>) original).get(NGSIConstants.JSON_LD_VALUE);

	}
}
