package eu.neclab.ngsildbroker.entityhandler.controller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.BatchResult;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.ldcontext.ContextResolverBasic;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.entityhandler.services.EntityService;

@RestController
@RequestMapping("/ngsi/v2told/entities")
public class V2Endpoint {

	private static final String SHORTEN_PROPS_PARAM = "shortenprops";
	private static final String OBSERVED_AT_PARAM = "observedat";

	@Autowired
	ObjectMapper objectMapper;

	@Autowired
	EntityService entityService;

	@Autowired
	@Qualifier("emconRes")
	ContextResolverBasic contextResolver;

	private final String URI_PREFIX = "urn:v2told:";
	// private boolean shortenProperties = true;
	private String splitString = "%^&*(";

	// private ArrayList<String> dateTimes = new ArrayList<String>();

	@PostMapping
	public ResponseEntity<byte[]> createEntity(HttpServletRequest request,
			@RequestBody(required = false) String payload) {
		Map<String, String[]> params = request.getParameterMap();
		boolean shortenProps = getShortenProps(params);
		ArrayList<String> observedAtParams = getObservedAtParams(params);
		ArrayListMultimap<String, String> headers = getLDHeadersFromV2Headers(request);
		try {
			String ldPayload = getLdPayloadFromV2(payload, shortenProps, observedAtParams);
			System.out.println("--------------------------------");
			System.out.println(payload);
			System.out.println("--------------------------------");
			System.out.println(ldPayload);
			System.out.println("--------------------------------");
			List<Object> contextLinks = (List) headers.get("Link");
			String ldResolved = contextResolver.expand(ldPayload, contextLinks, true, AppConstants.ENTITIES_URL_ID);
			BatchResult result = entityService.upsertMultipleMessage(headers, ldResolved);
			return ResponseEntity.status(HttpStatus.CREATED).header("location", AppConstants.ENTITES_URL + result)
					.build();
		} catch (ResponseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	private ArrayList<String> getObservedAtParams(Map<String, String[]> params) {
		ArrayList<String> result = new ArrayList<String>();
		if (params.containsKey(OBSERVED_AT_PARAM)) {
			for (String entry : params.get(OBSERVED_AT_PARAM)) {
				for (String param : entry.split(",")) {
					result.add(param);
				}
			}
		}
		return result;
	}

	private boolean getShortenProps(Map<String, String[]> params) {
		if (params.containsKey(SHORTEN_PROPS_PARAM)) {
			if (params.get(SHORTEN_PROPS_PARAM).length == 0) {
				return true;
			}
			String param = params.get(SHORTEN_PROPS_PARAM)[0].strip().toLowerCase();
			if (!param.equals("false") && !param.equals("0")) {
				return true;
			}

		}
		return false;
	}

	/**
	 * { "type": "Store", "id": "urn:ngsi-ld:Store:002", "address": { "type":
	 * "PostalAddress", "value": { "streetAddress": "Friedrichstraße 44",
	 * "addressRegion": "Berlin", "addressLocality": "Kreuzberg", "postalCode":
	 * "10969" }, "metadata": { "verified": { "value": true, "type": "Boolean" } }
	 * }, "location": { "type": "geo:json", "value": { "type": "Point",
	 * "coordinates": [13.3903, 52.5075] } }, "name": { "type": "Text", "value":
	 * "Checkpoint Markt" } }
	 */
	private String getLdPayloadFromV2(String payload, boolean shortenProps, ArrayList<String> observedAtParams)
			throws IOException {
		JsonNode root = objectMapper.readTree(payload);
		String datasetIdPrefix = "urn:v2told:" + root.get("id").asText() + ":";
		ArrayNode temp;
		ArrayNode result = objectMapper.createArrayNode();
		if (!root.isArray()) {
			temp = objectMapper.createArrayNode();
			temp.add(root);
		} else {
			temp = (ArrayNode) root;
		}
		Iterator<JsonNode> it = temp.iterator();
		while (it.hasNext()) {
			JsonNode next = it.next();
			result.add(getLdPayloadFromV2(next, "", datasetIdPrefix, shortenProps, observedAtParams));
		}
		return objectMapper.writeValueAsString(result);
	}

	private JsonNode getLdPayloadFromV2(JsonNode root, String path, String datasetIdPrefix, boolean shortenProps,
			ArrayList<String> observedAtParams) {
		ObjectNode result = objectMapper.createObjectNode();
		Iterator<Entry<String, JsonNode>> it = root.fields();
		while (it.hasNext()) {
			Entry<String, JsonNode> next = it.next();
			JsonNode value = next.getValue();
			String key = next.getKey();
			String newPath = path + splitString + key;
			switch (key) {
			case "id":
				String id = value.asText();
				if (id.indexOf(':') == -1) {
					id = URI_PREFIX + id;
				}
				result.put("id", id);
				break;
			case "type":
				result.set("type", value);
				break;
			case "location":
				JsonNode geoPropValue = getGeoPropValue(key, newPath, value, datasetIdPrefix, shortenProps,
						observedAtParams).getNode();
				result.set("location", geoPropValue);
				break;
			default:
				MyEntry newValue = getRelationship(key, newPath, value, datasetIdPrefix, shortenProps,
						observedAtParams);
				if (newValue == null) {
					newValue = getGeoPropValue(key, newPath, value, datasetIdPrefix, shortenProps, observedAtParams);
				}
				if (newValue == null) {
					newValue = getProperty(key, newPath, value, datasetIdPrefix, shortenProps, observedAtParams);
				}
				/*
				 * if (newValue.isArray()) { Iterator<JsonNode> it2 = newValue.elements(); while
				 * (it2.hasNext()) { JsonNode next2 = it2.next(); String name =
				 * next2.fieldNames().next(); result.set(name, next2.get(name)); } } else {
				 */

				result.set(newValue.getKey(), newValue.getNode());
				// }
				break;
			}

		}
		return result;
	}

	private MyEntry getProperty(String key, String path, JsonNode value, String datasetIdPrefix, boolean shortenProps,
			ArrayList<String> observedAtParams) {
		JsonNode result;
		if (observedAtParams.contains(key)) {
			return new MyEntry(NGSIConstants.QUERY_PARAMETER_OBSERVED_AT, value.get("value"));
		} else {
			result = objectMapper.createObjectNode();
			((ObjectNode) result).put(NGSIConstants.TYPE, NGSIConstants.NGSI_LD_PROPERTY_SHORT);
			if (!shortenProps && value.has(NGSIConstants.TYPE) && value.has(NGSIConstants.VALUE)) {
				ObjectNode temp = objectMapper.createObjectNode();
				temp.set(NGSIConstants.VALUE, value.get(NGSIConstants.VALUE));
				temp.set(NGSIConstants.TYPE, value.get(NGSIConstants.TYPE));
				((ObjectNode) result).set(NGSIConstants.VALUE, temp);
			} else {
				((ObjectNode) result).set(NGSIConstants.VALUE, value.get(NGSIConstants.VALUE));
			}
			Iterator<Entry<String, JsonNode>> it = value.fields();
			while (it.hasNext()) {
				Entry<String, JsonNode> next = it.next();
				String tempKey = next.getKey();
				if (tempKey.equals(NGSIConstants.TYPE) || tempKey.equals(NGSIConstants.VALUE)) {
					continue;
				}
				if (tempKey.equals("metadata")) {
					Iterator<Entry<String, JsonNode>> it2 = next.getValue().fields();
					String newBasePath = path.substring(0, path.length() - "metadata".length());
					while (it2.hasNext()) {
						Entry<String, JsonNode> next2 = it2.next();
						String newKey = next2.getKey();
						String newPath = newBasePath + splitString + newKey;
						ObjectNode temp = objectMapper.createObjectNode();
						temp.set(newKey, next2.getValue());
						JsonNode newValue = getLdPayloadFromV2(temp, newPath, datasetIdPrefix, shortenProps,
								observedAtParams);
						Entry<String, JsonNode> first = newValue.fields().next();
						((ObjectNode) result).set(first.getKey(), first.getValue());
					}
				} else {
					((ObjectNode) result).set(tempKey,
							getLdPayloadFromV2(next.getValue(), path, datasetIdPrefix, shortenProps, observedAtParams));
				}
			}
		}
		return new MyEntry(key, result);
	}

	private MyEntry getRelationship(String key, String path, JsonNode value, String datasetIdPrefix,
			boolean shortenProps, ArrayList<String> observedAtParams) {

		if (key.startsWith("ref") || value.has(NGSIConstants.TYPE)
				&& value.get(NGSIConstants.TYPE).asText().equals("Relationship")) {
			JsonNode relValue = value.get(NGSIConstants.VALUE);
			if (!relValue.isArray() || ((ArrayNode) relValue).size() == 1) {
				ObjectNode result = objectMapper.createObjectNode();
				if (relValue.isArray()) {
					relValue = ((ArrayNode) relValue).get(0);
				}
				String id = relValue.asText();
				if (id.indexOf(':') == -1) {
					id = URI_PREFIX + id;
				}
				result.put(NGSIConstants.TYPE, NGSIConstants.NGSI_LD_RELATIONSHIP_SHORT);
				result.put(NGSIConstants.OBJECT, id);
				Iterator<Entry<String, JsonNode>> it = value.fields();
				while (it.hasNext()) {
					Entry<String, JsonNode> next = it.next();
					String tempKey = next.getKey();
					if (tempKey.equals(NGSIConstants.TYPE) || tempKey.equals(NGSIConstants.VALUE)) {
						continue;
					}
					result.set(tempKey,
							getLdPayloadFromV2(next.getValue(), path, datasetIdPrefix, shortenProps, observedAtParams));
				}
				return new MyEntry(key, result);
			} else {
				ArrayNode resultArray = objectMapper.createArrayNode();
				Iterator<JsonNode> it = ((ArrayNode) relValue).iterator();
				int i = 0;
				while (it.hasNext()) {
					i++;
					String datasetId = datasetIdPrefix + key + ":" + i;
					JsonNode next = it.next();
					if (!next.isNull()) {
						ObjectNode result = objectMapper.createObjectNode();
						if (next.isArray()) {
							next = ((ArrayNode) next).get(0);
						}
						String id = next.asText();
						if (id.indexOf(':') == -1) {
							id = URI_PREFIX + id;
						}
						result.put(NGSIConstants.TYPE, NGSIConstants.NGSI_LD_RELATIONSHIP_SHORT);
						result.put(NGSIConstants.OBJECT, id);
						result.put(NGSIConstants.QUERY_PARAMETER_DATA_SET_ID, datasetId);
						Iterator<Entry<String, JsonNode>> it2 = value.fields();
						while (it2.hasNext()) {
							Entry<String, JsonNode> next2 = it2.next();
							String tempKey = next2.getKey();
							if (tempKey.equals(NGSIConstants.TYPE) || tempKey.equals(NGSIConstants.VALUE)) {
								continue;
							}
							result.set(tempKey, getLdPayloadFromV2(next2.getValue(), path, datasetIdPrefix,
									shortenProps, observedAtParams));
						}
						resultArray.add(result);
					}

				}
				return new MyEntry(key, resultArray);
			}
		}
		return null;
	}

//	"location": { "type": "geo:json", "value": { "type": "Point",
//		 * "coordinates": [13.3903, 52.5075] }
	private MyEntry getGeoPropValue(String key, String path, JsonNode value, String datasetIdPrefix,
			boolean shortenProps, ArrayList<String> observedAtParams) {
		if (value.has(NGSIConstants.VALUE) && value.get(NGSIConstants.VALUE).has(NGSIConstants.GEO_JSON_COORDINATES)
				&& value.get(NGSIConstants.VALUE).has(NGSIConstants.TYPE)
				|| value.has(NGSIConstants.GEO_JSON_COORDINATES) && value.has(NGSIConstants.TYPE)) {
			ObjectNode result = objectMapper.createObjectNode();
			result.put(NGSIConstants.TYPE, NGSIConstants.NGSI_LD_GEOPROPERTY_SHORT);
			if (value.has(NGSIConstants.GEO_JSON_COORDINATES) && value.has(NGSIConstants.TYPE)) {
				result.set(NGSIConstants.VALUE, value);
			} else {
				result.set(NGSIConstants.VALUE, value.get(NGSIConstants.VALUE));
			}
			Iterator<Entry<String, JsonNode>> it = value.fields();
			while (it.hasNext()) {
				Entry<String, JsonNode> next = it.next();
				String tempKey = next.getKey();
				if (tempKey.equals(NGSIConstants.TYPE) || tempKey.equals(NGSIConstants.VALUE)) {
					continue;
				}
				result.set(tempKey,
						getLdPayloadFromV2(next.getValue(), path, datasetIdPrefix, shortenProps, observedAtParams));
			}
			return new MyEntry(key, result);
		}
		return null;
	}

	private ArrayListMultimap<String, String> getLDHeadersFromV2Headers(HttpServletRequest request) {
		ArrayListMultimap<String, String> result = HttpUtils.getHeaders(request);
		if (result.containsKey(NGSIConstants.FIWARE_SERVICE_HEADER)) {
			List<String> tenant = result.removeAll(NGSIConstants.FIWARE_SERVICE_HEADER);
			result.putAll(NGSIConstants.TENANT_HEADER, tenant);
		}
		return result;
	}

	private class MyEntry {

		String key;
		JsonNode node;

		public MyEntry(String key, JsonNode node) {
			super();
			this.key = key;
			this.node = node;
		}

		public String getKey() {
			return key;
		}

		public JsonNode getNode() {
			return node;
		}

	}
}
