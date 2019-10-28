package eu.neclab.ngsildbroker.commons.ldcontext;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParseException;
import com.github.jsonldjava.core.JsonLdError;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;
import eu.neclab.ngsildbroker.commons.stream.service.KafkaOps;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;

@Component
public class ContextResolverBasic {

	private URI CORE_CONTEXT_URL;
	@Value("${context.coreurl:https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld}")
	private String CORE_CONTEXT_URL_STR;
	// private URI DEFAULT_CONTEXT_URL;

	@Autowired
	KafkaOps kafkaOps;

	@Autowired
	AtContextProducerChannel producerChannel;

	private JsonLdOptions defaultOptions = new JsonLdOptions();

	private String AT_CONTEXT_BASE_URL = "http://localhost:9090/ngsi-ld/atcontext/";
	private HttpUtils httpUtils = HttpUtils.getInstance(this);
	private Map<String, Object> CORE_CONTEXT;
	// private Map<String, Object> DEFAULT_CONTEXT;
	private Map<String, Object> BASE_CONTEXT = new HashMap<String, Object>();

	private static final String IS_FULL_VALID = "ajksd7868";

	@PostConstruct
	private void setup() {
		try {
			CORE_CONTEXT_URL = new URI(CORE_CONTEXT_URL_STR);

			String json = httpUtils.doGet(CORE_CONTEXT_URL);
			CORE_CONTEXT = (Map<String, Object>) ((Map) JsonUtils.fromString(json)).get("@context");
			// DEFAULT_CONTEXT_URL = new URI(
			// "https://forge.etsi.org/gitlab/NGSI-LD/NGSI-LD/raw/master/defaultContext/defaultContextVocab.jsonld");
			// json = httpUtils.doGet(DEFAULT_CONTEXT_URL);
			// DEFAULT_CONTEXT = (Map<String, Object>) ((Map)
			// JsonUtils.fromString(json)).get("@context");
			BASE_CONTEXT.putAll(CORE_CONTEXT);
			// BASE_CONTEXT.putAll(DEFAULT_CONTEXT);
		} catch (URISyntaxException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws Exception {
		ContextResolverBasic bla = new ContextResolverBasic();
		List<Object> contextLinks = null;
		String body = "{\r\n" + "  \"@context\":[\r\n" + "    {\r\n"
				+ "      \"Vehicle\":\"http://example.org/vehicle/Vehicle\",\r\n" +
				// " \"brandName\":\"http://example.org/vehicle/brandName\",\r\n" +
				"      \"speed\":\"http://example.org/vehicle/speed\",\r\n" + "      \"isParked\":{\r\n"
				+ "        \"@type\":\"@id\",\r\n" + "        \"@id\":\"http://example.org/common/isParked\"\r\n"
				+ "      },\r\n" + "      \"providedBy\": {\r\n" + "        \"@type\": \"@id\",\r\n"
				+ "        \"@id\": \"http://example.org/common/providedBy\"\r\n" + "      }    \r\n" + "    }\r\n"
				+ "  ],\r\n" + "  \"id\":\"urn:ngsi-ld:Vehicle:A4583\",\r\n" + "  \"type\":\"Vehicle\",\r\n"
				+ "  \"brandName\":{\r\n" + "    \"type\":\"Property\",\r\n" + "    \"value\":\"Mercedes\"\r\n"
				+ "  },\r\n" + "  \"isParked\":{\r\n" + "    \"type\":\"Relationship\",\r\n"
				+ "    \"object\":\"urn:ngsi-ld:OffStreetParking:Downtown1\",\r\n"
				+ "    \"observedAt\":\"2017-07-29T12:00:04\",\r\n" + "    \"providedBy\":{\r\n"
				+ "      \"type\":\"Relationship\",\r\n" + "      \"object\":\"urn:ngsi-ld:Person:Bob\"\r\n"
				+ "    }\r\n" + "  }\r\n" + "}";
		System.out.println(bla.expand(body, contextLinks));
	}

	public ContextResolverBasic(String atContextBaseUrl) {
		if (atContextBaseUrl != null) {
			this.AT_CONTEXT_BASE_URL = atContextBaseUrl;
		}

	}

	public ContextResolverBasic() {

	}

	public String expand(String body, List<Object> contextLinks) throws ResponseException {
		try {
			Map<String, Object> json = (Map<String, Object>) JsonUtils.fromString(body);
			Object tempCtx = json.get(NGSIConstants.JSON_LD_CONTEXT);
			List<Object> context;
			if (tempCtx == null) {
				context = new ArrayList<Object>();
			} else if (tempCtx instanceof List) {
				context = (List<Object>) tempCtx;
			} else {
				context = new ArrayList<Object>();
				context.add(tempCtx);
			}

			if (contextLinks != null && !contextLinks.isEmpty()) {
				context.addAll(contextLinks);
			}
			Map<String, Object> fullContext = getFullContext(context);
			validateAndCleanContext(fullContext);
			fullContext.remove(IS_FULL_VALID);
			ArrayList<Object> usedContext = new ArrayList<Object>();
			usedContext.add(fullContext);
			usedContext.add(BASE_CONTEXT);

			json.put(NGSIConstants.JSON_LD_CONTEXT, usedContext);
			List<Object> expanded = JsonLdProcessor.expand(json);
			protectGeoProps(expanded, usedContext);
			return JsonUtils.toPrettyString(expanded.get(0));
		} catch (IOException e) {
			e.printStackTrace();
			throw new ResponseException(ErrorType.InvalidRequest);
		}

	}

	private void protectGeoProps(List<Object> expanded, ArrayList<Object> usedContext)
			throws JsonGenerationException, JsonLdError, IOException {
		for (Object entry : expanded) {
			if (entry instanceof Map) {

				protectGeoProps((Map<String, Object>) entry, usedContext);
			} else if (entry instanceof List) {
				protectGeoProps((List) entry, usedContext);
			} else {
				// don't care for now i think
			}
		}

	}

	private void protectGeoProps(Map<String, Object> objMap, ArrayList<Object> usedContext)
			throws JsonGenerationException, JsonLdError, IOException {
		boolean typeFound = false;
		Object value = null;
		for (Entry<String, Object> mapEntry : objMap.entrySet()) {
			String key = mapEntry.getKey();
			Object mapValue = mapEntry.getValue();
			if (NGSIConstants.JSON_LD_TYPE.equals(key) && !(mapValue instanceof String)
					&& NGSIConstants.NGSI_LD_GEOPROPERTY.equals(((List) mapValue).get(0))) {
				typeFound = true;
			} else if (NGSIConstants.NGSI_LD_HAS_VALUE.equals(key)) {
				value = ((List) mapValue).get(0);
			} else {
				if (mapValue instanceof Map) {
					protectGeoProps((Map<String, Object>) mapValue, usedContext);
				} else if (mapValue instanceof List) {
					protectGeoProps((List) mapValue, usedContext);
				}
			}
		}
		if (typeFound && value != null) {
			Object potentialStringValue = ((Map)value).get(NGSIConstants.JSON_LD_VALUE);
			if(potentialStringValue != null) {
				return;
			}
			Map<String, Object> compactedFull = JsonLdProcessor.compact(value, usedContext, defaultOptions);
			compactedFull.remove(NGSIConstants.JSON_LD_CONTEXT);
			String proctedValue = JsonUtils.toString(compactedFull);
			// temp.replace("\"", "\\\"");
			ArrayList<Object> tempList = new ArrayList<Object>();
			Map<String, Object> tempMap = new HashMap<String, Object>();
			tempMap.put(NGSIConstants.JSON_LD_VALUE, proctedValue);
			tempList.add(tempMap);
			objMap.put(NGSIConstants.NGSI_LD_HAS_VALUE, tempList);
		}

	}

	private void unprotectGeoProps(Object json, List<Object> fullContext) throws JsonParseException, IOException {
		if (json instanceof Map) {
			unprotectGeoProps((Map<String, Object>) json, fullContext);
		} else if (json instanceof List) {
			unprotectGeoProps((List) json, fullContext);
		}

	}

	private void unprotectGeoProps(Map<String, Object> objMap, List<Object> usedContext)
			throws JsonParseException, IOException {
		boolean typeFound = false;
		Object value = null;
		for (Entry<String, Object> mapEntry : objMap.entrySet()) {
			String key = mapEntry.getKey();
			Object mapValue = mapEntry.getValue();
			if (NGSIConstants.JSON_LD_TYPE.equals(key)
					&& (mapValue instanceof List)) {
				Object tempObj = ((List) mapValue).get(0);
				if(NGSIConstants.NGSI_LD_GEOPROPERTY.equals(tempObj)){
					typeFound = true;
				}
//				if(tempObj instanceof Map) {
//					if(NGSIConstants.NGSI_LD_GEOPROPERTY.equals(((Map)tempObj).get(NGSIConstants.JSON_LD_VALUE))){
//						typeFound = true;
//					}
//				}
				
				
			} else if (NGSIConstants.NGSI_LD_HAS_VALUE.equals(key)) {
				value = mapValue;
			} else {
				if (mapValue instanceof Map) {
					unprotectGeoProps((Map<String, Object>) mapValue, usedContext);
				} else if (mapValue instanceof List) {
					unprotectGeoProps((List) mapValue, usedContext);
				}
			}
		}
		if(typeFound) {
			System.out.println("mybreak");
		}
		
		if (typeFound && value != null && ((Map)(((List) value).get(0))).get(NGSIConstants.JSON_LD_VALUE) instanceof String) {
			Map<String, Object> temp = (Map<String, Object>) JsonUtils.fromString((String) (((Map)(((List) value).get(0))).get(NGSIConstants.JSON_LD_VALUE)));
			temp.put(NGSIConstants.JSON_LD_CONTEXT, usedContext);
			
			
//			temp.replace("\\\"", "\"");
			objMap.put(NGSIConstants.NGSI_LD_HAS_VALUE, JsonLdProcessor.expand(temp));
			System.out.println();
		}

	}

	private void unprotectGeoProps(List<Object> objList, List<Object> usedContext)
			throws JsonParseException, IOException {
		for (Object entry : objList) {
			if (entry instanceof Map) {

				unprotectGeoProps((Map<String, Object>) entry, usedContext);
			} else if (entry instanceof List) {
				unprotectGeoProps((List) entry, usedContext);
			} else {
				// don't care for now i think
			}
		}

	}

	public CompactedJson compact(String body, List<Object> contextLinks) throws ResponseException {
		try {
			Object json = JsonUtils.fromString(body);
			Map<String, Object> context = new HashMap<String, Object>();
			for (Object url : contextLinks) {
				context.putAll(getRemoteContext((String) url));
			}
			return compact(json, context, contextLinks);
		} catch (JsonParseException e) {
			e.printStackTrace();
			throw new ResponseException(ErrorType.InvalidRequest);
		} catch (IOException e) {
			e.printStackTrace();
			throw new ResponseException(ErrorType.InvalidRequest);
		}

	}

	public CompactedJson compact(String body) throws ResponseException {
		try {
			// This should anyway never happen as we only compact for output an there will
			// be no @context in that
			// Object json = JsonUtils.fromString(body);
			// List<Object> context;
			// context = (List<Object>) json.getOrDefault(NGSIConstants.JSON_LD_CONTEXT, new
			// ArrayList<Object>());
			// Map<String, Object> fullContext = getFullContext(context);
			// json.remove(NGSIConstants.JSON_LD_CONTEXT);
			return compact(JsonUtils.fromString(body), null, null);
		} catch (JsonParseException e) {
			e.printStackTrace();
			throw new ResponseException(ErrorType.InvalidRequest);
		} catch (IOException e) {
			e.printStackTrace();
			throw new ResponseException(ErrorType.InvalidRequest);
		}
	}

	private CompactedJson compact(Object json, Map<String, Object> context, List<Object> rawContext)
			throws ResponseException {
		validateAndCleanContext(context);
		List<Object> fullContext = new ArrayList<Object>();
		if (context != null && !context.isEmpty()) {
			fullContext.add(context);
		}
		fullContext.add(BASE_CONTEXT);
		CompactedJson result = new CompactedJson();
		int hash = json.hashCode();
		if (context.containsKey(IS_FULL_VALID)) {
			result.setContextUrl((String) rawContext.get(0));
		} else {
			result.setContextUrl(generateAtContextServing(rawContext, hash));
		}
		context.remove(IS_FULL_VALID);
		try {
			unprotectGeoProps(json, fullContext);
			Map<String, Object> tempResult = JsonLdProcessor.compact(json, fullContext, defaultOptions);

			if (tempResult.containsKey("@graph")) {
				// we are in a multiresult set
				Object atContext = tempResult.get("@context");
				List<Map<String, Object>> toCompact = (List<Map<String, Object>>) tempResult.get("@graph");
				result.setCompacted(JsonUtils.toPrettyString(toCompact));
				for (Map<String, Object> entry : toCompact) {
					entry.put("@context", atContext);
				}
				result.setCompactedWithContext(JsonUtils.toPrettyString(toCompact));
			} else {

				result.setCompactedWithContext(JsonUtils.toPrettyString(tempResult));
				tempResult.remove(NGSIConstants.JSON_LD_CONTEXT);
				result.setCompacted(JsonUtils.toPrettyString(tempResult));
			}

		} catch (IOException e) {
			e.printStackTrace();
			throw new ResponseException(ErrorType.InvalidRequest);
		}
		return result;
	}

	private String generateAtContextServing(List<Object> rawContext, int hash) {
		ArrayList<Object> sorted = new ArrayList<Object>();
		if (rawContext != null && !rawContext.isEmpty()) {
			sorted.addAll(rawContext);
		}
		// sorted.add(DEFAULT_CONTEXT_URL);
		sorted.add(CORE_CONTEXT_URL);
		kafkaOps.pushToKafka(producerChannel.atContextWriteChannel(), (hash + "").getBytes(),
				DataSerializer.toJson(sorted).getBytes());
		return AT_CONTEXT_BASE_URL + hash;
	}

	private Map<String, Object> getFullContext(Object context) throws ResponseException {
		Map<String, Object> result = new HashMap<String, Object>();
		if (context instanceof String) {
			// just another url
			String temp = (String) context;
			if (temp.equals(CORE_CONTEXT_URL_STR)) {
				result.put(IS_FULL_VALID, true);
			}
			result.putAll(getRemoteContext(temp));
		} else if (context instanceof List) {
			for (Object entry : (List) context) {
				if (entry instanceof String) {
					// just another url
					String temp = (String) entry;
					if (temp.equals(CORE_CONTEXT_URL_STR)) {
						result.put(IS_FULL_VALID, true);
					}

					result.putAll(getRemoteContext(entry.toString()));
				} else if (entry instanceof Map) {
					result.putAll(((Map) entry));
				} else if (entry instanceof List) {
					result.putAll(getFullContext(entry));
				} else {
					// Everything else should be illegal for @context
					throw new ResponseException(ErrorType.BadRequestData, "Illegal state of @context");
				}
			}
		} else if (context instanceof Map) {
			// @context entries, straight key value pairs
			result.putAll(((Map) context));
		} else {
			// Everything else should be illegal for @context
			throw new ResponseException(ErrorType.BadRequestData, "Illegal state of @context");
		}
		return result;
	}

	private Map<String, Object> getRemoteContext(String url) throws ResponseException {
		try {
			String body = httpUtils.doGet(new URI(url));
			Map<String, Object> remoteContext = (Map<String, Object>) JsonUtils.fromString(body);
			Object temp = remoteContext.get(NGSIConstants.JSON_LD_CONTEXT);
			if (temp == null) {
				throw new ResponseException(ErrorType.BadRequestData, "Failed to get remote @context from " + url);
			}
			return getFullContext(temp);

		} catch (IOException | URISyntaxException e) {
			throw new ResponseException(ErrorType.BadRequestData, "Failed to get remote @context from " + url);
		}

	}

	private void validateAndCleanContext(Map<String, Object> contextToMerge) throws ResponseException {
		if (contextToMerge == null) {
			return;
		}
		Iterator<Entry<String, Object>> it = contextToMerge.entrySet().iterator();

		while (it.hasNext()) {
			Entry<String, Object> next = it.next();
			String key = next.getKey();
			Object value = next.getValue();
			if (BASE_CONTEXT.containsKey(key)) {
				if (!value.equals(BASE_CONTEXT.get(key))) {
					// Attemp to overwrite default context
					throw new ResponseException(ErrorType.BadRequestData,
							"Provided context entry " + key + "=" + value.toString() + " overrides base context");
				}
				it.remove();
				continue;
			}

		}
	}

}
