package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;

public class EntityRequest extends BaseRequest {
	protected String withSysAttrs;
	protected String entityWithoutSysAttrs;
	protected String keyValue;

	public EntityRequest() {

	}

	EntityRequest(ArrayListMultimap<String, String> headers, String id, Map<String, Object> requestPayload, int requestType) {
		super(headers, id, requestPayload, requestType);
	}

	public String getWithSysAttrs() {
		return withSysAttrs;
	}

	public void setWithSysAttrs(String withSysAttrs) {
		this.withSysAttrs = withSysAttrs;
	}

	public String getEntityWithoutSysAttrs() {
		return entityWithoutSysAttrs;
	}

	public void setEntityWithoutSysAttrs(String entityWithoutSysAttrs) {
		this.entityWithoutSysAttrs = entityWithoutSysAttrs;
	}

	public String getKeyValue() {
		return keyValue;
	}

	public void setKeyValue(String keyValue) {
		this.keyValue = keyValue;
	}

	@SuppressWarnings("unchecked")
	protected Map<String, Object> getKeyValueEntity(Map<String, Object> map) {
		Map<String, Object> kvJsonObject = new HashMap<String, Object>();
		for (Entry<String, Object> entry : map.entrySet()) {
			String key = entry.getKey();
			Object value = entry.getValue();
			if (key.equals(NGSIConstants.JSON_LD_ID) || key.equals(NGSIConstants.JSON_LD_TYPE)) {
				kvJsonObject.put(key, value);
			} else if (value instanceof List) {
				ArrayList<Object> values = new ArrayList<Object>();
				List<Object> list = (List<Object>) value;
				for (Object entry2 : list) {
					if (!(entry2 instanceof Map)) {
						continue;
					}
					Map<String, Object> map2 = (Map<String, Object>) entry2;
					if (map2.containsKey(NGSIConstants.JSON_LD_VALUE)) { // common members like createdAt do not have
						// hasValue/hasObject
						values.add(value);
					} else if (map2.containsKey(NGSIConstants.NGSI_LD_HAS_VALUE)) {
						values.add(map2.get(NGSIConstants.NGSI_LD_HAS_VALUE));
					} else if (map2.containsKey(NGSIConstants.NGSI_LD_HAS_OBJECT)
							&& map2.get(NGSIConstants.NGSI_LD_HAS_OBJECT) instanceof List
							&& ((Map<String, Object>) ((List<Object>) map2.get(NGSIConstants.NGSI_LD_HAS_OBJECT))
									.get(0)).containsKey(NGSIConstants.JSON_LD_ID)) {
						values.add(((Map<String, Object>) ((List<Object>) map2.get(NGSIConstants.NGSI_LD_HAS_OBJECT))
								.get(0)).get(NGSIConstants.JSON_LD_ID));
					}
				}
				if (values.size() == 1) {
					kvJsonObject.put(key, values.get(0));
				} else {
					kvJsonObject.put(key, values);
				}

			}
		}
		return kvJsonObject;
	}

	@SuppressWarnings("unchecked")
	protected void removeTemporalProperties(Object payload) {
		if (!(payload instanceof Map)) {
			return;
		}
		Map<String, Object> objectNode = (Map<String, Object>) payload;
		objectNode.remove(NGSIConstants.NGSI_LD_CREATED_AT);
		objectNode.remove(NGSIConstants.NGSI_LD_MODIFIED_AT);

		String regexNgsildAttributeTypes = new String(NGSIConstants.NGSI_LD_PROPERTY + "|"
				+ NGSIConstants.NGSI_LD_RELATIONSHIP + "|" + NGSIConstants.NGSI_LD_GEOPROPERTY);
		for (Entry<String, Object> entry : objectNode.entrySet()) {
			if (entry.getValue() instanceof List && !((List<Object>) entry.getValue()).isEmpty()) {
				List<Object> list = ((List<Object>) entry.getValue());
				for (Object entry2 : list) {
					if (entry2 instanceof Map) {
						Map<String, Object> map = (Map<String, Object>) entry2;
						if (map.containsKey(NGSIConstants.JSON_LD_TYPE)
								&& map.get(NGSIConstants.JSON_LD_TYPE) instanceof List
								&& !((List<Object>) map.get(NGSIConstants.JSON_LD_TYPE)).isEmpty()
								&& ((List<Object>) map.get(NGSIConstants.JSON_LD_TYPE)).get(0).toString()
										.matches(regexNgsildAttributeTypes)) {
							removeTemporalProperties(map);
						}
					}
				}
			}
		}
	}
}
