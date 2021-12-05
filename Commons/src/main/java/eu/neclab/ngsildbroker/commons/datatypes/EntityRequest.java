package eu.neclab.ngsildbroker.commons.datatypes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;

public class EntityRequest extends BaseRequest {

	protected int operationType;
	protected String id;
	protected ObjectMapper objectMapper = new ObjectMapper();
	protected String withSysAttrs;
	protected String entityWithoutSysAttrs;
	protected String keyValue;
	protected String operationValue;

	public EntityRequest() {

	}

	public EntityRequest(int operationType, ArrayListMultimap<String, String> headers) {
		super(headers);
		this.operationType = operationType;
	}

	public String getOperationValue() {
		return operationValue;
	}

	public void setOperationValue(String operationValue) {
		this.operationValue = operationValue;
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

	public int getOperationType() {
		return operationType;
	}

	public void setOperationType(int operationType) {
		this.operationType = operationType;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public ObjectMapper getObjectMapper() {
		return objectMapper;
	}

	public void setObjectMapper(ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
	}

	protected Map<String, Object> getKeyValueEntity(Map<String, Object> map) {
		Map<String, Object> kvJsonObject = new HashMap<String, Object>();
		for (Entry<String, Object> entry : map.entrySet()) {
			String key = entry.getKey();
			Object value = entry.getValue();
			if (key.equals(NGSIConstants.JSON_LD_ID) || key.equals(NGSIConstants.JSON_LD_TYPE)) {
				kvJsonObject.put(key, value);
			} else if (value instanceof List) {
				ArrayList<Object> values = new ArrayList<Object>();
				List list = (List) value;
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
							&& ((Map<String, Object>) ((List) map2.get(NGSIConstants.NGSI_LD_HAS_OBJECT)).get(0))
									.containsKey(NGSIConstants.JSON_LD_ID)) {
						values.add(((Map<String, Object>) ((List) map2.get(NGSIConstants.NGSI_LD_HAS_OBJECT)).get(0))
								.get(NGSIConstants.JSON_LD_ID));
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
			if (entry.getValue() instanceof List && !((List) entry.getValue()).isEmpty()) {
				List list = ((List) entry.getValue());
				for (Object entry2 : list) {
					if (entry2 instanceof Map) {
						Map<String, Object> map = (Map<String, Object>) entry2;
						if (map.containsKey(NGSIConstants.JSON_LD_TYPE)
								&& map.get(NGSIConstants.JSON_LD_TYPE) instanceof List
								&& !((List) map.get(NGSIConstants.JSON_LD_TYPE)).isEmpty()
								&& ((List) map.get(NGSIConstants.JSON_LD_TYPE)).get(0).toString()
										.matches(regexNgsildAttributeTypes)) {
							removeTemporalProperties(map);
						}
					}
				}
			}
		}
	}

	/**
	 * this method use for update the value of jsonNode.
	 * 
	 * @param it
	 * @param innerNode
	 * @param jsonToUpdate
	 * @param updateResult
	 * @param i
	 */
	protected void setFieldValue(Iterator<String> it, JsonNode innerNode, JsonNode jsonToUpdate,
			UpdateResult updateResult, int i) {
		while (it.hasNext()) {
			String field = it.next();
			// TOP level updates of context id or type are ignored
			if (field.equalsIgnoreCase(NGSIConstants.JSON_LD_CONTEXT)
					|| field.equalsIgnoreCase(NGSIConstants.JSON_LD_ID)
					|| field.equalsIgnoreCase(NGSIConstants.JSON_LD_TYPE)) {
				continue;
			}
			logger.trace("field: " + field);
			// logger.trace("attrId: " + attrId);
			if (innerNode != null) {
				((ObjectNode) innerNode.get(i)).replace(field, jsonToUpdate.get(field));
				logger.trace("appended json fields (partial): " + updateResult.getAppendedJsonFields().toString());
				updateResult.setStatus(true);
			}
		}
	}
}
