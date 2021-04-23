package eu.neclab.ngsildbroker.commons.datatypes;

import java.util.Iterator;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;

public abstract class EntityRequest extends BaseRequest{

	protected int operationType;
	protected String id;
	protected ObjectMapper objectMapper = new ObjectMapper();
	protected String withSysAttrs;
	protected String entityWithoutSysAttrs;
	protected String keyValue;

	public EntityRequest(int operationType, ArrayListMultimap<String, String> headers) {
		super(headers);
		this.operationType = operationType;
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
	
	protected JsonNode getKeyValueEntity(JsonNode json) {
		ObjectNode kvJsonObject = objectMapper.createObjectNode();
		Iterator<Map.Entry<String, JsonNode>> iter = json.fields();
		while (iter.hasNext()) {
			Map.Entry<String, JsonNode> entry = iter.next();
			if (entry.getKey().equals(NGSIConstants.JSON_LD_ID) || entry.getKey().equals(NGSIConstants.JSON_LD_TYPE)) {
				kvJsonObject.set(entry.getKey(), entry.getValue());
			} else if (entry.getValue().isArray()) {
				ArrayNode values = objectMapper.createArrayNode();
				Iterator<JsonNode> it = entry.getValue().elements();
				while (it.hasNext()) {
					ObjectNode attrObj = (ObjectNode) it.next();
					if (attrObj.has(NGSIConstants.JSON_LD_VALUE)) { // common members like createdAt do not have
						// hasValue/hasObject
						values.add(entry.getValue());
					} else if (attrObj.has(NGSIConstants.NGSI_LD_HAS_VALUE)) {
						values.add(attrObj.get(NGSIConstants.NGSI_LD_HAS_VALUE));
					} else if (attrObj.has(NGSIConstants.NGSI_LD_HAS_OBJECT)
							&& attrObj.get(NGSIConstants.NGSI_LD_HAS_OBJECT).isArray()
							&& attrObj.get(NGSIConstants.NGSI_LD_HAS_OBJECT).get(0).has(NGSIConstants.JSON_LD_ID)) {
						values.add(attrObj.get(NGSIConstants.NGSI_LD_HAS_OBJECT).get(0).get(NGSIConstants.JSON_LD_ID));
					}
				}
				if (values.size() == 1) {
					kvJsonObject.set(entry.getKey(), values.get(0));
				} else {
					kvJsonObject.set(entry.getKey(), values);
				}

			}
		}
		return kvJsonObject;
	}

	protected void removeTemporalProperties(JsonNode jsonNode) {
		if (!jsonNode.isObject()) {
			return;
		}
		ObjectNode objectNode = (ObjectNode) jsonNode;
		objectNode.remove(NGSIConstants.NGSI_LD_CREATED_AT);
		objectNode.remove(NGSIConstants.NGSI_LD_MODIFIED_AT);

		String regexNgsildAttributeTypes = new String(NGSIConstants.NGSI_LD_PROPERTY + "|"
				+ NGSIConstants.NGSI_LD_RELATIONSHIP + "|" + NGSIConstants.NGSI_LD_GEOPROPERTY);
		Iterator<Map.Entry<String, JsonNode>> iter = objectNode.fields();
		while (iter.hasNext()) {
			Map.Entry<String, JsonNode> entry = iter.next();
			if (entry.getValue().isArray() && entry.getValue().has(0) && entry.getValue().get(0).isObject()) {
				// ObjectNode attrObj = (ObjectNode) entry.getValue().get(0);
				// add createdAt/modifiedAt only to properties, geoproperties and relationships
				Iterator<JsonNode> valueIterator = ((ArrayNode) entry.getValue()).iterator();
				while (valueIterator.hasNext()) {
					ObjectNode attrObj = (ObjectNode) valueIterator.next();
					if (attrObj.has(NGSIConstants.JSON_LD_TYPE) && attrObj.get(NGSIConstants.JSON_LD_TYPE).isArray()
							&& attrObj.get(NGSIConstants.JSON_LD_TYPE).has(0) && attrObj.get(NGSIConstants.JSON_LD_TYPE)
									.get(0).asText().matches(regexNgsildAttributeTypes)) {
						removeTemporalProperties(attrObj);
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
