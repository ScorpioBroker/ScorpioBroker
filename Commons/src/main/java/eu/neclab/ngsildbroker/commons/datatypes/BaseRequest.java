package eu.neclab.ngsildbroker.commons.datatypes;

import java.util.Iterator;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;


public abstract class BaseRequest {

	
	ArrayListMultimap<String, String> headers;
	protected final static Logger logger = LogManager.getLogger(BaseRequest.class);
	
	protected BaseRequest() {
		
	}
	public BaseRequest(ArrayListMultimap<String, String> headers) {
		super();
		this.headers = headers;
	}
	
	public ArrayListMultimap<String, String> getHeaders() {
		return headers;
	}
	public void setHeaders(ArrayListMultimap<String, String> headers) {
		this.headers = headers;
	}
	protected void setTemporalProperties(JsonNode jsonNode, String createdAt, String modifiedAt, boolean rootOnly) {
		if (!jsonNode.isObject()) {
			return;
		}
		ObjectNode objectNode = (ObjectNode) jsonNode;
		if (!createdAt.isEmpty()) {
			objectNode.remove(NGSIConstants.NGSI_LD_CREATED_AT);
			objectNode.putArray(NGSIConstants.NGSI_LD_CREATED_AT).addObject()
					.put(NGSIConstants.JSON_LD_TYPE, NGSIConstants.NGSI_LD_DATE_TIME)
					.put(NGSIConstants.JSON_LD_VALUE, createdAt);
		}
		if (!modifiedAt.isEmpty()) {
			objectNode.remove(NGSIConstants.NGSI_LD_MODIFIED_AT);
			objectNode.putArray(NGSIConstants.NGSI_LD_MODIFIED_AT).addObject()
					.put(NGSIConstants.JSON_LD_TYPE, NGSIConstants.NGSI_LD_DATE_TIME)
					.put(NGSIConstants.JSON_LD_VALUE, modifiedAt);
			
		}
		if (rootOnly) {
			return;
		}
		
		Iterator<Map.Entry<String, JsonNode>> iter = objectNode.fields();
		while (iter.hasNext()) {
			Map.Entry<String, JsonNode> entry = iter.next();
			if (entry.getValue().isArray() && entry.getValue().has(0) && entry.getValue().get(0).isObject()) {
				Iterator<JsonNode> valueIterator = ((ArrayNode) entry.getValue()).iterator();
				while (valueIterator.hasNext()) {
					ObjectNode attrObj = (ObjectNode) valueIterator.next();
					// add createdAt/modifiedAt only to properties, geoproperties and relationships
					if (attrObj.has(NGSIConstants.JSON_LD_TYPE) && attrObj.get(NGSIConstants.JSON_LD_TYPE).isArray()
							&& attrObj.get(NGSIConstants.JSON_LD_TYPE).has(0) && attrObj.get(NGSIConstants.JSON_LD_TYPE)
									.get(0).asText().matches(NGSIConstants.REGEX_NGSI_LD_ATTR_TYPES)) {
						setTemporalProperties(attrObj, createdAt, modifiedAt, rootOnly);
					}
				}
			}
		}
	}

	
}
