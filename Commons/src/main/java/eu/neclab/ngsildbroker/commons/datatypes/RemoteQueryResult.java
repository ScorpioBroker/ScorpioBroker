package eu.neclab.ngsildbroker.commons.datatypes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:13:22
 */
public class RemoteQueryResult extends QueryResult {

	private HashMap<String, ObjectNode> id2Data = new HashMap<String, ObjectNode>();
	private ObjectMapper objectMapper = new ObjectMapper();

	public RemoteQueryResult(String errorMsg, ErrorType errorType, int shortErrorMsg, boolean success) {
		super(null, errorMsg, errorType, shortErrorMsg, success);

	}

	public void addData(JsonNode node) {
		if (node.isArray()) {
			Iterator<JsonNode> it = node.elements();
			while (it.hasNext()) {
				addData(it.next());
			}
		} else {
			ObjectNode objNode = (ObjectNode) node;
			String id = objNode.get(NGSIConstants.JSON_LD_ID).asText();
			if (id2Data.containsKey(id)) {
				if (!mergeEntity(id2Data.get(id), objNode)) {
					id2Data.put(id, objNode);
				}
			} else {
				id2Data.put(id, objNode);
			}
		}
	}

	private boolean mergeEntity(ObjectNode oldNode, ObjectNode newNode) {
		if (oldNode.get(NGSIConstants.JSON_LD_TYPE).get(0).asText()
				.equals(newNode.get(NGSIConstants.JSON_LD_TYPE).get(0).asText())) {
			return false;
		}
		Iterator<Entry<String, JsonNode>> it = newNode.fields();
		while (it.hasNext()) {
			Entry<String, JsonNode> next = it.next();
			String attrName = next.getKey();
			JsonNode attrValue = next.getValue();
			if (oldNode.has(attrName)) {
				JsonNode oldAttrValue = oldNode.get(attrName);
				mergeAttributeValue(oldAttrValue, attrValue, oldNode, attrName);
			} else {
				oldNode.set(attrName, attrValue);
			}
		}
		return true;
	}

	private void mergeAttributeValue(JsonNode oldAttrValue, JsonNode attrValue, ObjectNode oldNode, String fieldName) {
		// make everything into arrays for equal treatment
		if (!oldAttrValue.isArray()) {
			ArrayNode temp = objectMapper.createArrayNode();
			temp.add(oldAttrValue);
			oldAttrValue = temp;
		}
		if (!attrValue.isArray()) {
			ArrayNode temp = objectMapper.createArrayNode();
			temp.add(attrValue);
			attrValue = temp;
		}
		Iterator<JsonNode> it = attrValue.iterator();
		while (it.hasNext()) {
			JsonNode next = it.next();
			Iterator<JsonNode> it2 = oldAttrValue.iterator();
			boolean addToOld = true;
			while (it2.hasNext()) {
				JsonNode oldNext = it2.next();
				String oldAttrType = oldNext.get(NGSIConstants.JSON_LD_TYPE).get(0).asText();
				// if types don't match we don't care about other details it's a different value
				// to be added
				if (oldAttrType.equals(next.get(NGSIConstants.JSON_LD_TYPE).get(0).asText())) {
					JsonNode dataset = next.get(NGSIConstants.NGSI_LD_DATA_SET_ID);
					JsonNode oldDataset = oldNext.get(NGSIConstants.NGSI_LD_DATA_SET_ID);
					if ((dataset == null && oldDataset == null)
							|| (dataset != null && oldDataset != null && dataset.get(0).get(NGSIConstants.JSON_LD_ID)
									.asText().equals(oldDataset.get(0).get(NGSIConstants.JSON_LD_ID).asText()))) {
						JsonNode observedAtNew = next.get(NGSIConstants.NGSI_LD_OBSERVED_AT);
						JsonNode observedAtOld = oldNext.get(NGSIConstants.NGSI_LD_OBSERVED_AT);
						JsonNode modifiedAtNew = next.get(NGSIConstants.NGSI_LD_MODIFIED_AT);
						JsonNode modifiedAtOld = oldNext.get(NGSIConstants.NGSI_LD_MODIFIED_AT);
						JsonNode createdAtNew = next.get(NGSIConstants.NGSI_LD_CREATED_AT);
						JsonNode createdAtOld = oldNext.get(NGSIConstants.NGSI_LD_CREATED_AT);
						if (observedAtNew != null || observedAtOld != null) {
							if (compareDates(observedAtOld, observedAtNew)) {
								it2.remove();
							} else {
								addToOld = false;
							}
							break;
						}
						if (modifiedAtNew != null || modifiedAtOld != null) {
							if (compareDates(modifiedAtOld, modifiedAtNew)) {
								it2.remove();
							} else {
								addToOld = false;
							}
							break;
						}
						if (createdAtNew != null || createdAtOld != null) {
							if (compareDates(createdAtOld, createdAtNew)) {
								it2.remove();
							} else {
								addToOld = false;
							}
							break;
						}
						it2.remove();
						break;
					}
				}
			}
			if (addToOld) {
				((ArrayNode) oldAttrValue).add(next);
			}
		}
	}

	private boolean compareDates(JsonNode oldDate, JsonNode newDate) {
		String oldToComp, newToComp;
		if (oldDate == null) {
			oldToComp = "";
		} else {
			oldToComp = oldDate.get(0).get(NGSIConstants.JSON_LD_VALUE).asText();
		}
		if (newDate == null) {
			newToComp = "";
		} else {
			newToComp = newDate.get(0).get(NGSIConstants.JSON_LD_VALUE).asText();
		}
		if (newToComp.compareTo(oldToComp) > 0) {
			return true;
		}
		return false;
	}

	public HashMap<String, ObjectNode> getId2Data() {
		return id2Data;
	}

	public void setId2Data(HashMap<String, ObjectNode> id2Data) {
		this.id2Data = id2Data;
	}

	@Override
	public List<String> getActualDataString() {
		ArrayList<String> result = new ArrayList<String>();
		for (ObjectNode entry : id2Data.values()) {
			try {
				result.add(objectMapper.writeValueAsString(entry));
			} catch (JsonProcessingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		// TODO Auto-generated method stub
		return result;
	}

	@Override
	public List<String> getDataString() {
		return this.getActualDataString();
	}

	public void finalize() throws Throwable {

	}
}