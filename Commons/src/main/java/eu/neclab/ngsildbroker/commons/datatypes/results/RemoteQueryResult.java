package eu.neclab.ngsildbroker.commons.datatypes.results;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.jsonldjava.utils.JsonUtils;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:13:22
 */
public class RemoteQueryResult extends QueryResult {

	private static final Logger logger = LoggerFactory.getLogger(RemoteQueryResult.class);
	private HashMap<String, Map<String, Object>> id2Data = new HashMap<String, Map<String, Object>>();

	private ArrayList<String> DO_NOT_MERGE = new ArrayList<String>();

	public RemoteQueryResult(String errorMsg, ErrorType errorType, int shortErrorMsg, boolean success) {
		super(null, errorMsg, errorType, shortErrorMsg, success);
		DO_NOT_MERGE.add(NGSIConstants.JSON_LD_ID);
		DO_NOT_MERGE.add(NGSIConstants.JSON_LD_TYPE);
		DO_NOT_MERGE.add(NGSIConstants.NGSI_LD_CREATED_AT);
		DO_NOT_MERGE.add(NGSIConstants.NGSI_LD_OBSERVED_AT);
		DO_NOT_MERGE.add(NGSIConstants.NGSI_LD_MODIFIED_AT);
	}

	@SuppressWarnings("unchecked")
	public void addData(Object node) {
		if (node instanceof List) {
			List<Object> list = (List<Object>) node;
			for (Object obj : list) {
				addData(obj);
			}
		} else {
			Map<String, Object> objNode = (Map<String, Object>) node;
			String id = (String) objNode.get(NGSIConstants.JSON_LD_ID);
			if (id2Data.containsKey(id)) {
				if (!mergeEntity(id2Data.get(id), objNode)) {
					id2Data.put(id, objNode);
				}
			} else {
				id2Data.put(id, objNode);
			}
		}
	}

	@SuppressWarnings("unchecked")
	private boolean mergeEntity(Map<String, Object> oldNode, Map<String, Object> newNode) {
		if (!((List<String>) oldNode.get(NGSIConstants.JSON_LD_TYPE)).get(0)
				.equals(((List<String>) newNode.get(NGSIConstants.JSON_LD_TYPE)).get(0))) {
			return false;
		}
		Iterator<Entry<String, Object>> it = newNode.entrySet().iterator();
		while (it.hasNext()) {
			Entry<String, Object> next = it.next();
			String attrName = next.getKey();
			Object attrValue = next.getValue();
			if (oldNode.containsKey(attrName)) {
				Object oldAttrValue = oldNode.get(attrName);
				mergeAttributeValue(oldAttrValue, attrValue, oldNode, attrName);
			} else {
				oldNode.put(attrName, attrValue);
			}
		}
		return true;
	}

	@SuppressWarnings("unchecked")
	private void mergeAttributeValue(Object oldAttrValue, Object attrValue, Map<String, Object> oldNode,
			String fieldName) {
		// make everything into arrays for equal treatment

		if (DO_NOT_MERGE.contains(fieldName)) {
			return;
		}
		if (!(oldAttrValue instanceof List)) {
			ArrayList<Object> temp = new ArrayList<Object>();
			temp.add(oldAttrValue);
			oldAttrValue = temp;
		}
		if (!(attrValue instanceof List)) {
			ArrayList<Object> temp = new ArrayList<Object>();
			temp.add(attrValue);
			attrValue = temp;
		}
		Iterator<Map<String, Object>> it = ((List<Map<String, Object>>) attrValue).iterator();
		while (it.hasNext()) {
			Map<String, Object> next = it.next();
			Iterator<Map<String, Object>> it2 = ((List<Map<String, Object>>) oldAttrValue).iterator();
			boolean addToOld = true;
			while (it2.hasNext()) {
				Map<String, Object> oldNext = it2.next();
				String oldAttrType = ((List<String>) oldNext.get(NGSIConstants.JSON_LD_TYPE)).get(0);
				// if types don't match we don't care about other details it's a different value
				// to be added
				if (oldAttrType.equals(((List<String>) next.get(NGSIConstants.JSON_LD_TYPE)).get(0))) {
					Object dataset = next.get(NGSIConstants.NGSI_LD_DATA_SET_ID);
					Object oldDataset = oldNext.get(NGSIConstants.NGSI_LD_DATA_SET_ID);
					if ((dataset == null && oldDataset == null) || (dataset != null && oldDataset != null
							&& ((List<Map<String, Object>>) dataset).get(0).get(NGSIConstants.JSON_LD_ID).equals(
									((List<Map<String, Object>>) oldDataset).get(0).get(NGSIConstants.JSON_LD_ID)))) {
						Object observedAtNew = next.get(NGSIConstants.NGSI_LD_OBSERVED_AT);
						Object observedAtOld = oldNext.get(NGSIConstants.NGSI_LD_OBSERVED_AT);
						Object modifiedAtNew = next.get(NGSIConstants.NGSI_LD_MODIFIED_AT);
						Object modifiedAtOld = oldNext.get(NGSIConstants.NGSI_LD_MODIFIED_AT);
						Object createdAtNew = next.get(NGSIConstants.NGSI_LD_CREATED_AT);
						Object createdAtOld = oldNext.get(NGSIConstants.NGSI_LD_CREATED_AT);
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
				((List<Object>) oldAttrValue).add(next);
			}
		}
	}

	@SuppressWarnings("unchecked")
	private boolean compareDates(Object oldDate, Object newDate) {
		String oldToComp, newToComp;
		if (oldDate == null) {
			oldToComp = "";
		} else {
			oldToComp = (String) ((List<Map<String, Object>>) oldDate).get(0).get(NGSIConstants.JSON_LD_VALUE);
		}
		if (newDate == null) {
			newToComp = "";
		} else {
			newToComp = (String) ((List<Map<String, Object>>) newDate).get(0).get(NGSIConstants.JSON_LD_VALUE);
		}
		if (newToComp.compareTo(oldToComp) > 0) {
			return true;
		}
		return false;
	}

	public HashMap<String, Map<String, Object>> getId2Data() {
		return id2Data;
	}

	public void setId2Data(HashMap<String, Map<String, Object>> id2Data) {
		this.id2Data = id2Data;
	}

	@Override
	public List<String> getActualDataString() {
		ArrayList<String> result = new ArrayList<String>();
		for (Map<String, Object> entry : id2Data.values()) {
			try {
				result.add(JsonUtils.toPrettyString(entry));
			} catch (IOException e) {
				logger.error("Failed to generate data output");
			}
		}
		return result;
	}

	@Override
	public List<String> getDataString() {
		return this.getActualDataString();
	}

	public void finalize() throws Throwable {

	}
}