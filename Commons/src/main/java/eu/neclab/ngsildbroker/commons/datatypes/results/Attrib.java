package eu.neclab.ngsildbroker.commons.datatypes.results;

import java.util.Map;

import com.google.common.collect.Maps;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

public class Attrib {

	String attribName;
	String datasetId;

	public Attrib(String attribName, String datasetId) {
		super();
		this.attribName = attribName;
		this.datasetId = datasetId;
	}

	public String getAttribName() {
		return attribName;
	}

	public void setAttribName(String attribName) {
		this.attribName = attribName;
	}

	public String getDatasetId() {
		return datasetId;
	}

	public void setDatasetId(String datasetId) {
		this.datasetId = datasetId;
	}

	public Map<String, String> getJson() {
		Map<String, String> result = Maps.newHashMap();
		result.put(NGSIConstants.NGSI_LD_ATTRIBUTE_NAME_SHORT, attribName);
		if (datasetId != null) {
			result.put(NGSIConstants.QUERY_PARAMETER_DATA_SET_ID, datasetId);
		}
		return result;
	}

	public static Attrib fromPayload(Map<String, String> entry) throws ResponseException {
		//TODO error check content
		String attribName = entry.get(NGSIConstants.NGSI_LD_ATTRIBUTE_NAME_SHORT);
		String datasetId = entry.get(NGSIConstants.QUERY_PARAMETER_DATA_SET_ID);
		return new Attrib(attribName, datasetId);
	}

}
