package eu.neclab.ngsildbroker.commons.datatypes;

import java.util.List;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;

public class RelationshipEntry extends BaseEntry {
	private List<String> object;

	public RelationshipEntry(String dataSetId, List<String> object) {
		super(dataSetId);
		this.type = NGSIConstants.NGSI_LD_RELATIONSHIP;
		this.object = object;
	}

	public List<String> getObject() {
		return object;
	}

	public void setObject(List<String> object) {
		this.object = object;
	}

}
