package eu.neclab.ngsildbroker.commons.datatypes;

import java.net.URI;
import java.util.UUID;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;

public class RelationshipEntry extends BaseEntry{
	private URI object;
	
	
	
	public RelationshipEntry(String dataSetId, URI object) {
		super(dataSetId);
		this.type = NGSIConstants.NGSI_LD_RELATIONSHIP;	
		this.object = object;
	}
	
	public URI getObject() {
		return object;
	}
	public void setObject(URI object) {
		this.object = object;
	}
	
	
	

}
