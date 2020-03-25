package eu.neclab.ngsildbroker.commons.datatypes;

import java.util.UUID;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;

public class PropertyEntry extends BaseEntry{
	
	private Object value;
	private String unitCode;

	
	public PropertyEntry(String dataSetId, Object value) {
		super(dataSetId);
		this.value = value; 
		this.type = NGSIConstants.NGSI_LD_PROPERTY;
	}
	
	public Object getValue() {
		return value;
	}
	public void setValue(Object value) {
		this.value = value;
	}
	public String getUnitCode() {
		return unitCode;
	}

	public void setUnitCode(String unitCode) {
		this.unitCode = unitCode;
	}

	
	
	
}
