package eu.neclab.ngsildbroker.commons.datatypes;

import java.util.UUID;

public class PropertyEntry extends BaseEntry{
	
	private Object value;
	private String unitCode;

	
	public PropertyEntry(String dataSetId, Object value) {
		super(dataSetId);
		this.value = value;
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
