package eu.neclab.ngsildbroker.commons.datatypes;

import java.util.ArrayList;
import java.util.List;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:13:22
 */
public class Property extends BaseProperty {

	
	private String unitCode;

	private List<Object> value;
	public Property(){
		type = "Property";
	}

	public void finalize() throws Throwable {

	}

	public String getUnitCode() {
		return unitCode;
	}

	public void setUnitCode(String unitCode) {
		this.unitCode = unitCode;
	}

	public void setSingleValue(Object value) {
		ArrayList<Object> temp = new ArrayList<Object>();
		temp.add(value);
		setValue(temp);
	}
	
	public List<Object> getValue() {
		return value;
	}

	public void setValue(List<Object> value) {
		this.value = value;
	}
	
	

}