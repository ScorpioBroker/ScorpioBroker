package com.github.jsonldjava.core;

public class NGSIObject {
	
	private Object element;
	private boolean isProperty = false;
	private boolean isGeoProperty = false;
	private boolean isRelationship = false;
	private boolean hasAtValue = false;
	private boolean hasAtId = false;
	
	public NGSIObject(Object element) {
		super();
		this.element = element;
	}

	public Object getElement() {
		return element;
	}

	public void setElement(Object element) {
		this.element = element;
	}
	
	

}
