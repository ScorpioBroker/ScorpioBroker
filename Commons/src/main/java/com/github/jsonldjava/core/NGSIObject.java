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

	public NGSIObject duplicateSettings(Object newElement) {
		NGSIObject result = new NGSIObject(newElement);
		result.isProperty = this.isProperty;
		result.isGeoProperty = this.isGeoProperty;
		result.isRelationship = this.isRelationship;
		result.hasAtValue = this.hasAtValue;
		result.hasAtId = this.hasAtId;
		return result;
	}
}
