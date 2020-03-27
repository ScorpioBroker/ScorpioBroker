package eu.neclab.ngsildbroker.commons.datatypes;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:13:22
 */
public class Query extends BaseOperation {


	public Query() {
		super();
	}
	
	public Query(Map<String, String> customFlags, List<String> attributeNames, List<EntityInfo> entities,
			String ldContext, LDGeoQuery ldGeoQuery, String ldQuery, LDTemporalQuery ldTempQuery,
			List<URI> requestorList) {
		super(customFlags);
		this.attributeNames = attributeNames;
		if(this.attributeNames == null) {
			this.attributeNames = new ArrayList<String>();
		}
			
		this.entities = entities;
		this.ldContext = ldContext;
		this.ldGeoQuery = ldGeoQuery;
		this.ldQuery = ldQuery;
		this.ldTempQuery = ldTempQuery;
		this.requestorList = requestorList;
	}


	protected List<String> attributeNames;
	protected List<EntityInfo> entities = new ArrayList<EntityInfo>();
	protected String ldContext;
	protected LDGeoQuery ldGeoQuery;
	protected String ldQuery;
	protected LDTemporalQuery ldTempQuery;
	protected List<URI> requestorList;

	
	
	public List<String> getAttributeNames() {
		return attributeNames;
	}



	public void setAttributeNames(List<String> attributeNames) {
		this.attributeNames = attributeNames;
	}



	public List<EntityInfo> getEntities() {
		return entities;
	}



	public void setEntities(List<EntityInfo> entities) {
		this.entities = entities;
	}



	public String getLdContext() {
		return ldContext;
	}



	public void setLdContext(String ldContext) {
		this.ldContext = ldContext;
	}



	public LDGeoQuery getLdGeoQuery() {
		return ldGeoQuery;
	}



	public void setLdGeoQuery(LDGeoQuery ldGeoQuery) {
		this.ldGeoQuery = ldGeoQuery;
	}



	public String getLdQuery() {
		return ldQuery;
	}



	public void setLdQuery(String ldQuery) {
		this.ldQuery = ldQuery;
	}



	public LDTemporalQuery getLdTempQuery() {
		return ldTempQuery;
	}



	public void setLdTempQuery(LDTemporalQuery ldTempQuery) {
		this.ldTempQuery = ldTempQuery;
	}



	public List<URI> getRequestorList() {
		return requestorList;
	}



	public void setRequestorList(List<URI> requestorList) {
		this.requestorList = requestorList;
	}

	public void addEntityInfo(EntityInfo entity) {
		this.entities.add(entity);
	}
	public void removeEntityInfo(EntityInfo entity) {
		this.entities.remove(entity);
	}

	public void finalize() throws Throwable {

	}

}