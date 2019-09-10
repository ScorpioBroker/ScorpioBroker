package eu.neclab.ngsildbroker.commons.datatypes;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:32:45
 */
public class Entity {

	private URI id;
	private GeoProperty location;
	private GeoProperty observationSpace;
	private GeoProperty operationSpace;
	private List<Property> properties;
	private Object refToAccessControl;
	private List<Relationship> relationships;
	private List<GeoProperty> geoProperties;	
	private String type;
	private Long createdAt;
	private Long modifiedAt;
	private Long observedAt;

	private List<BaseProperty> allBaseProperties = new ArrayList<BaseProperty>();
	private Property createdAtProp = new Property();
	private Property modifiedAtProp = new Property();
	private Property observedAtProp = new Property();

	public Entity(URI id, GeoProperty location, GeoProperty observationSpace, GeoProperty operationSpace,
			List<Property> properties, Object refToAccessControl, List<Relationship> relationships, String type,
			List<GeoProperty> geoProperties) {
		super();
		this.id = id;
		this.location = location;
		this.observationSpace = observationSpace;
		this.operationSpace = operationSpace;
		this.properties = properties;
		this.refToAccessControl = refToAccessControl;
		this.relationships = relationships;
		this.geoProperties = geoProperties;
		this.type = type;
		try {
			createdAtProp.setId(new URI(NGSIConstants.NGSI_LD_CREATED_AT));
			modifiedAtProp.setId(new URI(NGSIConstants.NGSI_LD_MODIFIED_AT));
			observedAtProp.setId(new URI(NGSIConstants.NGSI_LD_OBSERVED_AT));
			createdAtProp.setValue(null);
			modifiedAtProp.setValue(null);
			observedAtProp.setValue(null);
			allBaseProperties.add(createdAtProp);
			allBaseProperties.add(modifiedAtProp);
			allBaseProperties.add(observedAtProp);
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
		if (properties != null) {
			allBaseProperties.addAll(properties);
		}
		if (relationships != null) {
			allBaseProperties.addAll(relationships);
		}
		if (geoProperties != null) {
			allBaseProperties.addAll(geoProperties);
		}

	}

	public Long getObservedAt() {
		return observedAt;
	}

	public void setObservedAt(Long observedAt) {
		this.observedAt = observedAt;
		observedAtProp.setSingleValue(observedAt);
	}

	public URI getId() {
		return id;
	}

	public void setId(URI id) {
		this.id = id;
	}

	public GeoProperty getLocation() {
		return location;
	}

	public void setLocation(GeoProperty location) {
		this.location = location;
	}

	public GeoProperty getObservationSpace() {
		return observationSpace;
	}

	public void setObservationSpace(GeoProperty observationSpace) {
		this.observationSpace = observationSpace;
	}

	public GeoProperty getOperationSpace() {
		return operationSpace;
	}

	public void setOperationSpace(GeoProperty operationSpace) {
		this.operationSpace = operationSpace;
	}

	public List<Property> getProperties() {
		return properties;
	}

	public void setProperties(List<Property> properties) {
		if (this.properties != null) {
			allBaseProperties.removeAll(this.properties);
		}
		allBaseProperties.addAll(properties);

		this.properties = properties;
	}

	public Object getRefToAccessControl() {
		return refToAccessControl;
	}

	public void setRefToAccessControl(Object refToAccessControl) {
		this.refToAccessControl = refToAccessControl;
	}

	public List<Relationship> getRelationships() {
		return relationships;
	}

	public void setRelationships(List<Relationship> relationships) {
		if (this.relationships != null) {
			allBaseProperties.removeAll(this.relationships);
		}
		if (relationships != null) {
			allBaseProperties.addAll(relationships);
		}
		this.relationships = relationships;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public void finalize() throws Throwable {

	}

	public Long getCreatedAt() {
		return createdAt;
	}

	public void setCreatedAt(Long createdAt) {
		createdAtProp.setSingleValue(createdAt);
		this.createdAt = createdAt;
	}

	public Long getModifiedAt() {
		return modifiedAt;
	}

	public void setModifiedAt(Long modifiedAt) {
		modifiedAtProp.setSingleValue(modifiedAt);
		this.modifiedAt = modifiedAt;
	}
	

	public List<BaseProperty> getAllBaseProperties() {
		return allBaseProperties;
	}


	public List<GeoProperty> getGeoProperties() {
		return geoProperties;
	}

	public void setGeoProperties(List<GeoProperty> geoProperties) {
		if (this.geoProperties != null) {
			allBaseProperties.removeAll(this.geoProperties);
		}
		if (geoProperties != null) {
			allBaseProperties.addAll(geoProperties);
		}
		this.geoProperties = geoProperties;
	}	
	
	@Override
	public String toString() {
		return "Entity [id=" + id + ", location=" + location + ", observationSpace=" + observationSpace
				+ ", operationSpace=" + operationSpace + ", properties=" + properties + ", refToAccessControl="
				+ refToAccessControl + ", relationships=" + relationships + ", type=" + type + "]";
	}

}