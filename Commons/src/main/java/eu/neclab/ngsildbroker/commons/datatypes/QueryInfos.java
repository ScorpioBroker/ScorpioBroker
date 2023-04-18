package eu.neclab.ngsildbroker.commons.datatypes;

import java.util.HashSet;
import java.util.Set;

import org.locationtech.spatial4j.shape.Shape;

public class QueryInfos {

	boolean fullIdFound = false;
	boolean fullTypesFound = false;
	boolean fullAttrsFound = false;
	boolean fullScopeFound = false;
	Set<String> ids = new HashSet<>();
	Set<String> types = new HashSet<>();
	Set<String> attrs = new HashSet<>();
	Set<String> scopes = new HashSet<>();
	Shape geo;
	String idPattern;

	public Set<String> getIds() {
		return ids;
	}

	public void setIds(Set<String> ids) {
		this.ids = ids;
	}

	public Set<String> getTypes() {
		return types;
	}

	public void setTypes(Set<String> types) {
		this.types = types;
	}

	public Set<String> getAttrs() {
		return attrs;
	}

	public void setAttrs(Set<String> attrs) {
		this.attrs = attrs;
	}

	public Set<String> getScopes() {
		return scopes;
	}

	public void setScopes(Set<String> scopes) {
		this.scopes = scopes;
	}

	public Shape getGeo() {
		return geo;
	}

	public void setGeo(Shape geo) {
		this.geo = geo;
	}

	public String getIdPattern() {
		return idPattern;
	}

	public void setIdPattern(String idPattern) {
		this.idPattern = idPattern;
	}

	public boolean isFullIdFound() {
		return fullIdFound;
	}

	public void setFullIdFound(boolean fullIdFound) {
		this.fullIdFound = fullIdFound;
	}

	public boolean isFullTypesFound() {
		return fullTypesFound;
	}

	public void setFullTypesFound(boolean fullTypesFound) {
		this.fullTypesFound = fullTypesFound;
	}

	public boolean isFullAttrsFound() {
		return fullAttrsFound;
	}

	public void setFullAttrsFound(boolean fullAttrsFound) {
		this.fullAttrsFound = fullAttrsFound;
	}

	public boolean isFullScopeFound() {
		return fullScopeFound;
	}

	public void setFullScopeFound(boolean fullScopeFound) {
		this.fullScopeFound = fullScopeFound;
	}

	public String toQueryString() {
		return null;
	}

}
