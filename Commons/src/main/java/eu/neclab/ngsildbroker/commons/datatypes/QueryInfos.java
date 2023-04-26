package eu.neclab.ngsildbroker.commons.datatypes;

import java.util.HashSet;
import java.util.Set;

import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.jts.JtsPoint;

import com.github.jsonldjava.core.Context;

import eu.neclab.ngsildbroker.commons.datatypes.terms.GeoQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.LanguageQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.TypeQueryTerm;

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
	private String geoRel;

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

	public String toQueryString(Context context, TypeQueryTerm typeQuery, GeoQueryTerm geoQuery,
			LanguageQueryTerm langQuery, boolean ignoredId) {
		StringBuilder result = new StringBuilder("?");

		if (!ids.isEmpty() && !ignoredId) {
			result.append("id=");
			result.append(String.join(",", ids));
			result.append('&');
		}
		if (idPattern != null) {
			result.append("idPattern=");
			result.append(idPattern);
			result.append('&');
		}
		if (!types.isEmpty() && typeQuery != null) {
			result.append("type=");
			typeQuery.toRequestString(result);
			result.append('&');
		}
		if (!attrs.isEmpty()) {
			result.append("attrs=");
			for (String attr : attrs) {
				result.append(context.compactIri(attr));
				result.append(',');
			}
			result.setCharAt(result.length() - 1, '&');
		}
		if (!scopes.isEmpty()) {
			result.append("scopeQ=");
			result.append(String.join(",", scopes));
			result.append('&');
		}
		if (langQuery != null) {
			langQuery.toRequestString(result);
		}
		if (geo != null && geoQuery != null) {
			geoQuery.toRequestString(result, geo, geoQuery.getGeorel());

		}

		result.setLength(result.length() - 1);
		return result.toString();
	}

	public void addId(String id) {
		ids.add(id);
	}

	public void removeId(String id) {
		ids.remove(id);
	}

	public void addType(String type) {
		types.add(type);
	}

	public void removeType(String type) {
		types.remove(type);
	}

	public void addAttr(String attr) {
		attrs.add(attr);
	}

	public void removeAttr(String attr) {
		attrs.remove(attr);
	}

	public void addScope(String scope) {
		scopes.add(scope);
	}

	public void removeScope(String scope) {
		scopes.remove(scope);
	}

	public void merge(QueryInfos other) {

		this.ids.addAll(other.ids);
		this.types.addAll(other.types);
		this.attrs.addAll(other.attrs);
		this.scopes.addAll(other.scopes);
		if (other.geo != null) {
			if (this.geo == null) {
				this.geo = other.geo;
			}
		}
		if (other.idPattern != null) {
			if (this.idPattern == null) {
				this.idPattern = other.idPattern;
			}
		}
	}

	public void setGeoOp(String geoRel) {
		this.geoRel = geoRel;

	}

}
