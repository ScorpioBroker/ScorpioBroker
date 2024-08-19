package eu.neclab.ngsildbroker.commons.datatypes;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.locationtech.spatial4j.shape.Shape;
import com.github.jsonldjava.core.Context;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import eu.neclab.ngsildbroker.commons.datatypes.terms.GeoQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.LanguageQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.TypeQueryTerm;
import io.smallrye.mutiny.tuples.Tuple2;

public class QueryInfos {

	TypeQueryTerm typeQuery;
	GeoQueryTerm geoQuery;
	LanguageQueryTerm langQuery;
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

	public Map<String, String> toQueryParams(Context context, boolean ignoredId, EntityCache fullEntityCache,
			QueryRemoteHost tmpHost) {

		Map<String, String> result = Maps.newHashMap();
		Set<String> idsToBeUsed;
		if (fullEntityCache != null && ids != null && !ids.isEmpty()) {
			idsToBeUsed = Sets.newHashSet();
			for (String id : ids) {
				if (fullEntityCache.containsEntity(id)) {
					Tuple2<Map<String, Object>, Set<String>> entityAndHosts = fullEntityCache.get(id);
					Set<String> csourceIds = entityAndHosts.getItem2();
					if (csourceIds == null) {
						idsToBeUsed.add(id);
					} else {
						if (!csourceIds.contains(tmpHost.cSourceId())) {
							idsToBeUsed.add(id);
						}
					}
				} else {
					idsToBeUsed.add(id);
				}
			}
			if (idsToBeUsed.isEmpty()) {
				return null;
			}
		} else {
			idsToBeUsed = ids;
		}
		if (!idsToBeUsed.isEmpty() && !ignoredId) {
			result.put("id", String.join(",", idsToBeUsed));
		}
		if (idPattern != null) {
			result.put("idPattern", idPattern);
		}
		if (!types.isEmpty() && typeQuery != null) {
			StringBuilder tmp = new StringBuilder();
			typeQuery.toRequestString(tmp, context);
			result.put("type", tmp.toString());
		}
		if (!attrs.isEmpty()) {
			StringBuilder tmp = new StringBuilder();
			for (String attr : attrs) {
				tmp.append(URLEncoder.encode(context.compactIri(attr), StandardCharsets.UTF_8));
				tmp.append(',');
			}
			tmp.setLength(tmp.length() - 1);
			result.put("attrs", tmp.toString());
		}
		if (!scopes.isEmpty()) {
			result.put("scopeQ", String.join(",", scopes));
		}
		if (langQuery != null) {
			result.put("lang", langQuery.toRequestString());

		}
		if (geo != null && geoQuery != null) {
			Map<String, Object> tmp = Maps.newHashMap();
			geoQuery.addToRequestParams(tmp , geo, geoQuery.getGeorel());
			tmp.entrySet().forEach(entry -> {
				result.put(entry.getKey(), (String) entry.getValue());
			});

		}

		return result;
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

	public TypeQueryTerm getTypeQuery() {
		return typeQuery;
	}

	public void setTypeQuery(TypeQueryTerm typeQuery) {
		this.typeQuery = typeQuery;
	}

	public GeoQueryTerm getGeoQuery() {
		return geoQuery;
	}

	public void setGeoQuery(GeoQueryTerm geoQuery) {
		this.geoQuery = geoQuery;
	}

	public LanguageQueryTerm getLangQuery() {
		return langQuery;
	}

	public void setLangQuery(LanguageQueryTerm langQuery) {
		this.langQuery = langQuery;
	}

}
