package eu.neclab.ngsildbroker.commons.datatypes.terms;

import java.util.Set;

public class Query {
	TypeQueryTerm typeQueryTerm;
	ScopeQueryTerm scopeQueryTerm;
	QQueryTerm qQueryTerm;
	PickTerm pickTerm;
	OmitTerm omitTerm;
	LanguageQueryTerm languageQueryTerm;
	GeoQueryTerm geoQueryTerm;
	DataSetIdTerm dataSetIdTerm;
	AttrsQueryTerm attrsQueryTerm;
	private Set<String> jsonKeys;
	private String join;
	private int joinLevel;
	public TypeQueryTerm getTypeQueryTerm() {
		return typeQueryTerm;
	}
	public void setTypeQueryTerm(TypeQueryTerm typeQueryTerm) {
		this.typeQueryTerm = typeQueryTerm;
	}
	public ScopeQueryTerm getScopeQueryTerm() {
		return scopeQueryTerm;
	}
	public void setScopeQueryTerm(ScopeQueryTerm scopeQueryTerm) {
		this.scopeQueryTerm = scopeQueryTerm;
	}
	public QQueryTerm getqQueryTerm() {
		return qQueryTerm;
	}
	public void setqQueryTerm(QQueryTerm qQueryTerm) {
		this.qQueryTerm = qQueryTerm;
	}
	public PickTerm getPickTerm() {
		return pickTerm;
	}
	public void setPickTerm(PickTerm pickTerm) {
		this.pickTerm = pickTerm;
	}
	public OmitTerm getOmitTerm() {
		return omitTerm;
	}
	public void setOmitTerm(OmitTerm omitTerm) {
		this.omitTerm = omitTerm;
	}
	public LanguageQueryTerm getLanguageQueryTerm() {
		return languageQueryTerm;
	}
	public void setLanguageQueryTerm(LanguageQueryTerm languageQueryTerm) {
		this.languageQueryTerm = languageQueryTerm;
	}
	public GeoQueryTerm getGeoQueryTerm() {
		return geoQueryTerm;
	}
	public void setGeoQueryTerm(GeoQueryTerm geoQueryTerm) {
		this.geoQueryTerm = geoQueryTerm;
	}
	public DataSetIdTerm getDataSetIdTerm() {
		return dataSetIdTerm;
	}
	public void setDataSetIdTerm(DataSetIdTerm dataSetIdTerm) {
		this.dataSetIdTerm = dataSetIdTerm;
	}
	public AttrsQueryTerm getAttrsQueryTerm() {
		return attrsQueryTerm;
	}
	public void setAttrsQueryTerm(AttrsQueryTerm attrsQueryTerm) {
		this.attrsQueryTerm = attrsQueryTerm;
	}
	public void setJsonKeys(Set<String> jsonKeys) {
		this.jsonKeys = jsonKeys;
	}
	public Set<String> getJsonKeys() {
		return jsonKeys;
	}
	public String getJoin() {
		return join;
	}
	
	public void setJoin(String join) {
		this.join = join;
	}
	public int getJoinLevel() {
		return joinLevel;
	}
	public void setJoinLevel(int joinLevel) {
		this.joinLevel = joinLevel;
	}
	
	
	
	
}
