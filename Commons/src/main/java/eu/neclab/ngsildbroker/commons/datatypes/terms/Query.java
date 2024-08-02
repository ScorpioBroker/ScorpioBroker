package eu.neclab.ngsildbroker.commons.datatypes.terms;

import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.github.jsonldjava.core.Context;

import eu.neclab.ngsildbroker.commons.datatypes.ViaHeaders;
import io.smallrye.mutiny.tuples.Tuple3;

public class Query {
	List<Tuple3<String[], TypeQueryTerm, String>> idsAndTypeAndIdPattern;
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
	private String entityMapToken;
	private boolean tokenProvided;
	private CSFQueryTerm csfQueryTerm;
	@JsonIgnore
	private Context context;
	private String checkSum;
	@JsonIgnore
	private ViaHeaders viaHeaders;
	private int limit;
	private String finalOptions;
	private int acceptHeader;
	public Query() {
		// for serializer
	}

	public List<Tuple3<String[], TypeQueryTerm, String>> getIdsAndTypeAndIdPattern() {
		return idsAndTypeAndIdPattern;
	}

	public void setIdsAndTypeAndIdPattern(List<Tuple3<String[], TypeQueryTerm, String>> idsAndTypeAndIdPattern) {
		this.idsAndTypeAndIdPattern = idsAndTypeAndIdPattern;
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

	public void setEntityMapToken(String entityMapToken) {
		this.entityMapToken = entityMapToken;

	}

	public String getEntityMapToken() {
		return entityMapToken;
	}

	public void setTokenProvided(boolean tokenProvided) {
		this.tokenProvided = tokenProvided;

	}

	public boolean isTokenProvided() {
		return tokenProvided;
	}

	public void setCsfQueryTerm(CSFQueryTerm csfQueryTerm) {
		this.csfQueryTerm = csfQueryTerm;

	}

	public CSFQueryTerm getCsfQueryTerm() {
		return csfQueryTerm;
	}

	@JsonIgnore
	public void setContext(Context context) {
		this.context = context;

	}

	@JsonIgnore
	public Context getContext() {
		return context;
	}

	public void setCheckSum(String checkSum) {
		this.checkSum = checkSum;

	}

	public String getCheckSum() {
		return checkSum;
	}

	@JsonIgnore
	public void setViaHeaders(ViaHeaders viaHeaders) {
		this.viaHeaders = viaHeaders;

	}

	@JsonIgnore
	public ViaHeaders getViaHeaders() {
		return viaHeaders;
	}

	public int getLimit() {
		return limit;
	}

	public void setLimit(int limit) {
		this.limit = limit;
	}

	public String getFinalOptions() {
		return finalOptions;
	}

	public void setFinalOptions(String finalOptions) {
		this.finalOptions = finalOptions;
	}

	public void setAcceptHeader(int acceptHeader) {
		this.acceptHeader = acceptHeader;

	}

	public int getAcceptHeader() {
		return acceptHeader;
	}

}
