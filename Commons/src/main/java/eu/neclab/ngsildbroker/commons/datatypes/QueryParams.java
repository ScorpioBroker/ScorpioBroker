package eu.neclab.ngsildbroker.commons.datatypes;

import java.util.List;
import java.util.Map;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class QueryParams {

	@SerializedName("tenant")
	@Expose
	private String tenant;
	@SerializedName("attrs")
	@Expose
	private String attrs;
	@SerializedName("instanceId")
	@Expose
	private String instanceId;
	@SerializedName("entities")
	@Expose
	private List<Map<String, String>> entities;
	@SerializedName("q")
	@Expose
	private String q;

	@SerializedName("csf")
	@Expose
	private String csf;
	@SerializedName("georel")
	@Expose
	private GeoqueryRel georel;
	@SerializedName("geometry")
	@Expose
	private String geometry;
	@SerializedName("coordinates")
	@Expose
	private String coordinates;
	@SerializedName("geoproperty")
	@Expose
	private String geoproperty;
	@SerializedName("timerel")
	@Expose
	private String timerel;
	@SerializedName("timeAt")
	@Expose
	private String timeAt;
	@SerializedName("endTimeAt")
	@Expose
	private String endTimeAt;
	@SerializedName("timeproperty")
	@Expose
	private String timeproperty;
	@SerializedName("includeSysAttrs")
	@Expose
	private boolean includeSysAttrs;
	@SerializedName("keyValues")
	@Expose
	private boolean keyValues;
	@SerializedName("temporalValues")
	@Expose
	private boolean temporalValues;

	@SerializedName("limit")
	@Expose
	private int limit = -1;
	private boolean countResult;
	@SerializedName("offSet")
	@Expose
	private int offSet = 0;
	private String check;
	private int lastN;

	private String scopeQ;

	public String getScopeQ() {
		return scopeQ;
	}

	public void setScopeQ(String scopeQ) {
		this.scopeQ = scopeQ;
	}

	public String getCheck() {
		return check;
	}

	public void setCheck(String check) {
		this.check = check;
	}

	public String getAttrs() {
		return attrs;
	}

	public void setAttrs(String attrs) {
		this.attrs = attrs;
	}

	public QueryParams withAttrs(String attrs) {
		this.attrs = attrs;
		return this;
	}

	public String getInstanceId() {
		return instanceId;
	}

	public void setInstanceId(String instanceId) {
		this.instanceId = instanceId;
	}

	public QueryParams withInstanceId(String instanceId) {
		this.instanceId = instanceId;
		return this;
	}

	public String getQ() {
		return q;
	}

	public void setQ(String q) {
		this.q = q;
	}

	public QueryParams withQ(String q) {
		this.q = q;
		return this;
	}

	public GeoqueryRel getGeorel() {
		return georel;
	}

	public void setGeorel(GeoqueryRel georel) {
		this.georel = georel;
	}

	public QueryParams withGeorel(GeoqueryRel georel) {
		this.georel = georel;
		return this;
	}

	public String getGeometry() {
		return geometry;
	}

	public void setGeometry(String geometry) {
		this.geometry = geometry;
	}

	public QueryParams withGeometry(String geometry) {
		this.geometry = geometry;
		return this;
	}

	public String getCoordinates() {
		return coordinates;
	}

	public void setCoordinates(String coordinates) {
		this.coordinates = coordinates;
	}

	public QueryParams withCoordinates(String coordinates) {
		this.coordinates = coordinates;
		return this;
	}

	public String getGeoproperty() {
		return geoproperty;
	}

	public void setGeoproperty(String geoproperty) {
		this.geoproperty = geoproperty;
	}

	public QueryParams withGeoproperty(String geoproperty) {
		this.geoproperty = geoproperty;
		return this;
	}

	public String getTimerel() {
		return timerel;
	}

	public void setTimerel(String timerel) {
		this.timerel = timerel;
	}

	public QueryParams withTimerel(String timerel) {
		this.timerel = timerel;
		return this;
	}

	public String getTimeAt() {
		return timeAt;
	}

	public void setTimeAt(String timeAt) {
		this.timeAt = timeAt;
	}

	public QueryParams withTime(String timeAt) {
		this.timeAt = timeAt;
		return this;
	}

	public String getEndTimeAt() {
		return endTimeAt;
	}

	public void setEndTimeAt(String endTimeAt) {
		this.endTimeAt = endTimeAt;
	}

	public QueryParams withEndTime(String endTimeAt) {
		this.endTimeAt = endTimeAt;
		return this;
	}

	public String getTimeproperty() {
		return timeproperty;
	}

	public void setTimeproperty(String timeproperty) {
		this.timeproperty = timeproperty;
	}

	public QueryParams withTimeproperty(String timeproperty) {
		this.timeproperty = timeproperty;
		return this;
	}

	public boolean getIncludeSysAttrs() {
		return includeSysAttrs;
	}

	public void setIncludeSysAttrs(boolean includeSysAttrs) {
		this.includeSysAttrs = includeSysAttrs;
	}

	public QueryParams withIncludeSysAttrs(boolean includeSysAttrs) {
		this.includeSysAttrs = includeSysAttrs;
		return this;
	}

	public boolean getKeyValues() {
		return keyValues;
	}

	public void setKeyValues(boolean keyValues) {
		this.keyValues = keyValues;
	}

	public QueryParams withKeyValues(boolean keyValues) {
		this.keyValues = keyValues;
		return this;
	}

	public boolean getTemporalValues() {
		return temporalValues;
	}

	public void setTemporalValues(boolean temporalValues) {
		this.temporalValues = temporalValues;
	}

	public QueryParams withTemporalValues(boolean temporalValues) {
		this.temporalValues = temporalValues;
		return this;
	}

	public int getLimit() {
		return limit;
	}

	public void setLimit(int limit) {
		this.limit = limit;
	}

	public int getOffSet() {
		return offSet;
	}

	public void setOffSet(int offSet) {
		this.offSet = offSet;
	}

	public boolean getCountResult() {
		return countResult;
	}

	public void setCountResult(boolean countResult) {
		this.countResult = countResult;
	}

	public String getTenant() {
		return tenant;
	}

	public void setTenant(String tenant) {
		this.tenant = tenant;
	}

	public List<Map<String, String>> getEntities() {
		return entities;
	}

	public void setEntities(List<Map<String, String>> entities) {
		this.entities = entities;
	}

	@Override
	public String toString() {

		return "QueryParams [tenant=" + tenant + ", attrs=" + attrs + ", instanceId=" + instanceId + ", entities="
				+ entities + ", q=" + q + ", georel=" + georel + ", geometry=" + geometry + ", coordinates="
				+ coordinates + ", geoproperty=" + geoproperty + ", timerel=" + timerel + ", time=" + timeAt
				+ ", endTimeAt=" + endTimeAt + ", timeproperty=" + timeproperty + ", includeSysAttrs=" + includeSysAttrs
				+ ", keyValues=" + keyValues + ", temporalValues=" + temporalValues + ", limit=" + limit
				+ ", countResult=" + countResult + ", offSet=" + offSet + ", check=" + check + "]";
	}

	public void setLastN(int lastN) {
		this.lastN = lastN;

	}

	public int getLastN() {
		return lastN;
	}

	public String getCsf() {
		return csf;
	}

	public void setCsf(String csf) {
		this.csf = csf;
	}

}