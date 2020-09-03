package eu.neclab.ngsildbroker.commons.datatypes;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang.builder.ToStringBuilder;

public class QueryParams {

	@SerializedName("id")
	@Expose
	private String id;
	@SerializedName("type")
	@Expose
	private String type;
	@SerializedName("attrs")
	@Expose
	private String attrs;
	@SerializedName("instanceId")
	@Expose
	private String instanceId;
	@SerializedName("idPattern")
	@Expose
	private String idPattern;
	@SerializedName("q")
	@Expose
	private String q;
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
	@SerializedName("time")
	@Expose
	private String time;
	@SerializedName("endTime")
	@Expose
	private String endTime;
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

	@SerializedName("offSet")
	@Expose
	private int offSet = -1;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public QueryParams withId(String id) {
		this.id = id;
		return this;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public QueryParams withType(String type) {
		this.type = type;
		return this;
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

	public String getIdPattern() {
		return idPattern;
	}

	public void setIdPattern(String idPattern) {
		this.idPattern = idPattern;
	}

	public QueryParams withIdPattern(String idPattern) {
		this.idPattern = idPattern;
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

	public String getTime() {
		return time;
	}

	public void setTime(String time) {
		this.time = time;
	}

	public QueryParams withTime(String time) {
		this.time = time;
		return this;
	}

	public String getEndTime() {
		return endTime;
	}

	public void setEndTime(String endTime) {
		this.endTime = endTime;
	}

	public QueryParams withEndTime(String endTime) {
		this.endTime = endTime;
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

	@Override
	public String toString() {
		return new ToStringBuilder(this).append("id", id).append("type", type).append("attrs", attrs)
				.append("idPattern", idPattern).append("q", q).append("georel", georel).append("geometry", geometry)
				.append("coordinates", coordinates).append("geoproperty", geoproperty).append("timerel", timerel)
				.append("time", time).append("endTime", endTime).append("timeproperty", timeproperty).toString();
	}

}