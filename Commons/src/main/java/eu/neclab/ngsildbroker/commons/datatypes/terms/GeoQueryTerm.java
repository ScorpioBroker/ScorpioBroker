package eu.neclab.ngsildbroker.commons.datatypes.terms;

import java.util.List;

import com.github.jsonldjava.core.Context;

import eu.neclab.ngsildbroker.commons.constants.DBConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.GeoRelation;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import io.smallrye.mutiny.tuples.Tuple2;

public class GeoQueryTerm {
	private String geometry;
	private String coordinates;
	private String geoproperty = "https://uri.etsi.org/ngsi-ld/location";
	private String georel = null;
	private String distanceType = null;
	private String distanceValue = null;
	private Context context;

	public GeoQueryTerm(Context context) {
		this.context = context;
	}

	public String getGeometry() {
		return geometry;
	}

	public void setGeometry(String geometry) {
		this.geometry = geometry;
	}

	public String getCoordinates() {
		return coordinates;
	}

	public void setCoordinates(String coordinates) {
		this.coordinates = coordinates;
	}

	public String getGeoproperty() {
		return geoproperty;
	}

	public void setGeoproperty(String geoproperty) {
		this.geoproperty = context.expandIri(geoproperty, false, true, null, null);
	}

	public String getGeorel() {
		return georel;
	}

	public void setGeorel(String georel) {
		this.georel = georel;
	}

	public String getDistanceType() {
		return distanceType;
	}

	public void setDistanceType(String distanceType) {
		this.distanceType = distanceType;
	}

	public String getDistanceValue() {
		return distanceValue;
	}

	public void setDistanceValue(String distanceValue) {
		this.distanceValue = distanceValue;
	}

	public Tuple2<Character, String> toSql(char startChar, Character prevResult) throws ResponseException {
		StringBuilder builder = new StringBuilder();
		builder.append(startChar);
		builder.append(" as (SELECT attr2iid.iid FROM ");
		if (prevResult != null) {
			builder.append(prevResult);
			builder.append(" LEFT JOIN attr2iid ON ");
			builder.append(prevResult);
			builder.append(".iid = attr2iid.iid WHERE ");
		} else {
			builder.append(" attr2iid WHERE ");
		}
		builder.append(" isGeo AND attr='");
		builder.append(geoproperty);
		builder.append("' AND ");
		builder.append(getGeoSQLQuery());
		return Tuple2.of(startChar, builder.toString());
	}

	private StringBuilder getGeoSQLQuery() throws ResponseException {
		String referenceValue = "ST_SetSRID(ST_GeomFromGeoJSON('{\"type\": \"" + geometry + "\", \"coordinates\": "
				+ coordinates + " }'), 4326)";
		String sqlPostgisFunction = DBConstants.NGSILD_TO_POSTGIS_GEO_OPERATORS_MAPPING.get(georel);
		StringBuilder result = new StringBuilder();
		switch (georel) {
		case NGSIConstants.GEO_REL_NEAR:
			if (distanceValue != null && distanceType != null) {
				if (distanceType.equals(NGSIConstants.GEO_REL_MIN_DISTANCE))
					result.append("NOT ");
				result.append(sqlPostgisFunction + "( geo_value::geography, " + referenceValue + "::geography, "
						+ distanceValue + ") ");
			} else {
				throw new ResponseException(ErrorType.BadRequestData,
						"GeoQuery: Type and distance are required for near relation");
			}
			break;
		case NGSIConstants.GEO_REL_WITHIN:
		case NGSIConstants.GEO_REL_CONTAINS:
		case NGSIConstants.GEO_REL_OVERLAPS:
		case NGSIConstants.GEO_REL_INTERSECTS:
		case NGSIConstants.GEO_REL_EQUALS:
		case NGSIConstants.GEO_REL_DISJOINT:
			result.append(sqlPostgisFunction + "(geo_value, " + referenceValue + ") ");
			break;
		default:
			throw new ResponseException(ErrorType.BadRequestData, "Invalid georel operator: " + georel);
		}
		return result;
	}
}