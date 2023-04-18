package eu.neclab.ngsildbroker.commons.datatypes.terms;

import java.util.ArrayList;
import java.util.List;

import org.locationtech.spatial4j.shape.Shape;

import com.github.jsonldjava.core.Context;
import com.google.common.collect.Lists;

import eu.neclab.ngsildbroker.commons.constants.DBConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.DBUtil;
import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.mutiny.tuples.Tuple4;
import io.vertx.mutiny.sqlclient.Tuple;

public class GeoQueryTerm {
	private String geometry;
	private String coordinates;
	private List<Object> coordinatesAsList = Lists.newArrayList();
	private String geoproperty = "https://uri.etsi.org/ngsi-ld/location";
	private String georel = null;
	private String distanceType = null;
	private Double distanceValue = null;
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
		String[] splitted = coordinates.split("],");

		int outerCount = 0;
		List<Object> outerShell = Lists.newArrayList();
		List<Object> mostInnerArray = outerShell;
		List<Object> last = outerShell;
		while (splitted[0].charAt(outerCount) == '[') {
			outerCount++;
			mostInnerArray.add(new ArrayList<Object>());
			last = mostInnerArray;
			mostInnerArray = (List<Object>) mostInnerArray.get(0);
		}
		String[] tmp = splitted[0].substring(outerCount).split(",");

		mostInnerArray.add(Double.parseDouble(tmp[0].trim().replaceAll("[,\\[\\]]", "")));
		mostInnerArray.add(Double.parseDouble(tmp[1].trim().replaceAll("[,\\[\\]]", "")));

		if (splitted.length > 1) {
			for (int i = 1; i < splitted.length - 1; i++) {
				tmp = splitted[i].substring(1).split(",");
				last.add(Lists.newArrayList(Double.parseDouble(tmp[0].trim().replaceAll("[,\\[\\]]", "")),
						Double.parseDouble(tmp[1].trim().replaceAll("[,\\[\\]]", ""))));
			}
			tmp = splitted[splitted.length - 1].substring(0, splitted[splitted.length - 1].length() - outerCount)
					.split(",");
			last.add(Lists.newArrayList(Double.parseDouble(tmp[0].trim().replaceAll("[,\\[\\]]", "")),
					Double.parseDouble(tmp[1].trim().replaceAll("[,\\[\\]]", ""))));
		}
		this.coordinatesAsList = outerShell;

	}

	public List<Object> getCoordinatesAsList() {
		return coordinatesAsList;
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

	public Double getDistanceValue() {
		return distanceValue;
	}

	public void setDistanceValue(Double distanceValue) {
		this.distanceValue = distanceValue;
	}

	public Tuple4<Character, String, Integer, List<Object>> toSql(char startChar, Character prevResult, int dollar)
			throws ResponseException {
		StringBuilder builder = new StringBuilder();
		List<Object> tupleItems = Lists.newArrayList();
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
		builder.append(" isGeo AND attr=$");
		builder.append(dollar);
		dollar++;
		tupleItems.add(geoproperty);
		builder.append(" AND ");
		Tuple2<StringBuilder, Integer> tmp = getGeoSQLQuery(tupleItems, dollar, "geo_value");
		builder.append(tmp.getItem1());

		return Tuple4.of(startChar, builder.toString(), tmp.getItem2(), tupleItems);
	}

	public Tuple2<StringBuilder, Integer> getGeoSQLQuery(List<Object> tupleItems, int dollar, String fieldName)
			throws ResponseException {
		String referenceValue = "ST_SetSRID(ST_GeomFromGeoJSON('{\"type\": \"" + geometry + "\", \"coordinates\": "
				+ coordinates + " }'), 4326)";
		String sqlPostgisFunction = DBConstants.NGSILD_TO_POSTGIS_GEO_OPERATORS_MAPPING.get(georel);
		StringBuilder result = new StringBuilder();
		switch (georel) {
		case NGSIConstants.GEO_REL_NEAR:
			if (distanceValue != null && distanceType != null) {
				if (distanceType.equals(NGSIConstants.GEO_REL_MIN_DISTANCE))
					result.append("NOT ");
				result.append(sqlPostgisFunction + "( " + fieldName + "::geography, " + referenceValue + "::geography, "
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
			result.append(sqlPostgisFunction + "(" + fieldName + ", " + referenceValue + ") ");
			break;
		default:
			throw new ResponseException(ErrorType.BadRequestData, "Invalid georel operator: " + georel);
		}
		return Tuple2.of(result, dollar);
	}

	public int toSql(StringBuilder query, Tuple tuple, int dollar) {
		String dbColumn;
		if (!geoproperty.equals(NGSIConstants.NGSI_LD_LOCATION)) {
			query.append("data @> '{\"");
			query.append(geoproperty);
			query.append("\": [{\"");
			query.append(NGSIConstants.JSON_LD_TYPE);
			query.append("\":[\"");
			query.append(NGSIConstants.NGSI_LD_GEOPROPERTY);
			query.append("\"]}]}' AND ");
			dbColumn = "ST_SetSRID(ST_GeomFromGeoJSON( getGeoJson( " + "data#>'{" + geoproperty + ",0,"
					+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0}') ), 4326)";
		} else {
			dbColumn = "location";
		}

		String referenceValue = "ST_SetSRID(ST_GeomFromGeoJSON('{\"type\": \"" + geometry + "\", \"coordinates\": "
				+ coordinates + " }'), 4326)";
		String sqlPostgisFunction = DBConstants.NGSILD_TO_POSTGIS_GEO_OPERATORS_MAPPING.get(georel);
		switch (georel) {
		case NGSIConstants.GEO_REL_NEAR:
			if (distanceType.equals(NGSIConstants.GEO_REL_MIN_DISTANCE)) {
				query.append("NOT ");
			}
			query.append(sqlPostgisFunction);
			query.append("( ");
			query.append(dbColumn);
			query.append("::geography, ");
			query.append(referenceValue);
			query.append("::geography, ");
			query.append(distanceValue);
			query.append(") or ");
			query.append(sqlPostgisFunction);
			query.append("( ");
			query.append(dbColumn);
			query.append(", ");
			query.append(referenceValue);
			query.append(", ");
			query.append(distanceValue);
			query.append(") ");
			break;
		case NGSIConstants.GEO_REL_WITHIN:
		case NGSIConstants.GEO_REL_CONTAINS:
		case NGSIConstants.GEO_REL_OVERLAPS:
		case NGSIConstants.GEO_REL_INTERSECTS:
		case NGSIConstants.GEO_REL_EQUALS:
		case NGSIConstants.GEO_REL_DISJOINT:
			query.append(sqlPostgisFunction);
			query.append("( ");
			query.append(dbColumn);
			query.append(", ");
			query.append(referenceValue);
			query.append(") ");
			break;
		}
		return dollar;
	}

	public int toTempSql(StringBuilder query, Tuple tuple, int dollar, TemporalQueryTerm tempQuery) {
		String dbColumn = "geovalue";
		query.append("attributeid = $");
		query.append(dollar);
		tuple.addString(geoproperty);
		dollar++;
		query.append(" AND ");

		String referenceValue = "ST_SetSRID(ST_GeomFromGeoJSON('{\"type\": \"" + geometry + "\", \"coordinates\": "
				+ coordinates + " }'), 4326)";
		String sqlPostgisFunction = DBConstants.NGSILD_TO_POSTGIS_GEO_OPERATORS_MAPPING.get(georel);
		switch (georel) {
		case NGSIConstants.GEO_REL_NEAR:
			if (distanceType.equals(NGSIConstants.GEO_REL_MIN_DISTANCE)) {
				query.append("NOT ");
			}
			query.append(sqlPostgisFunction);
			query.append("( ");
			query.append(dbColumn);
			query.append("::geography, ");
			query.append(referenceValue);
			query.append("::geography, ");
			query.append(distanceValue);
			query.append(") or ");
			query.append(sqlPostgisFunction);
			query.append("( ");
			query.append(dbColumn);
			query.append(", ");
			query.append(referenceValue);
			query.append(", ");
			query.append(distanceValue);
			query.append(") ");
			break;
		case NGSIConstants.GEO_REL_WITHIN:
		case NGSIConstants.GEO_REL_CONTAINS:
		case NGSIConstants.GEO_REL_OVERLAPS:
		case NGSIConstants.GEO_REL_INTERSECTS:
		case NGSIConstants.GEO_REL_EQUALS:
		case NGSIConstants.GEO_REL_DISJOINT:
			query.append(sqlPostgisFunction);
			query.append("( ");
			query.append(dbColumn);
			query.append(", ");
			query.append(referenceValue);
			query.append(") ");
			break;
		}
		if (tempQuery != null) {
			query.append(" AND ");
			dollar = tempQuery.toSql(query, tuple, dollar);
		}
		return dollar;
	}

	public Shape getShape() {
		
		return null;
	}

	public void toRequestString(StringBuilder result, Shape geo) {
		// TODO Auto-generated method stub
		
	}

}