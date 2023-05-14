package eu.neclab.ngsildbroker.commons.datatypes.terms;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.distance.DistanceUtils;
import org.locationtech.spatial4j.io.GeoJSONWriter;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.ShapeFactory.LineStringBuilder;
import org.locationtech.spatial4j.shape.ShapeFactory.MultiPolygonBuilder;
import org.locationtech.spatial4j.shape.ShapeFactory.PolygonBuilder;
import org.locationtech.spatial4j.shape.ShapeFactory.PolygonBuilder.HoleBuilder;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;
import org.locationtech.spatial4j.shape.jts.JtsPoint;

import com.github.jsonldjava.core.Context;
import com.google.common.collect.Lists;

import eu.neclab.ngsildbroker.commons.constants.DBConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.DBUtil;
import eu.neclab.ngsildbroker.commons.tools.SubscriptionTools;
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
		this.geoproperty = context.expandIri(geoproperty, false, true, context, null);
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
		Shape queryShape;
		List<List<Double>> tmp;
		switch (getGeometry()) {
		case NGSIConstants.GEO_TYPE_POINT:
			queryShape = SubscriptionTools.shapeFactory.pointLatLon((Double) getCoordinatesAsList().get(1),
					(Double) getCoordinatesAsList().get(0));
			break;
		case NGSIConstants.GEO_TYPE_LINESTRING:
			LineStringBuilder lineStringBuilder = SubscriptionTools.shapeFactory.lineString();
			tmp = (List<List<Double>>) getCoordinatesAsList().get(0);
			for (List<Double> point : tmp) {
				lineStringBuilder.pointLatLon(point.get(1), point.get(0));
			}
			queryShape = lineStringBuilder.build();
			break;
		case NGSIConstants.GEO_TYPE_POLYGON:
			List<Object> poly = getCoordinatesAsList();
			PolygonBuilder polygonBuilder = SubscriptionTools.shapeFactory.polygon();
			if (poly.size() > 1) {
				for (Object obj : poly) {
					List<List<Double>> subpoly = (List<List<Double>>) obj;
					HoleBuilder holeBuilder = polygonBuilder.hole();
					for (List<Double> point : subpoly) {
						holeBuilder.pointXY(point.get(0), point.get(1));
					}
					holeBuilder.endHole();
				}
			} else {
				for (List<Double> point : (List<List<Double>>) poly.get(0)) {
					polygonBuilder.pointXY(point.get(0), point.get(1));
				}
			}
			queryShape = polygonBuilder.build();
			break;
		case NGSIConstants.GEO_TYPE_MULTI_POLYGON:
			MultiPolygonBuilder multiPolyBuilder = SubscriptionTools.shapeFactory.multiPolygon();

			List<Object> list = getCoordinatesAsList();
			for (Object obj : list) {
				List<List<List<Double>>> multiPoly = (List<List<List<Double>>>) obj;
				PolygonBuilder tmpPolygonBuilder = multiPolyBuilder.polygon();
				if (multiPoly.size() > 1) {
					for (List<List<Double>> subpoly : multiPoly) {
						HoleBuilder holeBuilder = tmpPolygonBuilder.hole();
						for (List<Double> point : subpoly) {
							holeBuilder.pointLatLon(point.get(1), point.get(0));
						}
						holeBuilder.endHole();
					}
				} else {
					for (List<Double> point : multiPoly.get(0)) {
						tmpPolygonBuilder.pointLatLon(point.get(1), point.get(0));
					}
				}
			}
			queryShape = multiPolyBuilder.build();
			break;

		default:
			return null;

		}
		if (getDistanceValue() != null) {
			queryShape = queryShape.getBuffered(getDistanceValue() * DistanceUtils.KM_TO_DEG, queryShape.getContext());
		}
		return queryShape;
	}

	public void toRequestString(StringBuilder result, Shape geo, String georel) {
		result.append("geoproperty=");
		result.append(geoproperty);
		result.append("&georel=");
		result.append(georel);
		if (georel.equals(NGSIConstants.GEO_REL_NEAR)) {
			result.append(";");
			result.append(distanceType);
			result.append("=");
			result.append(distanceValue);
		}
		result.append("&coordinates=");
		result.append('[');
		if (geo instanceof JtsPoint) {
			JtsPoint point = (JtsPoint) geo;
			result.append(point.getLon());
			result.append(',');
			result.append(point.getLat());
		} else {
			JtsGeometry jtsGeom = (JtsGeometry) geo;
			Geometry geom = jtsGeom.getGeom();
			if (geom instanceof LineString) {
				LineString line = (LineString) geom;
				handleLine(line, result);
			} else if (geom instanceof MultiLineString) {
				MultiLineString multiLine = (MultiLineString) geom;
				int numGeo = multiLine.getNumGeometries();
				result.append('[');
				for (int i = 0; i < numGeo; i++) {
					LineString line = (LineString) multiLine.getGeometryN(i);
					handleLine(line, result);
					result.append(',');
				}
				result.setCharAt(result.length(), ']');
			} else if (geom instanceof Polygon) {
				Polygon poly = (Polygon) geom;
				handlePoly(poly, result);
			} else if (geom instanceof MultiPolygon) {
				MultiPolygon multiPoly = (MultiPolygon) geom;
				int numGeom = multiPoly.getNumGeometries();
				result.append('[');
				for (int i = 0; i < numGeom; i++) {
					handlePoly((Polygon) multiPoly.getGeometryN(i), result);
					result.append(',');
				}
				result.setCharAt(result.length(), ']');
			}
		}
		result.append(']');
	}

	private void handleLine(LineString line, StringBuilder result) {
		for (Coordinate coordinate : line.getCoordinates()) {
			result.append('[');
			result.append(coordinate.getX());
			result.append(',');
			result.append(coordinate.getY());
			result.append(']');
			result.append(',');
		}
		result.setLength(result.length() - 1);
	}

	private void handlePoly(Polygon poly, StringBuilder result) {
		LinearRing extRing = poly.getExteriorRing();
		result.append('[');
		for (Coordinate coordinate : extRing.getCoordinates()) {
			result.append('[');
			result.append(coordinate.getX());
			result.append(',');
			result.append(coordinate.getY());
			result.append(']');
		}
		int intRingNum = poly.getNumInteriorRing();
		for (int i = 0; i < intRingNum; i++) {
			LinearRing intRing = poly.getInteriorRingN(i);
			result.append(",[");
			for (Coordinate coordinate : intRing.getCoordinates()) {
				result.append('[');
				result.append(coordinate.getX());
				result.append(',');
				result.append(coordinate.getY());
				result.append(']');
			}
			result.append(']');

		}
		result.append(']');

	}

	public static void main(String[] args) {
		PolygonBuilder builder = SubscriptionTools.shapeFactory.polygon();

		builder.pointLatLon(0, 0).pointLatLon(0, 1).pointLatLon(1, 1).pointLatLon(1, 0).pointLatLon(0, 0);
		Shape poly = builder.build();
		Shape line = SubscriptionTools.shapeFactory.lineString().pointLatLon(0, 0).pointLatLon(0, 1).build();
		Shape multiline = SubscriptionTools.shapeFactory.multiLineString()
				.add(SubscriptionTools.shapeFactory.lineString().pointLatLon(0, 0).pointLatLon(0, 1)).build();
		Shape point = SubscriptionTools.shapeFactory.pointLatLon(0, 0);
		System.out.println(poly);
	}

}