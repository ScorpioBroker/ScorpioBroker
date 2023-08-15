package eu.neclab.ngsildbroker.commons.tools;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.PrimitiveIterator.OfInt;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

import com.github.jsonldjava.core.Context;
import com.google.common.collect.Sets;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.terms.AggrTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.AttrsQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.CSFQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.GeoQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.LanguageQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.QQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.ScopeQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.TemporalQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.TypeQueryTerm;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import io.smallrye.mutiny.tuples.Tuple2;

public class QueryParser {
	// Query = (QueryTerm / QueryTermAssoc) *(logicalOp (QueryTerm /
	// QueryTermAssoc))
	// QueryTermAssoc = %x28 QueryTerm *(logicalOp QueryTerm) %x29 ; (QueryTerm)
	private QueryParser() {

	}

	private static String andOp = ";";
	private static String orOp = "\\|";
	private static String logicalOp = "((" + andOp + ")|(" + orOp + "))";
	private static String quotedStr = "\".*\"";
	private static String equal = "==";
	private static String unequal = "!=";
	private static String greater = ">";
	private static String greaterEq = ">=";
	private static String less = "<";
	private static String lessEq = "<=";
	private static String patternOp = "~=";
	private static String notPatternOp = "!~=";
	private static String operator = "(" + equal + "|" + unequal + "|" + greaterEq + "|" + greater + "|" + lessEq + "|"
			+ less + ")";
	@SuppressWarnings("unused")
	private static String allOperators = "(" + equal + "|" + unequal + "|" + greaterEq + "|" + greater + "|" + lessEq
			+ "|" + less + "|" + patternOp + "|" + notPatternOp + ")";
	private static String dots = "\\.\\.";
	private static String dateTime = "\\d\\d\\d\\d-\\d\\d-\\d\\dT\\d\\d:\\d\\d:\\d\\d(,\\d\\d\\d\\d\\d\\d)?Z";
	private static String date = "\\d\\d\\d\\d-\\d\\d-\\d\\d";
	private static String time = "\\d\\d:\\d\\d:\\d\\d(,\\d\\d\\d\\d\\d\\d)?Z";
	private static String comparableValue = "((" + quotedStr + ")|(" + dateTime + ")|(" + date + ")|(" + time
			+ ")|(\\d+))";
	private static String otherValue = "(true|false)";
	private static String value = "(" + comparableValue + "|" + otherValue + ")";
	private static String valueList = value + "(," + value + ")*";
	private static String range = "(" + comparableValue + dots + comparableValue + ")";
	private static String uri = "\\w+:(\\/?\\/?)[^\\s]+";
	private static String compEqualityValue = "(" + otherValue + "|" + valueList + "|" + range + "|" + uri + ")";
	private static String attrName = "\\w+";
	private static String attrPathName = attrName + "(\\." + attrName + ")*";
	private static String compoundAttrName = attrName + "\\[" + attrName + "\\]*";
	private static String attribute = "(" + attrName + "|" + compoundAttrName + "|" + attrPathName + ")";
	private static String queryTermCompare = "" + attribute + "" + operator + "" + comparableValue + "";
	private static String queryTermEqual = "" + attribute + equal + compEqualityValue + "";
	private static String queryTermUnequal = "" + attribute + "" + unequal + "" + compEqualityValue + "";
	private static String queryTermPattern = "" + attribute + patternOp + "(.+)";
	private static String queryTermNotPattern = "" + attribute + notPatternOp + "(.*)";
	private static String queryTerm = "(" + queryTermCompare + ")|(" + queryTermEqual + ")|(" + queryTermUnequal + ")|("
			+ queryTermPattern + ")|(" + queryTermNotPattern + ")";
	private static String queryTermAssoc = "\\((" + queryTerm + "((" + logicalOp + ")(" + queryTerm + "))*)\\)";
	private static String query = "((" + queryTerm + ")|(" + queryTermAssoc + "))" + "((" + logicalOp + ")(("
			+ queryTerm + ")|(" + queryTermAssoc + ")))*";
	@SuppressWarnings("unused")
	// TODO validate queries still not working ... rework regex ???
	private static Pattern p = Pattern.compile(query);

	public static CSFQueryTerm parseCSFQuery(String input, Context context) throws ResponseException {
		return null;
	}

	public static QQueryTerm parseQuery(String input, Context context) throws ResponseException {
		if (input == null) {
			return null;
		}
		QQueryTerm root = new QQueryTerm(context);
		QQueryTerm current = root;
		boolean readingAttrib = true;
		String attribName = "";
		String operator = "";
		String operant = "";
		try {
			input = URLDecoder.decode(input, "utf-8");
		} catch (UnsupportedEncodingException e) {
			throw new ResponseException(ErrorType.InternalError, e.getMessage());
		}
		OfInt it = input.chars().iterator();
		while (it.hasNext()) {
			char b = (char) it.next().intValue();
			if (b == '(') {
				QQueryTerm child = new QQueryTerm(context);
				current.setFirstChild(child);
				current = child;
				readingAttrib = true;

			} else if (b == ';') {
				QQueryTerm next = new QQueryTerm(context);
				current.setOperant(operant);
				current.setNext(next);
				current.setNextAnd(true);
				current = next;
				readingAttrib = true;

				operant = "";

			} else if (b == '|') {
				QQueryTerm next = new QQueryTerm(context);
				current.setOperant(operant);
				current.setNext(next);
				current.setNextAnd(false);
				current = next;
				readingAttrib = true;

				operant = "";

			} else if (b == ')') {
				current.setOperant(operant);
				current = current.getParent();
				readingAttrib = true;

				operant = "";

			} else if (b == '!' || b == '=' || b == '<' || b == '>' || b == '~') {
				operator += (char) b;
				readingAttrib = false;
				if (!attribName.equals("")) {
					current.setAttribute(attribName);
					root.addAttrib(attribName);
					attribName = "";
				}
			} else {
				if (readingAttrib) {
					attribName += (char) b;
				} else {
					if (!operator.equals("")) {
						current.setOperator(operator);
						operator = "";
					}

					operant += (char) b;
				}
			}

		}
		if (readingAttrib) {
			current.setAttribute(attribName);
		}
		if (!operant.equals("")) {
			current.setOperant(operant);
		}
		return root;
	}

	public static GeoQueryTerm parseGeoQuery(String georel, String coordinates, String geometry, String geoproperty,
			Context context) throws ResponseException {
		if (georel == null && coordinates == null && geometry == null && geoproperty == null) {
			return null;
		}
		if (georel == null || georel.isEmpty()) {
			throw new ResponseException(ErrorType.BadRequestData, "georel needs to be provided");
		}
		if (coordinates == null) {
			throw new ResponseException(ErrorType.BadRequestData, "coordinates needs to be provided");
		}
		if (geometry == null) {
			throw new ResponseException(ErrorType.BadRequestData, "geometry needs to be provided");
		}
		String[] temp = georel.split(";");
		GeoQueryTerm result = new GeoQueryTerm(context);
		result.setGeorel(temp[0]);
		if (temp[0].equals(NGSIConstants.GEO_REL_NEAR)) {
			if (temp.length < 2) {
				throw new ResponseException(ErrorType.BadRequestData, "Georelation is not valid");
			}
			String[] maxMin = temp[1].split("==");
			result.setDistanceType(maxMin[0]);
			result.setDistanceValue(Double.parseDouble(maxMin[1]));
		}
		result.setCoordinates(coordinates);
		result.setGeometry(geometry);
		if (geoproperty != null) {
			result.setGeoproperty(geoproperty);
		}
		return result;
	}

	public static AttrsQueryTerm parseAttrs(String attrs, Context context) throws ResponseException {
		if (attrs == null) {
			return null;
		}
		AttrsQueryTerm result = new AttrsQueryTerm(context);
		for (String attr : attrs.split(",")) {
			result.addAttr(attr);
		}
		return result;
	}

	public static ScopeQueryTerm parseScopeQuery(String queryString) {
		if (queryString == null) {
			return null;
		}
		ScopeQueryTerm result = new ScopeQueryTerm();
		ScopeQueryTerm current = result;
		String scopeLevel = "";
		ArrayList<String> scopeLevels = new ArrayList<String>();
		OfInt it = queryString.chars().iterator();
		while (it.hasNext()) {
			char b = (char) it.next().intValue();
			if (b == '(') {
				ScopeQueryTerm child = new ScopeQueryTerm();
				current.setFirstChild(child);
				current = child;
			} else if (b == ';') {
				ScopeQueryTerm next = new ScopeQueryTerm();
				if (!scopeLevel.equals("")) {
					scopeLevels.add(scopeLevel);
					scopeLevel = "";
				}
				current.setScopeLevels(scopeLevels.toArray(new String[0]));
				current.setNext(next);
				current.setNextAnd(true);
				current = next;
				scopeLevels.clear();
			} else if (b == '|') {
				ScopeQueryTerm next = new ScopeQueryTerm();
				if (!scopeLevel.equals("")) {
					scopeLevels.add(scopeLevel);
					scopeLevel = "";
				}
				current.setScopeLevels(scopeLevels.toArray(new String[0]));
				current.setNext(next);
				current.setNextAnd(false);
				current = next;
				scopeLevels.clear();
			} else if (b == ')') {
				if (!scopeLevel.equals("")) {
					scopeLevels.add(scopeLevel);
					scopeLevel = "";
				}
				current.setScopeLevels(scopeLevels.toArray(new String[0]));
				current = current.getParent();
				scopeLevels.clear();

			} else if (b == '/') {
				if (!scopeLevel.equals("")) {
					scopeLevels.add(scopeLevel);
					scopeLevel = "";
				}
			} else {
				scopeLevel += (char) b;
			}

		}
		if (!scopeLevel.isEmpty()) {
			scopeLevels.add(scopeLevel);
		}
		if (!scopeLevels.isEmpty() && current != null) {
			current.setScopeLevels(scopeLevels.toArray(new String[0]));
		}
		return result;
	}

	public static TypeQueryTerm parseTypeQuery(String input, Context context) throws ResponseException {
		if (input == null) {
			return null;
		}
		Set<String> allTypes = Sets.newHashSet();
		TypeQueryTerm root = new TypeQueryTerm(context);
		TypeQueryTerm current = root;
		StringBuilder type = new StringBuilder();
		try {
			input = URLDecoder.decode(input, "utf-8");
		} catch (UnsupportedEncodingException e) {
			throw new ResponseException(ErrorType.InternalError, e.getMessage());
		}
		OfInt it = input.chars().iterator();
		while (it.hasNext()) {
			char b = (char) it.next().intValue();
			if (b == '(') {
				TypeQueryTerm child = new TypeQueryTerm(context);
				current.setFirstChild(child);
				current = child;
			} else if (b == ';') {
				TypeQueryTerm next = new TypeQueryTerm(context);
				current.setType(type.toString());
				allTypes.add(type.toString());
				current.setNext(next);
				current.setNextAnd(true);
				current = next;
				type.setLength(0);

			} else if (b == '|' || b == ',') {
				TypeQueryTerm next = new TypeQueryTerm(context);
				current.setType(type.toString());
				allTypes.add(type.toString());
				current.setNext(next);
				current.setNextAnd(false);
				current = next;
				type.setLength(0);

			} else if (b == ')') {
				current.setType(type.toString());
				allTypes.add(type.toString());
				current = current.getParent();
				type.setLength(0);

			} else {
				type.append((char) b);
			}

		}
		if (!type.isEmpty()) {
			current.setType(type.toString());
			allTypes.add(current.getType());
		}
		root.setAllTypes(allTypes);
		return root;
	}

	public static AggrTerm parseAggrTerm(String aggrMethods, String aggrPeriodDuration) throws ResponseException {
		if (aggrMethods == null && aggrPeriodDuration == null) {
			return null;
		}
		if (aggrMethods == null || aggrMethods.isEmpty()) {
			throw new ResponseException(ErrorType.InvalidRequest,
					"You must provide a aggrMethods entry when providing aggrPeriodDuration");
		}
		AggrTerm result = new AggrTerm();
		result.setPeriod(aggrPeriodDuration);
		String[] splitted = aggrMethods.split(",");
		Set<String> aggrFunctions = new HashSet<>(splitted.length);
		for (String aggrMethod : splitted) {
			if (!NGSIConstants.ALLOWED_AGGR_METH.contains(aggrMethod)) {
				throw new ResponseException(ErrorType.InvalidRequest, aggrMethod + " is not a valid aggrMethod. Use "
						+ StringUtils.join(NGSIConstants.ALLOWED_AGGR_METH, " or "));
			}
			aggrFunctions.add(aggrMethod);
		}
		result.setAggrFunctions(aggrFunctions);
		return result;
	}

	public static TemporalQueryTerm parseTempQuery(String timeProperty, String timeRel, String timeAt, String endTimeAt)
			throws ResponseException {
		if (timeProperty == null && timeRel == null && timeAt == null && endTimeAt == null) {
			return null;
		}
		if (timeProperty == null) {
			timeProperty = NGSIConstants.QUERY_PARAMETER_OBSERVED_AT;
		} else if (!NGSIConstants.ALLOWED_TIME_PROPERTIES.contains(timeProperty)) {
			throw new ResponseException(ErrorType.InvalidRequest, timeProperty + " is not an allowed timeproperty. Use "
					+ StringUtils.join(NGSIConstants.ALLOWED_TIME_PROPERTIES, " or "));
		}
		if (timeRel == null) {
			return new TemporalQueryTerm(timeProperty, null, null, null);
		}
		switch (timeRel) {
		case NGSIConstants.TIME_REL_AFTER:
			if (endTimeAt != null) {
				throw new ResponseException(ErrorType.InvalidRequest,
						NGSIConstants.TIME_REL_AFTER + " cannot be used with " + NGSIConstants.QUERY_PARAMETER_ENDTIME);
			}
			if (timeAt == null) {
				throw new ResponseException(ErrorType.InvalidRequest,
						NGSIConstants.TIME_REL_AFTER + " cannot be used without " + NGSIConstants.QUERY_PARAMETER_TIME);
			}
			try {
				SerializationTools.informatter.parse(timeAt);
			} catch (DateTimeParseException e) {
				throw new ResponseException(ErrorType.InvalidRequest, "Provided timeAt is not in a valid format");
			}
			break;
		case NGSIConstants.TIME_REL_BEFORE:
			if (endTimeAt != null) {
				throw new ResponseException(ErrorType.InvalidRequest, NGSIConstants.TIME_REL_BEFORE
						+ " cannot be used with " + NGSIConstants.QUERY_PARAMETER_ENDTIME);
			}
			if (timeAt == null) {
				throw new ResponseException(ErrorType.InvalidRequest, NGSIConstants.TIME_REL_BEFORE
						+ " cannot be used without " + NGSIConstants.QUERY_PARAMETER_TIME);
			}
			try {
				SerializationTools.informatter.parse(timeAt);
			} catch (DateTimeParseException e) {
				throw new ResponseException(ErrorType.InvalidRequest, "Provided timeAt is not in a valid format");
			}
			break;
		case NGSIConstants.TIME_REL_BETWEEN:
			if (endTimeAt == null) {
				throw new ResponseException(ErrorType.InvalidRequest, NGSIConstants.TIME_REL_BETWEEN
						+ " cannot be used without " + NGSIConstants.QUERY_PARAMETER_ENDTIME);
			}
			if (timeAt == null) {
				throw new ResponseException(ErrorType.InvalidRequest, NGSIConstants.TIME_REL_BETWEEN
						+ " cannot be used without " + NGSIConstants.QUERY_PARAMETER_TIME);
			}
			try {
				SerializationTools.informatter.parse(timeAt);
			} catch (DateTimeParseException e) {
				throw new ResponseException(ErrorType.InvalidRequest, "Provided timeAt is not in a valid format");
			}
			try {
				SerializationTools.informatter.parse(endTimeAt);
			} catch (DateTimeParseException e) {
				throw new ResponseException(ErrorType.InvalidRequest, "Provided endTimeAt is not in a valid format");
			}
			break;

		default:
			throw new ResponseException(ErrorType.InvalidRequest,
					timeRel + " is not an allowed timerel. Use " + NGSIConstants.TIME_REL_AFTER + " or "
							+ NGSIConstants.TIME_REL_BEFORE + " or " + NGSIConstants.TIME_REL_BETWEEN);
		}

		return new TemporalQueryTerm(timeProperty, timeRel, timeAt, endTimeAt);
	}

	public static LanguageQueryTerm parseLangQuery(String langString) throws ResponseException {
		if (langString == null) {
			return null;
		}
		LanguageQueryTerm result = new LanguageQueryTerm();
		String[] langPairs = langString.split(";");

		for (int i = 0; i < langPairs.length; i++) {
			Float q = 1.0f;
			if (i + 1 < langPairs.length) {
				String[] tmp = langPairs[i + 1].split(",");
				if (!tmp[0].startsWith("q=")) {
					throw new ResponseException(ErrorType.InvalidRequest,
							"syntax error in the lang query. looks like there is a q missing");
				} else {
					q = Float.parseFloat(tmp[0].substring(2));
					langPairs[i + 1] = langPairs[i + 1].substring(tmp[0].length());
				}
			}
			String[] tmp = langPairs[i].split(",");
			if (!tmp[0].isEmpty()) {
				result.addTuple(Tuple2.of(Set.of(tmp), q));
			}
		}
		result.sort();
		return result;
	}

}
