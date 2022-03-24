package eu.neclab.ngsildbroker.commons.ngsiqueries;

import java.util.ArrayList;
import java.util.PrimitiveIterator.OfInt;
import java.util.regex.Pattern;

import com.github.jsonldjava.core.Context;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.GeoqueryRel;
import eu.neclab.ngsildbroker.commons.datatypes.QueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.ScopeQueryTerm;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

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

	public static QueryTerm parseQuery(String input, Context context) throws ResponseException {
//		Matcher matcher = p.matcher(input);
//		if (!matcher.matches()) {
//			throw new BadRequestException();
//		}
		// TODO: regex doesn't validate brackets queries for some reason
		QueryTerm root = new QueryTerm(context);
		QueryTerm current = root;
		boolean readingAttrib = true;
		String attribName = "";
		String operator = "";
		String operant = "";
		OfInt it = input.chars().iterator();
		while (it.hasNext()) {
			char b = (char) it.next().intValue();
			if (b == '(') {
				QueryTerm child = new QueryTerm(context);
				current.setFirstChild(child);
				current = child;
				readingAttrib = true;

			} else if (b == ';') {
				QueryTerm next = new QueryTerm(context);
				current.setOperant(operant);
				current.setNext(next);
				current.setNextAnd(true);
				current = next;
				readingAttrib = true;

				operant = "";

			} else if (b == '|') {
				QueryTerm next = new QueryTerm(context);
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
		if (!operant.equals("")) {
			current.setOperant(operant);
		}
		return root;
	}

	public static GeoqueryRel parseGeoRel(String georel) throws ResponseException {
		if (georel == null || georel.isEmpty()) {
			throw new ResponseException(ErrorType.BadRequestData, "georel needs to be provided");
		}
		String[] temp = georel.split(";");
		GeoqueryRel result = new GeoqueryRel();
		result.setGeorelOp(temp[0]);
		if (temp[0].equals(NGSIConstants.GEO_REL_NEAR)) {
			if (temp.length < 2) {
				throw new ResponseException(ErrorType.BadRequestData, "Georelation is not valid");
			}
			String[] maxMin = temp[1].split("==");
			result.setDistanceType(maxMin[0]);
			result.setDistanceValue(maxMin[1]);
		}
		return result;
	}

	public static ScopeQueryTerm parseScopeQuery(String queryString) {
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

}
