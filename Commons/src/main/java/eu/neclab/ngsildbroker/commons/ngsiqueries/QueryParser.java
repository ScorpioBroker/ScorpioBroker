package eu.neclab.ngsildbroker.commons.ngsiqueries;

import java.util.List;
import java.util.regex.Pattern;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.GeoqueryRel;
import eu.neclab.ngsildbroker.commons.datatypes.QueryTerm;
import eu.neclab.ngsildbroker.commons.exceptions.BadRequestException;

@Component
public class QueryParser {
	// Query = (QueryTerm / QueryTermAssoc) *(logicalOp (QueryTerm /
	// QueryTermAssoc))
	// QueryTermAssoc = %x28 QueryTerm *(logicalOp QueryTerm) %x29 ; (QueryTerm)

	private String andOp = ";";
	private String orOp = "\\|";
	private String logicalOp = "((" + andOp + ")|(" + orOp + "))";
	private String quotedStr = "\".*\"";
	private String equal = "==";
	private String unequal = "!=";
	private String greater = ">";
	private String greaterEq = ">=";
	private String less = "<";
	private String lessEq = "<=";
	private String patternOp = "~=";
	private String notPatternOp = "!~=";
	private String operator = "(" + equal + "|" + unequal + "|" + greaterEq + "|" + greater + "|" + lessEq + "|" + less
			+ ")";
	@SuppressWarnings("unused")
	private String allOperators = "(" + equal + "|" + unequal + "|" + greaterEq + "|" + greater + "|" + lessEq + "|"
			+ less + "|" + patternOp + "|" + notPatternOp + ")";
	private String dots = "\\.\\.";
	private String dateTime = "\\d\\d\\d\\d-\\d\\d-\\d\\dT\\d\\d:\\d\\d:\\d\\d(,\\d\\d\\d\\d\\d\\d)?Z";
	private String date = "\\d\\d\\d\\d-\\d\\d-\\d\\d";
	private String time = "\\d\\d:\\d\\d:\\d\\d(,\\d\\d\\d\\d\\d\\d)?Z";
	private String comparableValue = "((" + quotedStr + ")|(" + dateTime + ")|(" + date + ")|(" + time + ")|(\\d+))";
	private String otherValue = "(true|false)";
	private String value = "(" + comparableValue + "|" + otherValue + ")";
	private String valueList = value + "(," + value + ")*";
	private String range = "(" + comparableValue + dots + comparableValue + ")";
	private String uri = "\\w+:(\\/?\\/?)[^\\s]+";
	private String compEqualityValue = "(" + otherValue + "|" + valueList + "|" + range + "|" + uri + ")";
	private String attrName = "\\w+";
	private String attrPathName = attrName + "(\\." + attrName + ")*";
	private String compoundAttrName = attrName + "\\[" + attrName + "\\]*";
	private String attribute = "(" + attrName + "|" + compoundAttrName + "|" + attrPathName + ")";
	private String queryTermCompare = "" + attribute + "" + operator + "" + comparableValue + "";
	private String queryTermEqual = "" + attribute + equal + compEqualityValue + "";
	private String queryTermUnequal = "" + attribute + "" + unequal + "" + compEqualityValue + "";
	private String queryTermPattern = "" + attribute + patternOp + "(.+)";
	private String queryTermNotPattern = "" + attribute + notPatternOp + "(.*)";
	private String queryTerm = "(" + queryTermCompare + ")|(" + queryTermEqual + ")|(" + queryTermUnequal + ")|("
			+ queryTermPattern + ")|(" + queryTermNotPattern + ")";
	private String queryTermAssoc = "\\((" + queryTerm + "((" + logicalOp + ")(" + queryTerm + "))*)\\)";
	private String query = "((" + queryTerm + ")|(" + queryTermAssoc + "))" + "((" + logicalOp + ")((" + queryTerm + ")|("
			+ queryTermAssoc + ")))*";
	@SuppressWarnings("unused")
	//TODO validate queries still not working ... rework regex ???
	private Pattern p = Pattern.compile(query);
	
	@Autowired
	ParamsResolver paramsResolver;

	public static void main(String[] args) throws Exception {
		QueryParser test = new QueryParser();
		String attribName1 = "test1";
		String operator1 = "==";
		String operant1 = "\"teststring\"";

		String attribName2 = "test2";
		String operator2 = ">=";
		String operant2 = "12345";
		
		String attribName3 = "test3";
		String operator3 = "!=";
		String operant3 = "\"testst123ring\"";

		String attribName4 = "test4";
		String operator4 = "<=";
		String operant4 = "12345";
		String q = "(" + attribName1 + operator1 + operant1 + ";(" + attribName2 + operator2 + operant2 + "|" + attribName3 + operator3 + operant3 + "))|" + attribName4 + operator4 + operant4;
		System.out.println(q);
		QueryTerm term = test.parseQuery(q, null);
		System.out.println(term);
		// Pattern.compile(test.queryTermUnequal).matcher("brandName!=\"Mercedes\"").group();

	}
	
	
	
	public QueryTerm parseQuery(String input, List<Object> linkHeaders) throws BadRequestException {
//		Matcher matcher = p.matcher(input);
//		if (!matcher.matches()) {
//			throw new BadRequestException();
//		}
		//TODO: regex doesn't validate brackets queries for some reason 
		QueryTerm root = new QueryTerm(linkHeaders, paramsResolver);
		QueryTerm current = root;
		boolean readingAttrib = true;
		String attribName = "";
		String operator = "";
		String operant = "";
		for (byte b : input.getBytes()) {

			if (b == '(') {
				QueryTerm child = new QueryTerm(linkHeaders, paramsResolver);
				current.setFirstChild(child);
				current = child;
				readingAttrib = true;

			} else if (b == ';') {
				QueryTerm next = new QueryTerm(linkHeaders, paramsResolver);
				current.setOperant(operant);
				current.setNext(next);
				current.setNextAnd(true);
				current = next;
				readingAttrib = true;

				operant = "";

			} else if (b == '|') {
				QueryTerm next = new QueryTerm(linkHeaders, paramsResolver);
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



	public GeoqueryRel parseGeoRel(String georel) throws BadRequestException {
		String[] temp = georel.split(";");
		GeoqueryRel result = new GeoqueryRel();
		result.setGeorelOp(temp[0]);
		if(temp[0].equals(NGSIConstants.GEO_REL_NEAR)) {
			if(temp.length < 2) {
				throw new BadRequestException();
			}
			String[] maxMin = temp[1].split("==");
			result.setDistanceType(maxMin[0]);
			result.setDistanceValue(maxMin[1]);
		}
		return result;
	}

}
