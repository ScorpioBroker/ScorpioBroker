package eu.neclab.ngsildbroker.commons.ngsiqueries.test;

import static org.junit.Assert.assertEquals;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import eu.neclab.ngsildbroker.commons.datatypes.QueryTerm;
import eu.neclab.ngsildbroker.commons.exceptions.BadRequestException;
import eu.neclab.ngsildbroker.commons.ngsiqueries.QueryParser;

public class QueryParserTests {

	private static QueryParser parser;

	@BeforeClass
	public static void setup() {
		parser = new QueryParser();
	}

	@Test
	public void testSingleQuery() {
		String attribName = "test1";
		String operator = "==";
		String operant = "\"teststring\"";
		String q = attribName + operator + operant;
		QueryTerm expected = new QueryTerm(null, null);
		expected.setAttribute(attribName);
		expected.setOperator(operator);
		expected.setOperant(operant);

		QueryTerm actual;
		try {
			actual = parser.parseQuery(q, null);
			assertEquals(expected, actual);
		} catch (BadRequestException e) {
			Assert.fail(e.getLocalizedMessage());
		}

	}

	@Test
	public void testMultiAndQuery() {
		String attribName1 = "test1";
		String operator1 = "==";
		String operant1 = "\"teststring\"";

		String attribName2 = "test1";
		String operator2 = ">=";
		String operant2 = "12345";

		QueryTerm expected = new QueryTerm(null, null);
		expected.setAttribute(attribName1);
		expected.setOperator(operator1);
		expected.setOperant(operant1);
		expected.setNextAnd(true);

		QueryTerm expectedNext = new QueryTerm(null, null);
		expectedNext.setAttribute(attribName2);
		expectedNext.setOperator(operator2);
		expectedNext.setOperant(operant2);
		expected.setNext(expectedNext);

		String q = attribName1 + operator1 + operant1 + ";" + attribName2 + operator2 + operant2;
		try {
			QueryTerm actual = parser.parseQuery(q, null);
			assertEquals(expected, actual);
		} catch (BadRequestException e) {
			Assert.fail(e.getLocalizedMessage());
		}
	}

	@Test
	public void testMultiOrQuery() {
		String attribName1 = "test1";
		String operator1 = "==";
		String operant1 = "\"teststring\"";

		String attribName2 = "test2";
		String operator2 = ">=";
		String operant2 = "12345";

		QueryTerm expected = new QueryTerm(null, null);
		expected.setAttribute(attribName1);
		expected.setOperator(operator1);
		expected.setOperant(operant1);
		expected.setNextAnd(false);

		QueryTerm expectedNext = new QueryTerm(null, null);
		expectedNext.setAttribute(attribName2);
		expectedNext.setOperator(operator2);
		expectedNext.setOperant(operant2);
		expected.setNext(expectedNext);

		String q = attribName1 + operator1 + operant1 + "|" + attribName2 + operator2 + operant2;
		try {
			QueryTerm actual = parser.parseQuery(q, null);
			assertEquals(expected, actual);
		} catch (BadRequestException e) {
			Assert.fail(e.getLocalizedMessage());
		}
	}

	@Test
	public void testMultiMixedAndOrQuery() {
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

		QueryTerm expected = new QueryTerm(null, null);
		expected.setAttribute(attribName1);
		expected.setOperator(operator1);
		expected.setOperant(operant1);
		expected.setNextAnd(false);

		QueryTerm expectedNext = new QueryTerm(null, null);
		expectedNext.setAttribute(attribName2);
		expectedNext.setOperator(operator2);
		expectedNext.setOperant(operant2);
		expected.setNext(expectedNext);

		QueryTerm expectedNext1 = new QueryTerm(null, null);
		expectedNext1.setAttribute(attribName3);
		expectedNext1.setOperator(operator3);
		expectedNext1.setOperant(operant3);
		expectedNext.setNext(expectedNext1);
		expectedNext.setNextAnd(true);

		QueryTerm expectedNext2 = new QueryTerm(null, null);
		expectedNext2.setAttribute(attribName4);
		expectedNext2.setOperator(operator4);
		expectedNext2.setOperant(operant4);
		expectedNext1.setNext(expectedNext2);
		expectedNext1.setNextAnd(false);

		String q = attribName1 + operator1 + operant1 + "|" + attribName2 + operator2 + operant2 + ";" + attribName3
				+ operator3 + operant3 + "|" + attribName4 + operator4 + operant4;
		try {
			QueryTerm actual = parser.parseQuery(q, null);
			assertEquals(expected, actual);
		} catch (BadRequestException e) {
			Assert.fail(e.getLocalizedMessage());
		}
	}

	@Test
	public void testBracketsQuery() {

		String attribName2 = "test2";
		String operator2 = ">=";
		String operant2 = "12345";

		String attribName3 = "test3";
		String operator3 = "!=";
		String operant3 = "\"testst123ring\"";

		QueryTerm expected = new QueryTerm(null, null);

		QueryTerm expectedFirstChild = new QueryTerm(null, null);
		expectedFirstChild.setAttribute(attribName2);
		expectedFirstChild.setOperator(operator2);
		expectedFirstChild.setOperant(operant2);
		expected.setFirstChild(expectedFirstChild);

		QueryTerm expectedFirstChildNext = new QueryTerm(null, null);
		expectedFirstChildNext.setAttribute(attribName3);
		expectedFirstChildNext.setOperator(operator3);
		expectedFirstChildNext.setOperant(operant3);
		expectedFirstChild.setNext(expectedFirstChildNext);
		expectedFirstChild.setNextAnd(true);
		String q = "(" + attribName2 + operator2 + operant2 + ";" + attribName3 + operator3 + operant3 + ")";
		try {
			QueryTerm actual = parser.parseQuery(q, null);
			assertEquals(expected, actual);
		} catch (BadRequestException e) {
			Assert.fail(e.getLocalizedMessage());
		}
	}

	@Test
	public void testMultiBracketsQuery() {

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

		String q = "(" + attribName1 + operator1 + operant1 + ";(" + attribName2 + operator2 + operant2 + "|"
				+ attribName3 + operator3 + operant3 + "))|" + attribName4 + operator4 + operant4;
		QueryTerm expected = new QueryTerm(null, null);
		QueryTerm expected1 = new QueryTerm(null, null);
		expected1.setAttribute(attribName1);
		expected1.setOperator(operator1);
		expected1.setOperant(operant1);
		expected1.setNextAnd(true);
		expected.setFirstChild(expected1);
		QueryTerm expected2 = new QueryTerm(null, null);
		expected1.setNext(expected2);

		QueryTerm expected3 = new QueryTerm(null, null);
		expected3.setAttribute(attribName2);
		expected3.setOperator(operator2);
		expected3.setOperant(operant2);
		expected3.setNextAnd(false);
		expected2.setFirstChild(expected3);
		QueryTerm expected4 = new QueryTerm(null, null);
		expected4.setAttribute(attribName3);
		expected4.setOperator(operator3);
		expected4.setOperant(operant3);
		expected3.setNext(expected4);

		QueryTerm expected5 = new QueryTerm(null, null);
		expected5.setAttribute(attribName4);
		expected5.setOperator(operator4);
		expected5.setOperant(operant4);
		expected.setNextAnd(false);
		expected.setNext(expected5);
		try {
			QueryTerm actual = parser.parseQuery(q, null);

			assertEquals(expected, actual);
		} catch (BadRequestException e) {
			Assert.fail(e.getLocalizedMessage());
		}

	}

}
