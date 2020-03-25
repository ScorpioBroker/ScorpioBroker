package eu.neclab.ngsildbroker.commons.ngsiqueries.test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.BaseProperty;
import eu.neclab.ngsildbroker.commons.datatypes.Property;
import eu.neclab.ngsildbroker.commons.datatypes.PropertyEntry;
import eu.neclab.ngsildbroker.commons.datatypes.QueryTerm;
import eu.neclab.ngsildbroker.commons.exceptions.BadRequestException;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.ngsiqueries.ParamsResolver;
import eu.neclab.ngsildbroker.commons.ngsiqueries.QueryParser;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;

/**
 * URISyntaxExceptions are thrown intentionally since they will never happen in
 * this fully controlled test setup with hand defined URIs
 * 
 * @author hebgen
 *
 */
public class QueryTest {

	private static QueryParser parser;
	private static ParamsResolver paramResolver;

	@BeforeClass
	public static void setup() {
		parser = new QueryParser();
		paramResolver = new ParamsResolver() {
			@Override
			public String expandAttribute(String attribute, List<Object> linkHeaders) throws ResponseException {
				switch (attribute) {
				case NGSIConstants.QUERY_PARAMETER_CREATED_AT:
					return NGSIConstants.NGSI_LD_CREATED_AT;
				case NGSIConstants.QUERY_PARAMETER_MODIFIED_AT:
					return NGSIConstants.NGSI_LD_MODIFIED_AT;
				case NGSIConstants.QUERY_PARAMETER_OBSERVED_AT:
					return NGSIConstants.NGSI_LD_OBSERVED_AT;
				default:
					return "http://mytestprop.org/" + attribute;
				}

			}

		};
	}

	@Test
	public void testObservedModifiedCreated() throws URISyntaxException {
		String dateTime = "2019-04-26T12:23:55Z";
		Property stringProp = new Property();

		stringProp.setId(new URI("http://mytestprop.org/teststring"));
		PropertyEntry propEntry = new PropertyEntry("test", "test");
		stringProp.setSingleEntry(propEntry);
		try {
			propEntry.setCreatedAt(SerializationTools.date2Long(dateTime));
			propEntry.setObservedAt(SerializationTools.date2Long(dateTime));
			propEntry.setModifiedAt(SerializationTools.date2Long(dateTime));
		} catch (Exception e1) {
			throw new AssertionError();
		}
		String qString = "teststring.observedAt==" + dateTime;
		try {
			QueryTerm term = parser.parseQuery(qString, null);
			term.setParamsResolver(paramResolver);
			assertTrue(term.calculate(stringProp));
			qString = "teststring.modifiedAt==" + dateTime;
			term = parser.parseQuery(qString, null);
			term.setParamsResolver(paramResolver);
			assertTrue(term.calculate(stringProp));
			qString = "teststring.createdAt==" + dateTime;
			term = parser.parseQuery(qString, null);
			term.setParamsResolver(paramResolver);
			assertTrue(term.calculate(stringProp));
		} catch (ResponseException e) {
			Assert.fail(e.getLocalizedMessage());
		}

	}

	@Test
	public void testEquals() throws URISyntaxException {

		Property stringProp = new Property();
		try {
			stringProp.setId(new URI("http://mytestprop.org/teststring"));
		} catch (URISyntaxException e) {
			// left Empty intentionally
		}
		stringProp.setSingleEntry(new PropertyEntry("test", "test"));

		String qString = "teststring==\"test\"";
		String qInt = "testint==4";
		String qDouble = "testdouble==123.456";

		String qNumberRange = "testint==1..6";
		String qStringRange = "teststring==\"a\"..\"e\"";

		Property simpleStringProp = new Property();
		simpleStringProp.setId(new URI("http://mytestprop.org/teststring"));
		simpleStringProp.setSingleEntry(new PropertyEntry("test", "d"));

		Property intProp = new Property();
		intProp.setId(new URI("http://mytestprop.org/testint"));
		intProp.setSingleEntry(new PropertyEntry("test", 4));

		Property doubleProp = new Property();
		doubleProp.setId(new URI("http://mytestprop.org/testdouble"));
		doubleProp.setSingleEntry(new PropertyEntry("test", 123.456));

		Property stringListProp = new Property();
		stringListProp.setId(new URI("http://mytestprop.org/teststring"));
		ArrayList<Object> stringList = new ArrayList<Object>();
		stringList.add("something");
		stringList.add("todo");
		stringList.add("test");// success
		stringListProp.setSingleEntry(new PropertyEntry("test", stringList));

		Property intListProp = new Property();
		intListProp.setId(new URI("http://mytestprop.org/testint"));

		ArrayList<Object> intList = new ArrayList<Object>();
		intList.add(23);
		intList.add(42);
		intList.add(123);// success
		intListProp.setSingleEntry(new PropertyEntry("test", intList));

		Property doubleListProp = new Property();
		doubleListProp.setId(new URI("http://mytestprop.org/testdouble"));
		ArrayList<Double> doubleList = new ArrayList<Double>();
		doubleList.add(23.45);
		doubleList.add(42.33);
		doubleList.add(123.456);// success
		doubleListProp.setSingleEntry(new PropertyEntry("test", doubleList));
		try {
			QueryTerm term = parser.parseQuery(qString, null);
			term.setParamsResolver(paramResolver);
			assertTrue(term.calculate(stringProp));

			term = parser.parseQuery(qInt, null);
			term.setParamsResolver(paramResolver);
			assertTrue(term.calculate(intProp));

			term = parser.parseQuery(qDouble, null);
			term.setParamsResolver(paramResolver);
			assertTrue(term.calculate(doubleProp));

			term = parser.parseQuery(qString, null);
			term.setParamsResolver(paramResolver);
			assertTrue(term.calculate(stringProp));

			term = parser.parseQuery(qNumberRange, null);
			term.setParamsResolver(paramResolver);
			assertTrue(term.calculate(intProp));

			term = parser.parseQuery(qStringRange, null);
			term.setParamsResolver(paramResolver);
			assertTrue(term.calculate(simpleStringProp));

			term = parser.parseQuery(qString, null);
			term.setParamsResolver(paramResolver);
			assertTrue(term.calculate(stringListProp));

			term = parser.parseQuery(qInt, null);
			term.setParamsResolver(paramResolver);
			assertFalse(term.calculate(intListProp));
		} catch (ResponseException e) {
			Assert.fail(e.getLocalizedMessage());
		}
	}

	@Test
	public void testUnequals() throws URISyntaxException {
		Property stringProp = new Property();
		stringProp.setId(new URI("http://mytestprop.org/teststring"));
		stringProp.setSingleEntry(new PropertyEntry("test", "test123"));

		String qString = "teststring!=\"test\"";
		String qInt = "testint!=4";
		String qDouble = "testdouble!=123.456";

		String qNumberRange = "testint!=1..6";
		String qStringRange = "teststring!=\"a\"..\"e\"";

		Property simpleStringProp = new Property();
		simpleStringProp.setId(new URI("http://mytestprop.org/teststring"));
		simpleStringProp.setSingleEntry(new PropertyEntry("test", "f"));

		Property intProp = new Property();
		intProp.setId(new URI("http://mytestprop.org/testint"));
		intProp.setSingleEntry(new PropertyEntry("test", 7));

		Property doubleProp = new Property();
		doubleProp.setId(new URI("http://mytestprop.org/testdouble"));
		doubleProp.setSingleEntry(new PropertyEntry("test", 143.456));

		Property stringListFalseProp = new Property();
		stringListFalseProp.setId(new URI("http://mytestprop.org/teststring"));
		ArrayList<Object> stringListFalse = new ArrayList<Object>();
		stringListFalse.add("something");
		stringListFalse.add("todo");
		stringListFalse.add("test");// success
		stringListFalseProp.setSingleEntry(new PropertyEntry("test", stringListFalse));

		Property intListFalseProp = new Property();
		intListFalseProp.setId(new URI("http://mytestprop.org/testint"));

		ArrayList<Object> intListFalse = new ArrayList<Object>();
		intListFalse.add(23);
		intListFalse.add(42);
		intListFalse.add(123);// success
		intListFalseProp.setSingleEntry(new PropertyEntry("test", intListFalse));

		// Property doubleListFalseProp = new Property();
		// doubleListFalseProp.setId(new URI("http://mytestprop.org/testdouble"));
		// ArrayList<Double> doubleListFalse = new ArrayList<Double>();
		// doubleListFalse.add(23.45);
		// doubleListFalse.add(42.33);
		// doubleListFalse.add(123.456);//success
		// doubleListFalseProp.setValue(intListFalse);
		Property stringListProp = new Property();
		stringListProp.setId(new URI("http://mytestprop.org/teststring"));
		ArrayList<Object> stringList = new ArrayList<Object>();
		stringList.add("something");
		stringList.add("todo");
		// stringList.add("test");//success
		stringListProp.setSingleEntry(new PropertyEntry("test", stringList));

		Property intListProp = new Property();
		intListProp.setId(new URI("http://mytestprop.org/testint"));

		ArrayList<Object> intList = new ArrayList<Object>();
		intList.add(23);
		intList.add(42);
		// intList.add(123);//success
		intListProp.setSingleEntry(new PropertyEntry("test", intList));
		try {
			QueryTerm term = parser.parseQuery(qString, null);
			term.setParamsResolver(paramResolver);
			assertTrue(term.calculate(stringProp));

			term = parser.parseQuery(qInt, null);
			term.setParamsResolver(paramResolver);
			assertTrue(term.calculate(intProp));

			term = parser.parseQuery(qDouble, null);
			term.setParamsResolver(paramResolver);
			assertTrue(term.calculate(doubleProp));

			term = parser.parseQuery(qString, null);
			term.setParamsResolver(paramResolver);
			assertTrue(term.calculate(stringProp));

			term = parser.parseQuery(qNumberRange, null);
			term.setParamsResolver(paramResolver);
			assertTrue(term.calculate(intProp));

			term = parser.parseQuery(qStringRange, null);
			term.setParamsResolver(paramResolver);
			assertTrue(term.calculate(simpleStringProp));

			term = parser.parseQuery(qString, null);
			term.setParamsResolver(paramResolver);
			assertFalse(term.calculate(stringListFalseProp));

			term = parser.parseQuery(qInt, null);
			term.setParamsResolver(paramResolver);
			assertTrue(term.calculate(intListFalseProp));

			term = parser.parseQuery(qString, null);
			term.setParamsResolver(paramResolver);
			assertTrue(term.calculate(stringListProp));

			term = parser.parseQuery(qInt, null);
			term.setParamsResolver(paramResolver);
			assertTrue(term.calculate(intListProp));
		} catch (ResponseException e) {
			Assert.fail(e.getLocalizedMessage());
		}
	}

	@Test
	public void testSmallerEquals() throws URISyntaxException {
		Property stringProp = new Property();
		stringProp.setId(new URI("http://mytestprop.org/teststring"));
		stringProp.setSingleEntry(new PropertyEntry("test", "test"));

		Property stringSProp = new Property();
		stringSProp.setId(new URI("http://mytestprop.org/teststring"));
		stringSProp.setSingleEntry(new PropertyEntry("test", "tes"));

		Property stringBProp = new Property();
		stringBProp.setId(new URI("http://mytestprop.org/teststring"));
		stringBProp.setSingleEntry(new PropertyEntry("test", "test123"));

		Property intProp = new Property();
		intProp.setId(new URI("http://mytestprop.org/testint"));
		intProp.setSingleEntry(new PropertyEntry("test", 4));

		Property intSProp = new Property();
		intSProp.setId(new URI("http://mytestprop.org/testint"));
		intSProp.setSingleEntry(new PropertyEntry("test", 3));

		Property intBProp = new Property();
		intBProp.setId(new URI("http://mytestprop.org/testint"));
		intBProp.setSingleEntry(new PropertyEntry("test", 5));
		String qString = "teststring<=\"test\"";
		String qInt = "testint<=4";
		try {
			QueryTerm term = parser.parseQuery(qString, null);
			term.setParamsResolver(paramResolver);
			assertTrue(term.calculate(stringProp));
			assertTrue(term.calculate(stringSProp));
			assertFalse(term.calculate(stringBProp));

			term = parser.parseQuery(qInt, null);
			term.setParamsResolver(paramResolver);
			assertTrue(term.calculate(intProp));
			assertTrue(term.calculate(intSProp));
			assertFalse(term.calculate(intBProp));
		} catch (ResponseException e) {
			Assert.fail(e.getLocalizedMessage());
		}

	}

	@Test
	public void testBiggerEquals() throws URISyntaxException {
		Property stringProp = new Property();
		stringProp.setId(new URI("http://mytestprop.org/teststring"));
		stringProp.setSingleEntry(new PropertyEntry("test", "test"));

		Property stringSProp = new Property();
		stringSProp.setId(new URI("http://mytestprop.org/teststring"));
		stringSProp.setSingleEntry(new PropertyEntry("test", "tes"));

		Property stringBProp = new Property();
		stringBProp.setId(new URI("http://mytestprop.org/teststring"));
		stringBProp.setSingleEntry(new PropertyEntry("test", "test123"));

		Property intProp = new Property();
		intProp.setId(new URI("http://mytestprop.org/testint"));
		intProp.setSingleEntry(new PropertyEntry("test", 4));

		Property intSProp = new Property();
		intSProp.setId(new URI("http://mytestprop.org/testint"));
		intSProp.setSingleEntry(new PropertyEntry("test", 3));

		Property intBProp = new Property();
		intBProp.setId(new URI("http://mytestprop.org/testint"));
		intBProp.setSingleEntry(new PropertyEntry("test", 5));
		String qString = "teststring>=\"test\"";
		String qInt = "testint>=4";
		try {
			QueryTerm term = parser.parseQuery(qString, null);
			term.setParamsResolver(paramResolver);
			assertTrue(term.calculate(stringProp));
			assertFalse(term.calculate(stringSProp));
			assertTrue(term.calculate(stringBProp));

			term = parser.parseQuery(qInt, null);
			term.setParamsResolver(paramResolver);
			assertTrue(term.calculate(intProp));
			assertFalse(term.calculate(intSProp));
			assertTrue(term.calculate(intBProp));
		} catch (ResponseException e) {
			Assert.fail(e.getLocalizedMessage());
		}
	}

	@Test
	public void testSmaller() throws URISyntaxException {
		Property stringProp = new Property();
		stringProp.setId(new URI("http://mytestprop.org/teststring"));
		stringProp.setSingleEntry(new PropertyEntry("test", "test"));

		Property stringSProp = new Property();
		stringSProp.setId(new URI("http://mytestprop.org/teststring"));
		stringSProp.setSingleEntry(new PropertyEntry("test", "tes"));

		Property stringBProp = new Property();
		stringBProp.setId(new URI("http://mytestprop.org/teststring"));
		stringBProp.setSingleEntry(new PropertyEntry("test", "test123"));

		Property intProp = new Property();
		intProp.setId(new URI("http://mytestprop.org/testint"));
		intProp.setSingleEntry(new PropertyEntry("test", 4));

		Property intSProp = new Property();
		intSProp.setId(new URI("http://mytestprop.org/testint"));
		intSProp.setSingleEntry(new PropertyEntry("test", 3));

		Property intBProp = new Property();
		intBProp.setId(new URI("http://mytestprop.org/testint"));
		intBProp.setSingleEntry(new PropertyEntry("test", 5));
		String qString = "teststring<\"test\"";
		String qInt = "testint<4";
		try {
			QueryTerm term = parser.parseQuery(qString, null);
			term.setParamsResolver(paramResolver);
			assertFalse(term.calculate(stringProp));
			assertTrue(term.calculate(stringSProp));
			assertFalse(term.calculate(stringBProp));

			term = parser.parseQuery(qInt, null);
			term.setParamsResolver(paramResolver);
			assertFalse(term.calculate(intProp));
			assertTrue(term.calculate(intSProp));
			assertFalse(term.calculate(intBProp));
		} catch (ResponseException e) {
			Assert.fail(e.getLocalizedMessage());
		}
	}

	@Test
	public void testBigger() throws URISyntaxException {
		Property stringProp = new Property();
		stringProp.setId(new URI("http://mytestprop.org/teststring"));
		stringProp.setSingleEntry(new PropertyEntry("test", "test"));

		Property stringSProp = new Property();
		stringSProp.setId(new URI("http://mytestprop.org/teststring"));
		stringSProp.setSingleEntry(new PropertyEntry("test", "tes"));

		Property stringBProp = new Property();
		stringBProp.setId(new URI("http://mytestprop.org/teststring"));
		stringBProp.setSingleEntry(new PropertyEntry("test", "test123"));

		Property intProp = new Property();
		intProp.setId(new URI("http://mytestprop.org/testint"));
		intProp.setSingleEntry(new PropertyEntry("test", 4));

		Property intSProp = new Property();
		intSProp.setId(new URI("http://mytestprop.org/testint"));
		intSProp.setSingleEntry(new PropertyEntry("test", 3));

		Property intBProp = new Property();
		intBProp.setId(new URI("http://mytestprop.org/testint"));
		intBProp.setSingleEntry(new PropertyEntry("test", 5));
		String qString = "teststring>\"test\"";
		String qInt = "testint>4";
		try {
			QueryTerm term = parser.parseQuery(qString, null);
			term.setParamsResolver(paramResolver);
			assertFalse(term.calculate(stringProp));
			assertFalse(term.calculate(stringSProp));
			assertTrue(term.calculate(stringBProp));

			term = parser.parseQuery(qInt, null);
			term.setParamsResolver(paramResolver);
			assertFalse(term.calculate(intProp));
			assertFalse(term.calculate(intSProp));
			assertTrue(term.calculate(intBProp));
		} catch (ResponseException e) {
			Assert.fail(e.getLocalizedMessage());
		}
	}

	@Test
	public void testPattern() throws URISyntaxException {
		Property stringProp = new Property();
		stringProp.setId(new URI("http://mytestprop.org/teststring"));
		stringProp.setSingleEntry(new PropertyEntry("test", "test32"));

		Property stringNoMatchProp = new Property();
		stringNoMatchProp.setId(new URI("http://mytestprop.org/teststring"));
		stringNoMatchProp.setSingleEntry(new PropertyEntry("test", "test"));

		String qString = "teststring~=\\w+32";
		try {
			QueryTerm term = parser.parseQuery(qString, null);
			term.setParamsResolver(paramResolver);
			assertTrue(term.calculate(stringProp));
			assertFalse(term.calculate(stringNoMatchProp));
		} catch (ResponseException e) {
			Assert.fail(e.getLocalizedMessage());
		}
	}

	@Test
	public void testNotPattern() throws Exception {
		Property stringProp = new Property();
		stringProp.setId(new URI("http://mytestprop.org/teststring"));
		stringProp.setSingleEntry(new PropertyEntry("test", "test32"));

		Property stringNoMatchProp = new Property();
		stringNoMatchProp.setId(new URI("http://mytestprop.org/teststring"));
		stringNoMatchProp.setSingleEntry(new PropertyEntry("test", "test"));

		String qString = "teststring!~=\\w+32";
		try {
			QueryTerm term = parser.parseQuery(qString, null);
			term.setParamsResolver(paramResolver);
			assertFalse(term.calculate(stringProp));
			assertTrue(term.calculate(stringNoMatchProp));
		} catch (

		BadRequestException e) {
			Assert.fail(e.getLocalizedMessage());
		}
	}

	@Test
	public void testCompoundValueQuery() throws URISyntaxException {
		String q = "testattrib[level1]==1111";

		Property testProp = new Property();
		testProp.setId(new URI("http://mytestprop.org/testattrib"));
		HashMap<String, List<Object>> value = new HashMap<String, List<Object>>();
		ArrayList<Object> valueList = new ArrayList<Object>();
		valueList.add(1111);
		value.put("http://mytestprop.org/level1", valueList);
		testProp.setSingleEntry(new PropertyEntry("test", value));
		try {
			QueryTerm term = parser.parseQuery(q, null);
			term.setParamsResolver(paramResolver);
			assertTrue(term.calculate(testProp));

			q = "testattrib[level1][level2]==2222";
			testProp = new Property();
			testProp.setId(new URI("http://mytestprop.org/testattrib"));
			HashMap<String, List<Object>> value2 = new HashMap<String, List<Object>>();
			ArrayList<Object> valueList2 = new ArrayList<Object>();
			valueList2.add(2222);
			value2.put("http://mytestprop.org/level2", valueList2);
			value = new HashMap<String, List<Object>>();
			valueList = new ArrayList<Object>();
			valueList.add(value2);
			value.put("http://mytestprop.org/level1", valueList);
			testProp.setSingleEntry(new PropertyEntry("test", value));
			term = parser.parseQuery(q, null);
			term.setParamsResolver(paramResolver);
			assertTrue(term.calculate(testProp));
//not a possible query at the moment
			/*
			 * q = "testattrib.subattrib[level1][level2]==3333"; Property topProp = new
			 * Property(); topProp.setId(new URI("http://mytestprop.org/testattrib"));
			 * 
			 * testProp = new Property(); testProp.setId(new
			 * URI("http://mytestprop.org/subattrib")); value2 = new HashMap<String,
			 * List<Object>>(); valueList2 = new ArrayList<Object>(); valueList2.add(3333);
			 * value2.put("http://mytestprop.org/level2", valueList2); value = new
			 * HashMap<String, List<Object>>(); valueList = new ArrayList<Object>();
			 * valueList.add(value2); value.put("http://mytestprop.org/level1", valueList);
			 * PropertyEntry entry = new PropertyEntry("test", value);
			 * testProp.setSingleEntry(entry);
			 * 
			 * List<Property> properties = new ArrayList<Property>();
			 * properties.add(testProp); entry.setProperties(properties); term =
			 * parser.parseQuery(q, null); term.setParamsResolver(paramResolver);
			 * assertTrue(term.calculate(topProp));
			 */
		} catch (ResponseException e) {
			Assert.fail(e.getLocalizedMessage());
		}
	}

	@Test
	public void testMultiQuery() throws URISyntaxException {
		String q = "(test1==\"teststring\";(test2>=12345|test3!=\"testst123ring\"))|test4<=12345";
		Property test1 = new Property();
		test1.setId(new URI("http://mytestprop.org/test1"));
		test1.setSingleEntry(new PropertyEntry("test", "teststring"));
		Property test1Not = new Property();
		test1Not.setId(new URI("http://mytestprop.org/test1"));
		test1Not.setSingleEntry(new PropertyEntry("test", "teststringasdasdas"));

		Property test2 = new Property();
		test2.setId(new URI("http://mytestprop.org/test2"));
		test2.setSingleEntry(new PropertyEntry("test", 12345));
		Property test2Not = new Property();
		test2Not.setId(new URI("http://mytestprop.org/test2"));
		test2Not.setSingleEntry(new PropertyEntry("test", 1234));

		Property test3 = new Property();
		test3.setId(new URI("http://mytestprop.org/test3"));
		test3.setSingleEntry(new PropertyEntry("test", "teststring"));
		Property test3Not = new Property();
		test3Not.setId(new URI("http://mytestprop.org/test3"));
		test3Not.setSingleEntry(new PropertyEntry("test", "testst123ring"));

		Property test4 = new Property();
		test4.setId(new URI("http://mytestprop.org/test4"));
		test4.setSingleEntry(new PropertyEntry("test", 12345));
		Property test4Not = new Property();
		test4Not.setId(new URI("http://mytestprop.org/test4"));
		test4Not.setSingleEntry(new PropertyEntry("test", 123456));

		ArrayList<BaseProperty> allValid = new ArrayList<BaseProperty>();
		allValid.add(test1);
		allValid.add(test2);
		allValid.add(test3);
		allValid.add(test4);

		ArrayList<BaseProperty> orStillValid = new ArrayList<BaseProperty>();
		orStillValid.add(test1);
		orStillValid.add(test2);
		orStillValid.add(test3Not);
		orStillValid.add(test4Not);

		ArrayList<BaseProperty> orStillValid2 = new ArrayList<BaseProperty>();
		orStillValid2.add(test1Not);
		orStillValid2.add(test2Not);
		orStillValid2.add(test3Not);
		orStillValid2.add(test4);

		ArrayList<BaseProperty> notValid1 = new ArrayList<BaseProperty>();
		notValid1.add(test1Not);
		notValid1.add(test2);
		notValid1.add(test3Not);
		notValid1.add(test4Not);

		ArrayList<BaseProperty> notValid2 = new ArrayList<BaseProperty>();
		notValid2.add(test1);
		notValid2.add(test2Not);
		notValid2.add(test3Not);
		notValid2.add(test4Not);
		try {
			QueryTerm term = parser.parseQuery(q, null);
			term.setParamsResolver(paramResolver);
			assertTrue(term.calculate(allValid));
			assertTrue(term.calculate(orStillValid));
			assertTrue(term.calculate(orStillValid2));
			assertFalse(term.calculate(notValid1));
			assertFalse(term.calculate(notValid2));
		} catch (ResponseException e) {
			Assert.fail(e.getLocalizedMessage());
		}
	}
}
