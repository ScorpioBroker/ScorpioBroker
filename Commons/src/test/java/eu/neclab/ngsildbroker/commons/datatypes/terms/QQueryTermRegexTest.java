package eu.neclab.ngsildbroker.commons.datatypes.terms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.github.jsonldjava.core.Context;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.QueryParser;
import io.vertx.mutiny.sqlclient.Tuple;

/**
 * Pattern operations (~= / !~=) in q: the regex has to reach the regex engine
 * unchanged, even if it contains q syntax characters.
 */
public class QQueryTermRegexTest {

	private static QQueryTerm parse(String q) throws ResponseException {
		return QueryParser.parseQuery(q, new Context());
	}

	private static String jsonPathArgs(QQueryTerm term) {
		StringBuilder sql = new StringBuilder();
		Tuple tuple = Tuple.tuple();
		term.toSql(sql, 1, tuple, false, true, null);
		return tuple.deepToString();
	}

	private static String tempSqlAndArgs(QQueryTerm term) {
		StringBuilder sql = new StringBuilder();
		Tuple tuple = Tuple.tuple();
		term.toTempSql(sql, 1, tuple, false, true);
		return sql + " " + tuple.deepToString();
	}

	private static void assertSingleTerm(QQueryTerm term, String attribute, String operator, String operant) {
		assertNull(term.getFirstChild(), "unexpected child term: " + term);
		assertNull(term.getNext(), "unexpected next term: " + term);
		assertEquals(attribute, term.getAttribute());
		assertEquals(operator, term.getOperator());
		assertEquals(operant, term.getOperant());
	}

	private static Map<String, Object> entityWithValue(String attrib, Object value) {
		return Map.of(attrib, List.of(Map.of(NGSIConstants.NGSI_LD_HAS_VALUE, List.of(Map.of(NGSIConstants.JSON_LD_VALUE, value)))));
	}

	// ---- parsing ----

	@Test
	public void simplePatternStillParses() throws ResponseException {
		assertSingleTerm(parse("status~=\"^active$\""), "status", "~=", "\"^active$\"");
	}

	@Test
	public void negativeLookaheadIsKeptInOneTerm() throws ResponseException {
		assertSingleTerm(parse("status~=\"^(?!active$).*\""), "status", "~=", "\"^(?!active$).*\"");
	}

	@Test
	public void alternationIsNotAnOr() throws ResponseException {
		assertSingleTerm(parse("status~=\"foo|bar\""), "status", "~=", "\"foo|bar\"");
	}

	@Test
	public void semicolonAndBracesInsidePattern() throws ResponseException {
		assertSingleTerm(parse("status~=\"a;b{1,3}\""), "status", "~=", "\"a;b{1,3}\"");
	}

	@Test
	public void plusIsNotDecodedToSpace() throws ResponseException {
		assertSingleTerm(parse("status~=\"^\\d+$\""), "status", "~=", "\"^\\d+$\"");
	}

	@Test
	public void escapedQuoteInsidePattern() throws ResponseException {
		assertSingleTerm(parse("name~=\"a\\\"(b)\""), "name", "~=", "\"a\\\"(b)\"");
	}

	@Test
	public void notPatternWithGroup() throws ResponseException {
		assertSingleTerm(parse("status!~=\"^(active|pending)$\""), "status", "!~=", "\"^(active|pending)$\"");
	}

	@Test
	public void quotedPatternCombinedWithOtherTerms() throws ResponseException {
		QQueryTerm term = parse("status~=\"(a|b)\";temperature>20|name==\"x;y\"");
		assertEquals("status", term.getAttribute());
		assertEquals("\"(a|b)\"", term.getOperant());
		QQueryTerm second = term.getNext();
		assertEquals("temperature", second.getAttribute());
		assertEquals(">", second.getOperator());
		assertEquals("20", second.getOperant());
		QQueryTerm third = second.getNext();
		assertEquals("name", third.getAttribute());
		assertEquals("\"x;y\"", third.getOperant());
		assertNull(third.getNext());
	}

	@Test
	public void quotedPatternInsideGroup() throws ResponseException {
		QQueryTerm term = parse("(status~=\"(a|b)\"|temperature>20)");
		QQueryTerm child = term.getFirstChild();
		assertEquals("status", child.getAttribute());
		assertEquals("\"(a|b)\"", child.getOperant());
		assertEquals("temperature", child.getNext().getAttribute());
	}

	@Test
	public void commaAndDoubleDotAreNotListOrRange() throws ResponseException {
		assertSingleTerm(parse("status~=\"a{1,3}\""), "status", "~=", "\"a{1,3}\"");
		assertSingleTerm(parse("status~=\".*foo..*\""), "status", "~=", "\".*foo..*\"");
		assertSingleTerm(parse("status~=.*foo..*"), "status", "~=", ".*foo..*");
	}

	@Test
	public void unterminatedQuoteIsRejected() {
		assertThrows(ResponseException.class, () -> parse("status~=\"(abc"));
	}

	@Test
	public void percentIsNotDecodedAgain() throws ResponseException {
		// the controller already decoded the value once, %41 must stay literal
		assertSingleTerm(parse("status~=\"100%41\""), "status", "~=", "\"100%41\"");
	}

	@Test
	public void nonRegexQueriesAreUnchanged() throws ResponseException {
		QQueryTerm term = parse("temperature==20..30;name==\"a\",\"b\"");
		assertEquals("20..30", term.getOperant());
		assertEquals("\"a\",\"b\"", term.getNext().getOperant());
		assertThrows(ResponseException.class, () -> parse("temperature>=1,2"));
	}

	// ---- sql ----

	@Test
	public void lookaheadReachesLikeRegex() throws ResponseException {
		String args = jsonPathArgs(parse("status~=\"^(?!active$).*\""));
		assertTrue(args.contains("$.\"status\"[*]"), args);
		assertTrue(args.contains("like_regex \"^(?!active$).*\""), args);
	}

	@Test
	public void alternationReachesLikeRegexAsSinglePattern() throws ResponseException {
		String args = jsonPathArgs(parse("status~=\"foo|bar\""));
		assertTrue(args.contains("like_regex \"foo|bar\""), args);
		assertFalse(args.contains("bar\"\""), args);
	}

	@Test
	public void backslashesAreEscapedForJsonPath() throws ResponseException {
		String args = jsonPathArgs(parse("status~=\"^\\d+$\""));
		assertTrue(args.contains("like_regex \"^\\\\d+$\""), args);
	}

	@Test
	public void quotesAreEscapedForJsonPath() throws ResponseException {
		String args = jsonPathArgs(parse("name~=\"a\\\"b\""));
		// pattern a\"b -> jsonpath literal "a\\\"b"
		assertTrue(args.contains("like_regex \"a\\\\\\\"b\""), args);
	}

	@Test
	public void commaPatternIsNotSplit() throws ResponseException {
		String args = jsonPathArgs(parse("status~=\"a{1,3}\""));
		assertTrue(args.contains("like_regex \"a{1,3}\""), args);
	}

	@Test
	public void unquotedPatternStillWorks() throws ResponseException {
		String args = jsonPathArgs(parse("testrelationship~=.*:testref1"));
		assertTrue(args.contains("like_regex \".*:testref1\""), args);
	}

	@Test
	public void notPatternReachesLikeRegex() throws ResponseException {
		String args = jsonPathArgs(parse("status!~=\"^(active|pending)$\""));
		assertTrue(args.contains("like_regex \"^(active|pending)$\""), args);
	}

	@Test
	public void temporalQueryReachesLikeRegex() throws ResponseException {
		String args = tempSqlAndArgs(parse("status~=\"^(?!active$).*\""));
		assertTrue(args.contains("like_regex \"^(?!active$).*\""), args);
	}

	// ---- in memory evaluation (subscriptions) ----

	@Test
	public void inMemoryLookahead() throws ResponseException {
		QQueryTerm term = parse("status~=\"^(?!active$).*\"");
		assertFalse(term.calculate(entityWithValue("status", "active"), null));
		assertTrue(term.calculate(entityWithValue("status", "inactive"), null));
	}

	@Test
	public void inMemoryAlternationAndQuantifiers() throws ResponseException {
		QQueryTerm term = parse("status~=\"(foo|bar)\\d{1,3}\"");
		assertTrue(term.calculate(entityWithValue("status", "bar12"), null));
		assertFalse(term.calculate(entityWithValue("status", "baz12"), null));
	}

	@Test
	public void inMemoryDoubleDotIsNotARange() throws ResponseException {
		QQueryTerm term = parse("status~=\".*foo..*\"");
		assertTrue(term.calculate(entityWithValue("status", "xfooab"), null));
		assertFalse(term.calculate(entityWithValue("status", "xfoo"), null));
	}

	@Test
	public void inMemoryNotPattern() throws ResponseException {
		QQueryTerm term = parse("status!~=\"^(active|pending)$\"");
		assertFalse(term.calculate(entityWithValue("status", "pending"), null));
		assertTrue(term.calculate(entityWithValue("status", "closed"), null));
	}

	@Test
	public void inMemoryNumericValue() throws ResponseException {
		assertTrue(parse("intproperty~=\"^1\\d+$\"").calculate(entityWithValue("intproperty", 123), null));
	}

	@Test
	public void inMemoryInvalidPatternDoesNotMatch() throws ResponseException {
		QQueryTerm term = parse("status~=\"(abc\"");
		assertFalse(term.calculate(entityWithValue("status", "(abc"), null));
	}
}
