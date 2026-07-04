package eu.neclab.ngsildbroker.commons.datatypes.terms;

import com.github.jsonldjava.core.Context;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.QueryParser;
import io.vertx.mutiny.sqlclient.Tuple;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression tests for correlating several ';' (AND) connected q conditions
 * that target the same base attribute (e.g. a Relationship with several
 * datasetId instances such as myRel[id]==X;myRel.subAttr==Y) to
 * the SAME array element/instance, instead of letting each condition match a
 * different instance independently.
 */
public class QQueryTermSameElementTest {

    private static int countOccurrences(String haystack, String needle) {
        int count = 0;
        int idx = 0;
        while ((idx = haystack.indexOf(needle, idx)) != -1) {
            count++;
            idx += needle.length();
        }
        return count;
    }

    private static void assertBalancedParens(String jsonPath) {
        int depth = 0;
        for (char c : jsonPath.toCharArray()) {
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
                assertTrue(depth >= 0, "Unbalanced parens (unexpected closing) in: " + jsonPath);
            }
        }
        assertEquals(0, depth, "Unbalanced parens in: " + jsonPath);
    }

    @Test
    public void relationshipBracketAndDotConditionsAreMergedIntoOneCorrelatedJsonpath() throws ResponseException {
        Context ctx = new Context();
        QQueryTerm term = QueryParser.parseQuery("myRel[id]==\"urn:x\";myRel.subAttr==\"a\"", ctx);
        StringBuilder sql = new StringBuilder();
        Tuple tuple = Tuple.tuple();
        term.toSql(sql, 1, tuple, false, true, null);

        // both conditions must be folded into a single jsonpath bind (root-key check + one jsonpath),
        // not two independent "entity @? ... and entity @? ..." predicates.
        assertEquals(2, tuple.size());
        assertEquals("myRel", tuple.getString(0));
        String jsonPath = tuple.getString(1);
        assertBalancedParens(jsonPath);
        assertTrue(jsonPath.startsWith("$.\"myRel\"[*] ? ("));
        assertTrue(jsonPath.contains("exists(@.\"subAttr\"[*] ? ("), "subAttr sub-attribute must be checked relative to the shared element");
        assertTrue(jsonPath.contains(" && "), "the two conditions must be ANDed inside the same filter");
        assertFalse(sql.toString().contains(" and entity"), "must not fall back to two independent AND'ed predicates");
    }

    @Test
    public void unrelatedAttributesAreNotMerged() throws ResponseException {
        // sanity check: conditions on different base attributes must keep behaving
        // as independent per-condition existence checks.
        Context ctx = new Context();
        QQueryTerm term = QueryParser.parseQuery("temperature==20;humidity==30", ctx);
        StringBuilder sql = new StringBuilder();
        Tuple tuple = Tuple.tuple();
        term.toSql(sql, 1, tuple, false, true, null);

        assertTrue(sql.toString().contains(" and "), "unrelated attributes must still be combined with a plain AND");
        assertEquals("temperature", tuple.getString(0));
        assertEquals("humidity", tuple.getString(7));
    }

    @Test
    public void usageCharacteristicCrossElementMatchingStaysUnchanged() throws ResponseException {
        // regression guard: this mirrors the query tested in api-test.json, which
        // expects name/value pairs to be checked independently within a single
        // property's own structured list value (NOT correlated) - our same-element
        // merge only applies at the outer multi-instance/datasetId level and must
        // not change this existing, tested behavior.
        Context ctx = new Context();
        QQueryTerm term = QueryParser.parseQuery(
                "usageCharacteristic[value]==\"urn:x\";usageCharacteristic[name]==\"productId\"", ctx);
        StringBuilder sql = new StringBuilder();
        Tuple tuple = Tuple.tuple();
        term.toSql(sql, 1, tuple, false, true, null);

        assertEquals(2, tuple.size());
        String jsonPath = tuple.getString(1);
        assertBalancedParens(jsonPath);
        // there is only ever one outer usageCharacteristic instance here, so merging
        // at the outer [*] level is a no-op and the inner name/value pairs are still
        // matched independently within hasValue's own structured array, unchanged.
        assertEquals(1, countOccurrences(jsonPath, "$.\"usageCharacteristic\"[*]"));
    }

    @Test
    public void negatedConditionsAreNeverMerged() throws ResponseException {
        // != / ! must keep using the existing independent per-term handling untouched.
        Context ctx = new Context();
        QQueryTerm term = QueryParser.parseQuery("myRel[id]!=\"urn:x\";myRel.subAttr==\"a\"", ctx);
        StringBuilder sql = new StringBuilder();
        Tuple tuple = Tuple.tuple();
        term.toSql(sql, 1, tuple, false, true, null);

        assertTrue(sql.toString().contains(" and "), "negated terms must fall back to independent AND handling");
    }
}
