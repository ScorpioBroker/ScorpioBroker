package eu.neclab.ngsildbroker.commons.datatypes.terms;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OrderByTermTest {

    @Test
    public void testTermEntryOrderDirectionAsc() {
        OrderByTerm orderByTerm = new OrderByTerm();
        orderByTerm.addTerm("temperature", null, null, "asc", null);

        StringBuilder sql = new StringBuilder();
        orderByTerm.toSqlOrder(sql);

        assertTrue(sql.toString().contains("ASC"), "Expected ASC in SQL but got: " + sql);
        assertFalse(sql.toString().contains("DESC"), "Expected no DESC in SQL but got: " + sql);
    }

    @Test
    public void testTermEntryOrderDirectionDesc() {
        OrderByTerm orderByTerm = new OrderByTerm();
        orderByTerm.addTerm("temperature", null, null, "desc", null);

        StringBuilder sql = new StringBuilder();
        orderByTerm.toSqlOrder(sql);

        assertTrue(sql.toString().contains("DESC"), "Expected DESC in SQL but got: " + sql);
        assertFalse(sql.toString().contains("ASC"), "Expected no ASC in SQL but got: " + sql);
    }

    @Test
    public void testTermEntryOrderDirectionNull() {
        OrderByTerm orderByTerm = new OrderByTerm();
        orderByTerm.addTerm("temperature", null, null, null, null);

        StringBuilder sql = new StringBuilder();
        orderByTerm.toSqlOrder(sql);

        assertFalse(sql.toString().contains("ASC"), "Expected no ASC in SQL but got: " + sql);
        assertFalse(sql.toString().contains("DESC"), "Expected no DESC in SQL but got: " + sql);
    }

}
