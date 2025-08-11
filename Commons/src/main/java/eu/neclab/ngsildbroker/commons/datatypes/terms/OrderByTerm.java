package eu.neclab.ngsildbroker.commons.datatypes.terms;

import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import com.github.jsonldjava.core.Context;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.sqlclient.Tuple;

public class OrderByTerm {
    Set<String> orderTerms;
    String collation;
    String orderFrom;
    boolean asc;
    Context context;

    public Set<String> getOrderTerms() {
        return orderTerms;
    }

    public void setOrderTerms(Set<String> orderTerms) {
        this.orderTerms = orderTerms;
    }

    public String getCollation() {
        return collation;
    }

    public void setCollation(String collation) {
        this.collation = collation;
    }

    public String getOrderFrom() {
        return orderFrom;
    }

    public void setOrderFrom(String orderFrom) {
        this.orderFrom = orderFrom;
    }

    public boolean isAsc() {
        return asc;
    }

    public void setAsc(boolean asc) {
        this.asc = asc;
    }

    public void toSqlOrderValue(StringBuilder sql, int dollar, Tuple tuple) {
        for (String orderTerm : orderTerms) {
            String[] split = StringUtils.split(orderTerm, '.');
            switch (orderTerm) {
                case NGSIConstants.JSON_LD_ID:
                    sql.append("ID");
                    break;
                case NGSIConstants.JSON_LD_TYPE:
                    sql.append("E_TYPES");
                    break;
                case NGSIConstants.NGSI_LD_LOCATION:
                    if (orderFrom != null) {
                        sql.append("ST_DISTANCE(");
                    }
                    sql.append("LOCATION");
                    if (orderFrom != null) {
                        sql.append(", ST_SetSRID(ST_GeomFromGeoJSON($");
                        sql.append(dollar);
                        dollar++;
                        tuple.addJsonObject(getGeoJson());
                        sql.append("), 4326))");
                    }
                    break;
                case NGSIConstants.NGSI_LD_CREATED_AT:
                    sql.append("CREATEDAT");
                    break;
                case NGSIConstants.NGSI_LD_MODIFIED_AT:
                    sql.append("MODIFIEDAT");
                    break;

                default:
                    sql.append("CASE WHEN ");
                    break;
            }
        }

    }

    private JsonObject getGeoJson() {
        // TODO Auto-generated method; stub
        throw new UnsupportedOperationException("Unimplemented method 'getGeoJson'");
    }

    public void toSqlOrder(StringBuilder sql) {
        sql.append("ORDER BY orderValue ");
        if (asc) {
            sql.append("ASC");
        } else {
            sql.append("DESC");
        }
    }

    public Context getContext() {
        return context;
    }

    public void setContext(Context context) {
        this.context = context;
    }
}
