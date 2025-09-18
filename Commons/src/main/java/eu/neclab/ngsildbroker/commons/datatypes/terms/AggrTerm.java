package eu.neclab.ngsildbroker.commons.datatypes.terms;

import java.io.Serializable;
import java.util.Set;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import io.vertx.mutiny.sqlclient.Tuple;

public class AggrTerm implements Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = -4795426642930378574L;
    private String period;
    private Set<String> aggrFunctions;

    public AggrTerm() {
        // for serialization
    }

    public String getPeriod() {
        return period;
    }

    public void setPeriod(String period) {
        this.period = period;
    }

    public Set<String> getAggrFunctions() {
        return aggrFunctions;
    }

    public void setAggrFunctions(Set<String> aggrFunctions) {
        this.aggrFunctions = aggrFunctions;
    }

    public int toSqlBla(StringBuilder sql, Tuple tuple, int dollar, String temporalProperty, String from, String to) {
        int fromDollar;
        if (from != null) {
            tuple.addString(from);
            fromDollar = dollar;
            dollar++;
        } else {
            fromDollar = -1;
        }

        int toDollar;
        if (to != null) {
            tuple.addString(to);
            toDollar = dollar;
            dollar++;
        } else {
            toDollar = -1;
        }

        int periodDollar;
        if (period != null) {
            tuple.addString(period);
            periodDollar = dollar;
            dollar++;
        } else {
            periodDollar = -1;
        }

        sql.append(
                "period_stats AS (SELECT ei.id, ei.e_types, ei.r_createdat, ei.r_modifiedat, ei.r_deletedat, ei.scope_entry, teai.attributeid, (teai.data #>> '{");
        sql.append(NGSIConstants.JSON_LD_TYPE);
        sql.append(",0}') as attr_type,");
        if (period != null) {
            sql.append(" pr.period,");
        }

        for (String aggrFunction : aggrFunctions) {
            StringBuilder tmp = new StringBuilder(128);
            switch (aggrFunction) {
                case NGSIConstants.AGGR_METH_SUM:
                    tmp.append("SUM(CASE WHEN jsonb_typeof(teai.data #> '{");
                    tmp.append(NGSIConstants.NGSI_LD_HAS_VALUE);
                    tmp.append(",0,");
                    tmp.append(NGSIConstants.JSON_LD_VALUE);
                    tmp.append("}') = 'number' THEN (teai.data #> '{");
                    tmp.append(NGSIConstants.NGSI_LD_HAS_VALUE);
                    tmp.append(",0,");
                    tmp.append(NGSIConstants.JSON_LD_VALUE);
                    tmp.append("}')::numeric WHEN jsonb_typeof(teai.data #> '{");
                    tmp.append(NGSIConstants.NGSI_LD_HAS_VALUE);
                    tmp.append(",0,");
                    tmp.append(NGSIConstants.JSON_LD_VALUE);
                    tmp.append("}') = 'boolean' THEN (teai.data #> '{");
                    tmp.append(NGSIConstants.NGSI_LD_HAS_VALUE);
                    tmp.append(",0,");
                    tmp.append(NGSIConstants.JSON_LD_VALUE);
                    tmp.append("}')::numeric WHEN jsonb_typeof(teai.data #> '{");
                    tmp.append(NGSIConstants.NGSI_LD_HAS_VALUE);
                    tmp.append(",0,");
                    tmp.append(NGSIConstants.JSON_LD_VALUE);
                    tmp.append("}') = 'array' THEN jsonb_array_length(teai.data #> '{");
                    tmp.append(NGSIConstants.NGSI_LD_HAS_VALUE);
                    tmp.append(",0,");
                    tmp.append(NGSIConstants.JSON_LD_VALUE);
                    tmp.append("}')::numeric ELSE NULL END)");
                    buildAggrValue(sql, "sum", fromDollar, toDollar, periodDollar, tmp.toString(), temporalProperty);
                    break;
                case NGSIConstants.AGGR_METH_MIN:

                    tmp.append("MIN(CASE WHEN jsonb_typeof(teai.data #> '{");
                    tmp.append(NGSIConstants.NGSI_LD_HAS_VALUE);
                    tmp.append(",0,");
                    tmp.append(NGSIConstants.JSON_LD_VALUE);
                    tmp.append("}') IN ('number', 'boolean', 'string') THEN (teai.data #>> '{");
                    tmp.append(NGSIConstants.NGSI_LD_HAS_VALUE);
                    tmp.append(",0,");
                    tmp.append(NGSIConstants.JSON_LD_VALUE);
                    tmp.append("}') WHEN jsonb_typeof(teai.data #> '{");
                    tmp.append(NGSIConstants.NGSI_LD_HAS_VALUE);
                    tmp.append(",0,");
                    tmp.append(NGSIConstants.JSON_LD_VALUE);
                    tmp.append("}') = 'array' THEN jsonb_array_length(teai.data #> '{");
                    tmp.append(NGSIConstants.NGSI_LD_HAS_VALUE);
                    tmp.append(",0,");
                    tmp.append(NGSIConstants.JSON_LD_VALUE);
                    tmp.append("}')::text ELSE NULL END)");
                    buildAggrValue(sql, "min", fromDollar, toDollar, periodDollar, tmp.toString(), temporalProperty);

                    break;
                case NGSIConstants.AGGR_METH_MAX:

                    tmp.append("MAX(CASE WHEN jsonb_typeof(teai.data #> '{");
                    tmp.append(NGSIConstants.NGSI_LD_HAS_VALUE);
                    tmp.append(",0,");
                    tmp.append(NGSIConstants.JSON_LD_VALUE);
                    tmp.append("}') IN ('number', 'boolean', 'string') THEN (teai.data #>> '{");
                    tmp.append(NGSIConstants.NGSI_LD_HAS_VALUE);
                    tmp.append(",0,");
                    tmp.append(NGSIConstants.JSON_LD_VALUE);
                    tmp.append("}') WHEN jsonb_typeof(teai.data #> '{");
                    tmp.append(NGSIConstants.NGSI_LD_HAS_VALUE);
                    tmp.append(",0,");
                    tmp.append(NGSIConstants.JSON_LD_VALUE);
                    tmp.append("}') = 'array' THEN jsonb_array_length(teai.data #> '{");
                    tmp.append(NGSIConstants.NGSI_LD_HAS_VALUE);
                    tmp.append(",0,");
                    tmp.append(NGSIConstants.JSON_LD_VALUE);
                    tmp.append("}')::text ELSE NULL END)");
                    buildAggrValue(sql, "max", fromDollar, toDollar, periodDollar, tmp.toString(), temporalProperty);
                    break;
                case NGSIConstants.AGGR_METH_AVG:
                    tmp.append("AVG(CASE WHEN jsonb_typeof(teai.data #> '{");
                    tmp.append(NGSIConstants.NGSI_LD_HAS_VALUE);
                    tmp.append(",0,");
                    tmp.append(NGSIConstants.JSON_LD_VALUE);
                    tmp.append("}') = 'number' THEN (teai.data #> '{");
                    tmp.append(NGSIConstants.NGSI_LD_HAS_VALUE);
                    tmp.append(",0,");
                    tmp.append(NGSIConstants.JSON_LD_VALUE);
                    tmp.append("}')::numeric WHEN jsonb_typeof(teai.data #> '{");
                    tmp.append(NGSIConstants.NGSI_LD_HAS_VALUE);
                    tmp.append(",0,");
                    tmp.append(NGSIConstants.JSON_LD_VALUE);
                    tmp.append("}') = 'boolean' THEN (teai.data #> '{");
                    tmp.append(NGSIConstants.NGSI_LD_HAS_VALUE);
                    tmp.append(",0,");
                    tmp.append(NGSIConstants.JSON_LD_VALUE);
                    tmp.append("}')::numeric WHEN jsonb_typeof(teai.data #> '{");
                    tmp.append(NGSIConstants.NGSI_LD_HAS_VALUE);
                    tmp.append(",0,");
                    tmp.append(NGSIConstants.JSON_LD_VALUE);
                    tmp.append("}') = 'array' THEN jsonb_array_length(teai.data #> '{");
                    tmp.append(NGSIConstants.NGSI_LD_HAS_VALUE);
                    tmp.append(",0,");
                    tmp.append(NGSIConstants.JSON_LD_VALUE);
                    tmp.append("}')::numeric ELSE NULL END)");
                    buildAggrValue(sql, "avg", fromDollar, toDollar, periodDollar, tmp.toString(), temporalProperty);
                    break;
                case NGSIConstants.AGGR_METH_STDDEV:
                    tmp.append("STDDEV(CASE WHEN jsonb_typeof(teai.data #> '{");
                    tmp.append(NGSIConstants.NGSI_LD_HAS_VALUE);
                    tmp.append(",0,");
                    tmp.append(NGSIConstants.JSON_LD_VALUE);
                    tmp.append("}') = 'number' THEN (teai.data #> '{");
                    tmp.append(NGSIConstants.NGSI_LD_HAS_VALUE);
                    tmp.append(",0,");
                    tmp.append(NGSIConstants.JSON_LD_VALUE);
                    tmp.append("}')::numeric WHEN jsonb_typeof(teai.data #> '{");
                    tmp.append(NGSIConstants.NGSI_LD_HAS_VALUE);
                    tmp.append(",0,");
                    tmp.append(NGSIConstants.JSON_LD_VALUE);
                    tmp.append("}') = 'boolean' THEN (teai.data #> '{");
                    tmp.append(NGSIConstants.NGSI_LD_HAS_VALUE);
                    tmp.append(",0,");
                    tmp.append(NGSIConstants.JSON_LD_VALUE);
                    tmp.append("}')::numeric WHEN jsonb_typeof(teai.data #> '{");
                    tmp.append(NGSIConstants.NGSI_LD_HAS_VALUE);
                    tmp.append(",0,");
                    tmp.append(NGSIConstants.JSON_LD_VALUE);
                    tmp.append("}') = 'array' THEN jsonb_array_length(teai.data #> '{");
                    tmp.append(NGSIConstants.NGSI_LD_HAS_VALUE);
                    tmp.append(",0,");
                    tmp.append(NGSIConstants.JSON_LD_VALUE);
                    tmp.append("}')::numeric ELSE NULL END)");
                    buildAggrValue(sql, "stddev", fromDollar, toDollar, periodDollar, tmp.toString(), temporalProperty);
                    break;
                case NGSIConstants.AGGR_METH_SUMSQ:
                    tmp.append("SUM(CASE WHEN jsonb_typeof(teai.data #> '{");
                    tmp.append(NGSIConstants.NGSI_LD_HAS_VALUE);
                    tmp.append(",0,");
                    tmp.append(NGSIConstants.JSON_LD_VALUE);
                    tmp.append("}') = 'number' THEN (teai.data #> '{");
                    tmp.append(NGSIConstants.NGSI_LD_HAS_VALUE);
                    tmp.append(",0,");
                    tmp.append(NGSIConstants.JSON_LD_VALUE);
                    tmp.append("}')::numeric ^ 2 WHEN jsonb_typeof(teai.data #> '{");
                    tmp.append(NGSIConstants.NGSI_LD_HAS_VALUE);
                    tmp.append(",0,");
                    tmp.append(NGSIConstants.JSON_LD_VALUE);
                    tmp.append("}') = 'boolean' THEN (teai.data #> '{");
                    tmp.append(NGSIConstants.NGSI_LD_HAS_VALUE);
                    tmp.append(",0,");
                    tmp.append(NGSIConstants.JSON_LD_VALUE);
                    tmp.append("}')::numeric ^ 2 WHEN jsonb_typeof(teai.data #> '{");
                    tmp.append(NGSIConstants.NGSI_LD_HAS_VALUE);
                    tmp.append(",0,");
                    tmp.append(NGSIConstants.JSON_LD_VALUE);
                    tmp.append("}') = 'array' THEN jsonb_array_length(teai.data #> '{");
                    tmp.append(NGSIConstants.NGSI_LD_HAS_VALUE);
                    tmp.append(",0,");
                    tmp.append(NGSIConstants.JSON_LD_VALUE);
                    tmp.append("}')::numeric ^ 2 ELSE NULL END)");
                    buildAggrValue(sql, "sumsq", fromDollar, toDollar, periodDollar, tmp.toString(), temporalProperty);
                    break;
                case NGSIConstants.AGGR_METH_TOTAL_COUNT:
                    buildAggrValue(sql, "totalcount", fromDollar, toDollar, periodDollar, "COUNT(teai.data)",
                            temporalProperty);
                    break;
                case NGSIConstants.AGGR_METH_DISTINCT_COUNT:
                    tmp.append("COUNT(DISTINCT CASE WHEN teai.data @> '{\"");
                    tmp.append(NGSIConstants.JSON_LD_TYPE);
                    tmp.append("\": [\"");
                    tmp.append(NGSIConstants.NGSI_LD_PROPERTY);
                    tmp.append("\"]}' THEN teai.data#>'{");
                    tmp.append(NGSIConstants.NGSI_LD_HAS_VALUE);
                    tmp.append(",0,");
                    tmp.append(NGSIConstants.JSON_LD_VALUE);
                    tmp.append("}' WHEN teai.data @>'{\"");
                    tmp.append(NGSIConstants.JSON_LD_TYPE);
                    tmp.append("\": [\"");
                    tmp.append(NGSIConstants.NGSI_LD_RELATIONSHIP);
                    tmp.append("\"]}' THEN teai.data#>'{");
                    tmp.append(NGSIConstants.NGSI_LD_HAS_OBJECT);
                    tmp.append(",0,");
                    tmp.append(NGSIConstants.JSON_LD_ID);
                    tmp.append("}' WHEN teai.data @>'{\"");
                    tmp.append(NGSIConstants.JSON_LD_TYPE);
                    tmp.append("\": [\"");
                    tmp.append(NGSIConstants.NGSI_LD_GEOPROPERTY);
                    tmp.append("\"]}' THEN teai.data#>'{");
                    tmp.append(NGSIConstants.NGSI_LD_HAS_VALUE);
                    tmp.append(",0,");
                    tmp.append(NGSIConstants.JSON_LD_VALUE);
                    tmp.append("}' WHEN teai.data @>'{\"");
                    tmp.append(NGSIConstants.JSON_LD_TYPE);
                    tmp.append("\": [\"");
                    tmp.append(NGSIConstants.NGSI_LD_LANGPROPERTY);
                    tmp.append("\"]}' THEN teai.data#>'{");
                    tmp.append(NGSIConstants.NGSI_LD_HAS_LANGUAGE_MAP);
                    tmp.append("}' ELSE teai.data END)");
                    buildAggrValue(sql, "distinctcount", fromDollar, toDollar, periodDollar, tmp.toString(),
                            temporalProperty);
                    break;
                default:
                    break;
            }
            sql.append(',');
        }
        sql.setLength(sql.length() - 1);
        sql.append(" FROM entityInfos ei LEFT JOIN temporalentityattrinstance teai ON teai.temporalentity_id = ei.id");
        if (period != null) {
            sql.append(" CROSS JOIN LATERAL (SELECT generate_series((SELECT ");
            if (from != null) {
                sql.append("GREATEST($");
                sql.append(fromDollar);
                sql.append("::text::timestamp, MIN(");
                sql.append(temporalProperty);
                sql.append("))");
            } else {
                sql.append("MIN(");
                sql.append(temporalProperty);
                sql.append(')');
            }
            sql.append(" FROM temporalentityattrinstance WHERE attributeid = teai.attributeid), (SELECT ");
            if (to != null) {
                sql.append("LEAST($");
                sql.append(toDollar);
                sql.append("::text::timestamp, MAX(");
                sql.append(temporalProperty);
                sql.append("))");
            } else {
                sql.append("MAX(");
                sql.append(temporalProperty);
                sql.append(')');
            }
            sql.append(" FROM temporalentityattrinstance WHERE attributeid = teai.attributeid), $");
            sql.append(periodDollar);
            sql.append("::text::interval) as period) pr");
        }
        sql.append(" WHERE ");
        sql.append(temporalProperty);
        sql.append(" IS NOT NULL ");
        if (from != null || to != null) {
            sql.append(" AND ");
            if (from != null) {
                sql.append(temporalProperty);
                sql.append(" > $");
                sql.append(fromDollar);
                sql.append("::text::timestamp");
                if (to != null) {
                    sql.append(" AND ");
                }
            }
            if (to != null) {
                sql.append(temporalProperty);
                sql.append(" < $");
                sql.append(toDollar);
                sql.append("::text::timestamp");
            }
        }
        sql.append(
                " GROUP BY ei.id, ei.e_types, ei.r_createdat, ei.r_modifiedat, ei.r_deletedat, ei.scope_entry, teai.attributeid, (teai.data #>> '{@type,0}')");
        if (period != null) {
            sql.append(", pr.period");
        }
        // sql.append(" teai.");
        // sql.append(temporalProperty);
        sql.append("),");
        sql.append(
                "attribute_arrays AS (SELECT id, e_types, r_createdat, r_modifiedat, r_deletedat, scope_entry, attributeid, jsonb_build_array(jsonb_strip_nulls(jsonb_build_object('@type', jsonb_build_array(MAX(attr_type)),");
        StringBuilder tmp = new StringBuilder(128);
        for (String aggrFunction : aggrFunctions) {
            String aggrResult;
            String aggrTitle;
            switch (aggrFunction) {
                case NGSIConstants.AGGR_METH_SUM:
                    aggrResult = "sum_result";
                    aggrTitle = NGSIConstants.NGSI_LD_SUM;
                    break;
                case NGSIConstants.AGGR_METH_MIN:
                    aggrResult = "min_result";
                    aggrTitle = NGSIConstants.NGSI_LD_MIN;
                    break;
                case NGSIConstants.AGGR_METH_MAX:
                    aggrResult = "max_result";
                    aggrTitle = NGSIConstants.NGSI_LD_MAX;
                    break;
                case NGSIConstants.AGGR_METH_AVG:
                    aggrResult = "avg_result";
                    aggrTitle = NGSIConstants.NGSI_LD_AVG;
                    break;
                case NGSIConstants.AGGR_METH_STDDEV:
                    aggrResult = "stddev_result";
                    aggrTitle = NGSIConstants.NGSI_LD_STDDEV;
                    break;
                case NGSIConstants.AGGR_METH_SUMSQ:
                    aggrResult = "sumsq_result";
                    aggrTitle = NGSIConstants.NGSI_LD_SUMSQ;
                    break;
                case NGSIConstants.AGGR_METH_TOTAL_COUNT:
                    aggrResult = "totalcount_result";
                    aggrTitle = NGSIConstants.NGSI_LD_TOTALCOUNT;
                    break;
                case NGSIConstants.AGGR_METH_DISTINCT_COUNT:
                    aggrResult = "distinctcount_result";
                    aggrTitle = NGSIConstants.NGSI_LD_DISTINCTCOUNT;
                    break;
                default:
                    continue;
            }

            tmp.append(aggrResult);
            tmp.append(',');
            sql.append('\'');
            sql.append(aggrTitle);
            sql.append("', CASE WHEN bool_or(");
            sql.append(aggrResult);
            sql.append(" IS NOT NULL AND NOT ");
            sql.append(aggrResult);
            sql.append(
                    " @> '{\"@list\": [{\"@value\": null}]}'::jsonb) THEN jsonb_build_array(jsonb_build_object('@list', jsonb_agg(");
            sql.append(aggrResult);
            if (period != null) {
                sql.append(" ORDER BY period");
            }
            sql.append(") FILTER (WHERE ");
            sql.append(aggrResult);
            sql.append(" IS NOT NULL)))");
            sql.append(" ELSE NULL END");
            sql.append(',');

        }
        tmp.setLength(tmp.length() - 1);
        tmp.append(')');
        sql.setLength(sql.length() - 1);
        sql.append(
                "))) as data_array FROM period_stats GROUP BY id, e_types, r_createdat, r_modifiedat, r_deletedat, scope_entry, attributeid, attr_type, period)");
        // sql.append(tmp.toString());

        return dollar;

    }

    private void buildAggrValue(StringBuilder sql, String aggrName, int from, int to, int period, String sqlAggr,
            String tempProp) {
        sql.append("jsonb_build_object('");
        sql.append(NGSIConstants.JSON_LD_LIST);
        sql.append("', jsonb_build_array(jsonb_build_object('");
        sql.append(NGSIConstants.JSON_LD_VALUE);
        sql.append("', ");
        sql.append(sqlAggr);
        sql.append("), jsonb_build_object('");
        sql.append(NGSIConstants.JSON_LD_VALUE);
        sql.append("', to_char(");
        if (period != -1) {
            sql.append("pr.period");
        } else if (from != -1) {
            sql.append('$');
            sql.append(from);
            sql.append("::text::timestamp");
        } else {
            sql.append("MIN(");
            sql.append(tempProp);
            sql.append(')');
        }
        sql.append(", 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')), jsonb_build_object('");
        sql.append(NGSIConstants.JSON_LD_VALUE);
        sql.append("', to_char(");
        if (period != -1) {
            sql.append("pr.period + $");
            sql.append(period);
            sql.append("::text::interval");
        } else if (to != -1) {
            sql.append('$');
            sql.append(to);
            sql.append("::text::timestamp");
        } else {
            sql.append("MAX(");
            sql.append(tempProp);
            sql.append(')');
        }
        sql.append(", 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')))) as ");
        sql.append(aggrName);
        sql.append("_result");
    }

}
