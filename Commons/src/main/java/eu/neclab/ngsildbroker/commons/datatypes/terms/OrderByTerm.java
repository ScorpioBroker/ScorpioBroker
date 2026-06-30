package eu.neclab.ngsildbroker.commons.datatypes.terms;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.Lists;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.sqlclient.Tuple;

public class OrderByTerm {

    private class TermEntry {
        String orderTerm;
        String collation;
        String orderFrom;
        String orderDirection;
        String orderGeometry;
        String[] splittedOrderTerm;

        public TermEntry(String orderTerm, String collation, String orderFrom, String orderDirection,
                String orderGeometry) {
            this.orderTerm = orderTerm;
            this.collation = collation;
            this.orderFrom = orderFrom;

            this.orderGeometry = orderGeometry;
            if (orderDirection != null) {
                if (orderDirection.contains("asc")) {
                    this.orderDirection = "ASC";
                } else {
                    this.orderDirection = "DESC";
                }
            } else {
                this.orderDirection = null;
            }
            splittedOrderTerm = StringUtils.split(orderTerm, '.');
        }

    }

    List<TermEntry> orderTerms = Lists.newArrayList();

    public void addTerm(String orderTerm, String collation, String orderFrom,
            String orderDirection, String orderGeometry) {
        orderTerms.add(new TermEntry(orderTerm, collation, orderFrom, orderDirection, orderGeometry));
    }

    public int toSqlOrderValue(StringBuilder sql, int dollar, Tuple tuple, ObjectMapper objectMapper, Context context,
            DataSetIdTerm datasetIdTerm, boolean useDefaultForGeo)
            throws ResponseException {
        int i = 0;
        for (TermEntry orderTerm : orderTerms) {
            if (orderTerm.splittedOrderTerm.length == 1) {
                String term = orderTerm.splittedOrderTerm[0];
                String expandedTerm = context.expandIri(term, false, true, null, null);
                switch (expandedTerm) {
                    case NGSIConstants.JSON_LD_ID:
                        sql.append("ID");
                        sql.append(" as ORDER_VALUE");
                        sql.append(i);
                        sql.append(',');
                        i++;
                        continue;
                    case NGSIConstants.JSON_LD_TYPE:
                        sql.append("E_TYPES");
                        sql.append(" as ORDER_VALUE");
                        sql.append(i);
                        sql.append(',');
                        i++;
                        continue;
                    case NGSIConstants.NGSI_LD_LOCATION:
                        if (useDefaultForGeo || datasetIdTerm == null
                                || datasetIdTerm.getIds().contains(NGSIConstants.JSON_LD_NONE)) {
                            if (orderTerm.orderFrom != null) {
                                sql.append("ST_DistanceSphere(");
                            }
                            sql.append("LOCATION");
                            if (orderTerm.orderFrom != null) {
                                sql.append(", ST_SetSRID(ST_GeomFromGeoJSON($");
                                sql.append(dollar);
                                dollar++;
                                tuple.addJsonObject(
                                        getGeoJson(orderTerm.orderFrom, orderTerm.orderGeometry, objectMapper));
                                sql.append("::jsonb), 4326))");
                            }
                            sql.append(" as ORDER_VALUE");
                            sql.append(i);
                            sql.append(',');
                            i++;
                            continue;
                        }
                        break;
                    case NGSIConstants.NGSI_LD_CREATED_AT:
                        sql.append("CREATEDAT");
                        sql.append(" as ORDER_VALUE");
                        sql.append(i);
                        sql.append(',');
                        i++;
                        continue;
                    case NGSIConstants.NGSI_LD_MODIFIED_AT:
                        sql.append("MODIFIEDAT");
                        sql.append(" as ORDER_VALUE");
                        sql.append(i);
                        sql.append(',');
                        i++;
                        continue;

                }
            }
            String term = orderTerm.splittedOrderTerm[orderTerm.splittedOrderTerm.length - 1];
            int complexIndex = term.indexOf('[');
            String complexPart;
            String[] complexSplitted;
            if (complexIndex != -1) {
                complexPart = term.substring(complexIndex + 1, term.length() - 1);
                complexSplitted = StringUtils.split(complexPart, '.');
                orderTerm.splittedOrderTerm[orderTerm.splittedOrderTerm.length - 1] = term.substring(0,
                        complexIndex);
            } else {
                complexPart = null;
                complexSplitted = null;
            }
            StringBuilder termBase = new StringBuilder(128);
            termBase.append("$.");
            for (String subTerm : orderTerm.splittedOrderTerm) {
                String expandedTerm = context.expandIri(subTerm, false, true, null, null);
                termBase.append('"');
                termBase.append(expandedTerm);
                termBase.append("\"[*].");
            }
            termBase.setLength(termBase.length() - 1);
            String sTermBase = termBase.toString();
            termBase.setLength(0);
            termBase.append(sTermBase);
            termBase.append(" ? (@.\"");
            termBase.append(NGSIConstants.JSON_LD_TYPE);
            termBase.append("\"[0] == \"");
            termBase.append(NGSIConstants.NGSI_LD_PROPERTY);
            termBase.append("\"");
            if (datasetIdTerm != null) {
                termBase.append(" && ");
                datasetIdTerm.toJsonPath(termBase);
            }
            termBase.append(").\"");
            termBase.append(NGSIConstants.NGSI_LD_HAS_VALUE);
            termBase.append('"');
            if (complexPart != null) {
                termBase.append("[*].");
                for (String complexEntry : complexSplitted) {
                    termBase.append('"');
                    termBase.append(context.expandIri(complexEntry, false, true, null, null));
                    termBase.append("\"[*].");
                }
                termBase.setLength(termBase.length() - 4);
            }
            termBase.append("[0].\"");
            termBase.append(NGSIConstants.JSON_LD_VALUE);
            termBase.append('"');
            sql.append("COALESCE(jsonb_path_query_first(ENTITY, $");
            sql.append(dollar);
            dollar++;
            tuple.addString(termBase.toString());
            termBase.setLength(sTermBase.length());
            sql.append("::jsonpath),");

            if (datasetIdTerm != null) {
                termBase.append(" ? ");
                datasetIdTerm.toJsonPath(termBase);
            }
            termBase.append(".\"");
            termBase.append(NGSIConstants.JSON_LD_VALUE);
            termBase.append('"');

            sql.append("jsonb_path_query_first(ENTITY, $");
            sql.append(dollar);
            dollar++;
            tuple.addString(termBase.toString());
            termBase.setLength(sTermBase.length());
            sql.append("::jsonpath),");

            termBase.append(".\"");
            termBase.append(NGSIConstants.NGSI_LD_HAS_OBJECT);
            termBase.append("\"[0].\"");
            termBase.append(NGSIConstants.JSON_LD_ID);
            termBase.append('"');
            if (datasetIdTerm != null) {
                termBase.append(" ? ");
                datasetIdTerm.toJsonPath(termBase);
            }
            sql.append("jsonb_path_query_first(ENTITY, $");
            sql.append(dollar);
            dollar++;
            tuple.addString(termBase.toString());
            termBase.setLength(sTermBase.length());
            sql.append("::jsonpath),");

            termBase.append(".\"");
            termBase.append(NGSIConstants.NGSI_LD_HAS_VOCAB);
            termBase.append("\"[0].\"");
            termBase.append(NGSIConstants.JSON_LD_ID);
            termBase.append('"');
            if (datasetIdTerm != null) {
                termBase.append(" ? ");
                datasetIdTerm.toJsonPath(termBase);
            }

            sql.append("jsonb_path_query_first(ENTITY, $");
            sql.append(dollar);
            dollar++;
            tuple.addString(termBase.toString());
            termBase.setLength(sTermBase.length());
            sql.append("::jsonpath),");

            if (datasetIdTerm != null) {
                termBase.append(" ? ");
                datasetIdTerm.toJsonPath(termBase);
            }
            termBase.append(".\"");
            termBase.append(NGSIConstants.NGSI_LD_HAS_LANGUAGE_MAP);
            termBase.append('"');

            if (complexPart == null || complexPart.equals("*")) {
                termBase.append("[0].\"");
            } else {
                termBase.append("[*].\"");
            }
            termBase.append(NGSIConstants.JSON_LD_VALUE);
            termBase.append('"');
            if (complexPart != null) {
                termBase.append(" ? (@.\"");
                termBase.append(NGSIConstants.JSON_LD_LANGUAGE);
                termBase.append("\" == \"");
                termBase.append(complexPart);
                termBase.append("\")'),");
            }
            sql.append("jsonb_path_query_first(ENTITY, $");
            sql.append(dollar);
            dollar++;
            tuple.addString(termBase.toString());
            termBase.setLength(sTermBase.length());
            sql.append("::jsonpath),");

            if (datasetIdTerm != null) {
                termBase.append(" ? ");
                datasetIdTerm.toJsonPath(termBase);
            }
            termBase.append(".\"");
            termBase.append(NGSIConstants.NGSI_LD_HAS_JSON);
            termBase.append("\"[0].\"");
            termBase.append(NGSIConstants.JSON_LD_VALUE);
            termBase.append('"');
            if (complexPart != null) {
                termBase.append('.');
                for (String complexEntry : complexSplitted) {
                    termBase.append('"');
                    termBase.append(complexEntry);
                    termBase.append("\".");
                }
                termBase.setLength(termBase.length() - 1);
            }
            sql.append("jsonb_path_query_first(ENTITY, $");
            sql.append(dollar);
            dollar++;
            tuple.addString(termBase.toString());
            termBase.setLength(sTermBase.length());
            sql.append("::jsonpath),");

            if (datasetIdTerm != null) {
                termBase.append(" ? ");
                datasetIdTerm.toJsonPath(termBase);
            }
            termBase.append(".\"");
            termBase.append(NGSIConstants.NGSI_LD_HAS_LIST);
            termBase.append("\"[0].\"");
            termBase.append(NGSIConstants.JSON_LD_LIST);
            termBase.append('"');
            if (complexPart != null) {
                termBase.append("[*].");
                for (String complexEntry : complexSplitted) {
                    termBase.append('"');
                    termBase.append(context.expandIri(complexEntry, false, true, null, null));
                    termBase.append("\"[*].");
                }
                termBase.setLength(termBase.length() - 4);
            }
            termBase.append("[0].\"");
            termBase.append(NGSIConstants.JSON_LD_VALUE);
            termBase.append('"');
            sql.append("jsonb_path_query_first(ENTITY, $");
            sql.append(dollar);
            dollar++;
            tuple.addString(termBase.toString());
            termBase.setLength(sTermBase.length());
            sql.append("::jsonpath),");

            if (datasetIdTerm != null) {
                termBase.append(" ? ");
                datasetIdTerm.toJsonPath(termBase);
            }
            termBase.append(".\"");
            termBase.append(NGSIConstants.NGSI_LD_HAS_OBJECT_LIST);
            termBase.append("\"[0].\"");
            termBase.append(NGSIConstants.JSON_LD_LIST);
            termBase.append("\"[*].\"");
            termBase.append(NGSIConstants.NGSI_LD_HAS_OBJECT);
            termBase.append("\"[*].\"");
            termBase.append(NGSIConstants.JSON_LD_ID);
            termBase.append('"');
            sql.append("jsonb_path_query_first(ENTITY, $");
            sql.append(dollar);
            dollar++;
            tuple.addString(termBase.toString());
            termBase.setLength(sTermBase.length());
            sql.append("::jsonpath)");

            if (orderTerm.orderFrom != null) {
                sql.append(", to_jsonb(ST_DistanceSphere(ST_SetSRID(ST_GeomFromGeoJSON(getgeojson(");

                termBase.append(" ? ((@.\"");
                termBase.append(NGSIConstants.JSON_LD_TYPE);
                termBase.append("\"[0] == \"");
                termBase.append(NGSIConstants.NGSI_LD_GEOPROPERTY);
                termBase.append("\")");
                if (datasetIdTerm != null) {
                    termBase.append(" && ");
                    datasetIdTerm.toJsonPath(termBase);
                }
                termBase.append(").\"");
                termBase.append(NGSIConstants.NGSI_LD_HAS_VALUE);
                termBase.append("\"[0].\"");
                termBase.append(NGSIConstants.JSON_LD_VALUE);
                termBase.append('"');
                sql.append("jsonb_path_query_first(ENTITY, $");
                sql.append(dollar);
                dollar++;

                tuple.addString(termBase.toString());
                sql.append("::jsonpath))), 4326)");

                sql.append(", ST_SetSRID(ST_GeomFromGeoJSON($");
                sql.append(dollar);
                dollar++;
                tuple.addJsonObject(
                        getGeoJson(orderTerm.orderFrom, orderTerm.orderGeometry, objectMapper));
                sql.append("::jsonb), 4326)))");
            }
            sql.append(')');
            sql.append(" as ORDER_VALUE");
            sql.append(i);
            sql.append(',');
            i++;
        }
        sql.setLength(sql.length() - 1);
        return dollar;
    }

    private JsonObject getGeoJson(String coordinateString, String coordinateType, ObjectMapper objectMapper)
            throws ResponseException {
        Map<String, Object> result = new HashMap<>(2);

        try {
            JsonNode coordinates = objectMapper.readTree(coordinateString);
            List<Object> coordResult = Lists.newArrayList();
            int depth = parseJsonNode(coordResult, coordinates, 0);
            result.put(NGSIConstants.GEO_JSON_COORDINATES, coordResult);
            switch (depth) {
                case 0:
                    result.put(NGSIConstants.TYPE, NGSIConstants.GEO_TYPE_POINT);
                    break;
                case 1:
                    result.put(NGSIConstants.TYPE, NGSIConstants.GEO_TYPE_LINESTRING);
                    break;
                case 2:
                    result.put(NGSIConstants.TYPE, NGSIConstants.GEO_TYPE_POLYGON);
                    break;
                case 3:
                    result.put(NGSIConstants.TYPE, NGSIConstants.GEO_TYPE_MULTI_POLYGON);
                    break;
                default:
                    break;
            }
        } catch (JsonProcessingException e) {
            throw new ResponseException(ErrorType.BadRequestData,
                    "Unable to parse coordinate string " + coordinateString);
        }

        if (coordinateType != null) {
            result.put(NGSIConstants.TYPE, coordinateType);
        }

        return new JsonObject(result);
    }

    private static int parseJsonNode(List<Object> result, JsonNode node, int depth) {
        if (node.isArray()) {

            for (JsonNode child : node) {
                if (child.isNumber()) {
                    result.add(child.asDouble());
                } else {
                    List<Object> tmp = new ArrayList<>();
                    result.add(tmp);
                    depth = parseJsonNode(tmp, child, depth + 1);
                }
            }
            return depth;
        }
        result.add(node.asDouble());
        return depth;
    }

    public void toSqlOrder(StringBuilder sql) {
        sql.append(" ORDER BY ");
        int i = 0;
        for (TermEntry term : orderTerms) {
            sql.append("ORDER_VALUE");
            sql.append(i);
            if (term.collation != null) {
                sql.append(" COLLATE \"");
                sql.append(term.collation);
                sql.append('"');
            }
            if (term.orderDirection != null) {
                sql.append(' ');
                sql.append(term.orderDirection);
            }
            sql.append(',');
            i++;
        }
        sql.setLength(sql.length() - 1);
    }

    public int termSize() {
        return orderTerms.size();
    }

    public String getTerm(int i) {
        return orderTerms.get(i).orderTerm;
    }

}
