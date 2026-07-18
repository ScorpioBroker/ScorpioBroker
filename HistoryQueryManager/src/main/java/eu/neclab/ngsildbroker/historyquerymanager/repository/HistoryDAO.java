package eu.neclab.ngsildbroker.historyquerymanager.repository;

import java.time.LocalDateTime;
import java.util.ArrayList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import jakarta.inject.Inject;
import jakarta.enterprise.context.ApplicationScoped;
import io.quarkus.runtime.Startup;

import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLDService;
import com.github.jsonldjava.core.JsonLdConsts;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.RegistrationEntry;
import eu.neclab.ngsildbroker.commons.datatypes.results.QueryResult;
import eu.neclab.ngsildbroker.commons.datatypes.terms.AggrTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.AttrsQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.DataSetIdTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.GeoQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.OmitTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.OrderByTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.PickTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.QQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.ScopeQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.TemporalQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.TypeQueryTerm;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.storage.ConnectionManager;
import eu.neclab.ngsildbroker.commons.tools.DBUtil;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple3;

import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowIterator;
import io.vertx.mutiny.sqlclient.Tuple;

@ApplicationScoped
@Startup
public class HistoryDAO {

	private static Logger logger = LoggerFactory.getLogger(HistoryDAO.class);

	private final String TIMESTAMP_FORMAT = "'YYYY-MM-DDThh24:MI:SS.usZ'";

	@Inject
	ConnectionManager connectionManager;

	@Inject
	ObjectMapper objectMapper;

	@Inject
	JsonLDService ldService;

	public Uni<Table<String, String, List<RegistrationEntry>>> getAllRegistries() {
		return DBUtil.getAllRegistries(connectionManager, ldService,
				"SELECT cs_id, c_id, e_id, e_id_p, e_type, e_prop, e_rel, ST_AsGeoJSON(i_location), scopes, EXTRACT(MILLISECONDS FROM expires), endpoint, tenant_id, headers, reg_mode, createEntity, updateEntity, appendAttrs, updateAttrs, deleteAttrs, deleteEntity, createBatch, upsertBatch, updateBatch, deleteBatch, upsertTemporal, appendAttrsTemporal, deleteAttrsTemporal, updateAttrsTemporal, deleteAttrInstanceTemporal, deleteTemporal, mergeEntity, replaceEntity, replaceAttrs, mergeBatch, retrieveEntity, queryEntity, queryBatch, retrieveTemporal, queryTemporal, retrieveEntityTypes, retrieveEntityTypeDetails, retrieveEntityTypeInfo, retrieveAttrTypes, retrieveAttrTypeDetails, retrieveAttrTypeInfo, createSubscription, updateSubscription, retrieveSubscription, querySubscription, deleteSubscription, queryEntityMap, createEntityMap, updateEntityMap, deleteEntityMap, retrieveEntityMap, csource_Alias, purgeEntity  FROM csourceinformation WHERE retrieveTemporal OR queryTemporal",
				logger);
	}

	private List<Map<String, String>> getScope(String[] scopes) {
		List<Map<String, String>> result = new ArrayList<>(scopes.length);
		for (String scope : scopes) {
			Map<String, String> tmp = new HashMap<>(1);
			tmp.put(NGSIConstants.JSON_LD_VALUE, scope);
			result.add(tmp);
		}
		return result;
	}

	private int addAggrQuery(StringBuilder sql, AggrTerm aggrQuery, TemporalQueryTerm tempQuery, int dollarCount,
			Tuple tuple, String tempProp) {
		// Doc comment:
		// we first build a jsonb array with jsonb_agg if it has no contents because of
		// the filter it will be null this will be used later on to filter out null
		// values
		int dollarplus = 1;
		// ei.id, ei.createdat, ei.e_types, ei.modifiedat, ei.deletedat, ei.scopes,
		// teai.attributeid, (array_agg(teai.data ORDER BY teai.
		sql.append(
				", aggr as (SELECT ID, createdat, e_types, modifiedat, deletedat, scopes, attributeid, ATTRTYPE, PRSTART, PRSTOP, ");
		for (String aggrFunction : aggrQuery.getAggrFunctions()) {
			sql.append("JSONB_BUILD_OBJECT('" + NGSIConstants.JSON_LD_LIST + "', JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT('"
					+ NGSIConstants.JSON_LD_VALUE + "', ");
			switch (aggrFunction) {
				case NGSIConstants.AGGR_METH_SUM:
					sql.append("SUM(CASE ");
					sql.append("WHEN JSONB_TYPEOF(data #> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
							+ NGSIConstants.JSON_LD_VALUE + "}') = 'number' THEN (data #> '{"
							+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE + "}')::numeric ");
					sql.append("WHEN JSONB_TYPEOF(data #> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
							+ NGSIConstants.JSON_LD_VALUE + "}') = 'boolean' THEN (data #> '{"
							+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE + "}')::numeric ");
					sql.append("WHEN JSONB_TYPEOF(data #> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
							+ NGSIConstants.JSON_LD_VALUE + "}') = 'array' THEN (JSONB_ARRAY_LENGTH(data #> ('{"
							+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE
							+ "}')))::numeric ");
					// nulling here will make the result null as well and it will run into the
					// filter from above
					sql.append("ELSE NULL END)) ");
					dollarplus = generateTimestampForAttr(sql, dollarCount, tempQuery, aggrQuery);
					sql.append(")) as SUMDATA");
					break;
				case NGSIConstants.AGGR_METH_MIN:
					sql.append("MIN(CASE ");
					sql.append("WHEN JSONB_TYPEOF(data #> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
							+ NGSIConstants.JSON_LD_VALUE + "}') = 'number' THEN (data #>> '{"
							+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE + "}') ");
					sql.append("WHEN JSONB_TYPEOF(data #> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
							+ NGSIConstants.JSON_LD_VALUE + "}') = 'boolean' THEN (data #>> '{"
							+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE + "}') ");
					sql.append("WHEN JSONB_TYPEOF(data #> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
							+ NGSIConstants.JSON_LD_VALUE + "}') = 'string' THEN (data #>> '{"
							+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE + "}') ");
					sql.append("WHEN JSONB_TYPEOF(data #> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
							+ NGSIConstants.JSON_LD_VALUE + "}') = 'array' THEN (JSONB_ARRAY_LENGTH(data #> ('{"
							+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE + "}')))::text ");
					sql.append("ELSE NULL END)) ");
					dollarplus = generateTimestampForAttr(sql, dollarCount, tempQuery, aggrQuery);
					sql.append(")) as MINDATA");
					break;
				case NGSIConstants.AGGR_METH_MAX:

					sql.append("(MAX(CASE ");
					sql.append("WHEN JSONB_TYPEOF(data #> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
							+ NGSIConstants.JSON_LD_VALUE + "}') = 'number' THEN (data #>> '{"
							+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE + "}') ");
					sql.append("WHEN JSONB_TYPEOF(data #> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
							+ NGSIConstants.JSON_LD_VALUE + "}') = 'boolean' THEN (data #>> '{"
							+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE + "}') ");
					sql.append("WHEN JSONB_TYPEOF(data #> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
							+ NGSIConstants.JSON_LD_VALUE + "}') = 'string' THEN (data #>> '{"
							+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE + "}') ");
					sql.append("WHEN JSONB_TYPEOF(data #> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
							+ NGSIConstants.JSON_LD_VALUE + "}') = 'array' THEN (JSONB_ARRAY_LENGTH(data #> ('{"
							+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE + "}')))::text ");
					sql.append("ELSE NULL END))) ");
					dollarplus = generateTimestampForAttr(sql, dollarCount, tempQuery, aggrQuery);
					sql.append(")) as MAXDATA");
					break;
				case NGSIConstants.AGGR_METH_AVG:
					sql.append("AVG(CASE ");
					sql.append("WHEN JSONB_TYPEOF(data #> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
							+ NGSIConstants.JSON_LD_VALUE + "}') = 'number' THEN (data #> '{"
							+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE + "}')::numeric ");
					sql.append("WHEN JSONB_TYPEOF(data #> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
							+ NGSIConstants.JSON_LD_VALUE + "}') = 'boolean' THEN (data #> '{"
							+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE + "}')::numeric ");
					sql.append("WHEN JSONB_TYPEOF(data #> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
							+ NGSIConstants.JSON_LD_VALUE + "}') = 'array' THEN (JSONB_ARRAY_LENGTH(data #> ('{"
							+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE
							+ "}')))::numeric ");
					sql.append("ELSE NULL END)) ");
					dollarplus = generateTimestampForAttr(sql, dollarCount, tempQuery, aggrQuery);
					sql.append(")) as AVGDATA");
					break;
				case NGSIConstants.AGGR_METH_STDDEV:
					sql.append("STDDEV(CASE ");
					sql.append("WHEN JSONB_TYPEOF(data #> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
							+ NGSIConstants.JSON_LD_VALUE + "}') = 'number' THEN (data #> '{"
							+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE + "}')::numeric ");
					sql.append("WHEN JSONB_TYPEOF(data #> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
							+ NGSIConstants.JSON_LD_VALUE + "}') = 'boolean' THEN (data #> '{"
							+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE + "}')::numeric ");
					sql.append("WHEN JSONB_TYPEOF(data #> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
							+ NGSIConstants.JSON_LD_VALUE + "}') = 'array' THEN (JSONB_ARRAY_LENGTH(data #> ('{"
							+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE
							+ "}')))::numeric ");
					sql.append("ELSE NULL END)) ");
					dollarplus = generateTimestampForAttr(sql, dollarCount, tempQuery, aggrQuery);
					sql.append(")) as STDDEVDATA");
					break;
				case NGSIConstants.AGGR_METH_SUMSQ:
					sql.append("SUM(CASE ");
					sql.append("WHEN JSONB_TYPEOF(data #> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
							+ NGSIConstants.JSON_LD_VALUE + "}') = 'number' THEN ((data #> '{"
							+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE
							+ "}')::numeric)^2 ");
					sql.append("WHEN JSONB_TYPEOF(data #> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
							+ NGSIConstants.JSON_LD_VALUE + "}') = 'boolean' THEN ((data #> '{"
							+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE
							+ "}')::numeric)^2 ");
					sql.append("WHEN JSONB_TYPEOF(data #> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
							+ NGSIConstants.JSON_LD_VALUE + "}') = 'array' THEN (JSONB_ARRAY_LENGTH(data #> ('{"
							+ NGSIConstants.NGSI_LD_HAS_VALUE + ",0," + NGSIConstants.JSON_LD_VALUE
							+ "}')))::numeric^2 ");
					sql.append("ELSE NULL END)) ");
					dollarplus = generateTimestampForAttr(sql, dollarCount, tempQuery, aggrQuery);
					sql.append(")) as SUMSQDATA");
					break;
				case NGSIConstants.AGGR_METH_TOTAL_COUNT:
					sql.append("COUNT(DATA)) ");
					dollarplus = generateTimestampForAttr(sql, dollarCount, tempQuery, aggrQuery);
					sql.append(")) as TOTALCOUNTDATA");
					break;
				case NGSIConstants.AGGR_METH_DISTINCT_COUNT:
					sql.append("COUNT(DISTINCT CASE ");
					sql.append(
							"WHEN DATA@>'{\"" + NGSIConstants.JSON_LD_TYPE + "\": [\"" + NGSIConstants.NGSI_LD_PROPERTY
									+ "\"]}' THEN DATA #> '{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,"
									+ NGSIConstants.JSON_LD_VALUE + "}'");
					sql.append("WHEN DATA@>'{\"" + NGSIConstants.JSON_LD_TYPE + "\": [\""
							+ NGSIConstants.NGSI_LD_RELATIONSHIP + "\"]}' THEN DATA #> '{"
							+ NGSIConstants.NGSI_LD_HAS_OBJECT + ",0," + NGSIConstants.JSON_LD_ID + "}'");
					sql.append("WHEN DATA@>'{\"" + NGSIConstants.JSON_LD_TYPE + "\": [\""
							+ NGSIConstants.NGSI_LD_GEOPROPERTY + "\"]}' THEN DATA #> '{"
							+ NGSIConstants.NGSI_LD_HAS_VALUE
							+ ",0," + NGSIConstants.JSON_LD_VALUE + "}'");
					sql.append(
							"WHEN DATA@>'{\"" + NGSIConstants.JSON_LD_TYPE + "\": [\""
									+ NGSIConstants.NGSI_LD_LANGPROPERTY
									+ "\"]}' THEN DATA #> '{" + NGSIConstants.NGSI_LD_HAS_LANGUAGE_MAP + "}'");
					sql.append("ELSE DATA END)) ");
					dollarplus = generateTimestampForAttr(sql, dollarCount, tempQuery, aggrQuery);
					sql.append(")) as DISTINCTCOUNTDATA");
					break;
				default:
					break;
			}
			sql.append(',');
		}

		if (aggrQuery.getPeriod() != null) {
			sql.append("PR");
			tuple.addString(aggrQuery.getPeriod());
		} else {
			sql.setLength(sql.length() - 1);
		}

		dollarCount += dollarplus;

		sql.append(" FROM attribute_arrays_temp teai, unnest(data_array) as data ");
		String expandTimeProp;
		switch (tempProp) {
			case NGSIConstants.QUERY_PARAMETER_OBSERVED_AT:
				expandTimeProp = NGSIConstants.NGSI_LD_OBSERVED_AT;
				break;
			case NGSIConstants.QUERY_PARAMETER_CREATED_AT:
				expandTimeProp = NGSIConstants.NGSI_LD_CREATED_AT;
				break;
			case NGSIConstants.QUERY_PARAMETER_MODIFIED_AT:
				expandTimeProp = NGSIConstants.NGSI_LD_MODIFIED_AT;
				break;
			case NGSIConstants.QUERY_PARAMETER_DELETED_AT:
				expandTimeProp = NGSIConstants.NGSI_LD_DELETED_AT;
				break;

			default:
				expandTimeProp = NGSIConstants.NGSI_LD_OBSERVED_AT;
				break;
		}
		if (aggrQuery.getPeriod() != null) {
			sql.append("LEFT JOIN generate_series (");
			if (tempQuery == null || tempQuery.getTimerel() == null) {
				sql.append("PRSTART, PRSTOP");
			} else {
				switch (tempQuery.getTimerel()) {
					case NGSIConstants.TIME_REL_BEFORE:
						sql.append("PRSTART, $");
						sql.append(dollarCount);
						sql.append("::text::timestamp");
						tuple.addString(tempQuery.getTimeAt());
						dollarCount++;
						break;
					case NGSIConstants.TIME_REL_AFTER:
						sql.append("$");
						sql.append(dollarCount);
						sql.append("::text::timestamp");
						sql.append(", PRSTOP");
						tuple.addString(tempQuery.getTimeAt());
						dollarCount++;
						break;
					case NGSIConstants.TIME_REL_BETWEEN:
						sql.append("$");
						sql.append(dollarCount);
						sql.append("::text::timestamp");
						tuple.addString(tempQuery.getTimeAt());
						dollarCount++;
						sql.append(", $");
						sql.append(dollarCount);
						sql.append("::text::timestamp");
						tuple.addString(tempQuery.getEndTimeAt());
						dollarCount++;
						break;
				}
			}
			dollarplus = 1;
			sql.append(", $");
			sql.append(dollarCount);
			sql.append("::text::interval) as pr(period) on (data #>> '{");
			sql.append(expandTimeProp);
			sql.append(",0,");
			sql.append(NGSIConstants.JSON_LD_VALUE);
			sql.append("}')::timestamp between pr.period and pr.period + $");
			sql.append(dollarCount);
			sql.append("::text::interval");
			tuple.addString(aggrQuery.getPeriod());
		}
		dollarCount += dollarplus;

		sql.append(" WHERE data ? '");
		sql.append(expandTimeProp);
		sql.append("'");
		if (aggrQuery.getPeriod() != null) {
			sql.append(" AND PR IS NOT NULL");
		}
		sql.append(" GROUP BY ID, SCOPES, E_TYPES, CREATEDAT, MODIFIEDAT, DELETEDAT, ATTRIBUTEID,");
		if (aggrQuery.getPeriod() != null) {
			sql.append(" PR,");
		}
		sql.append(" ATTRTYPE");
		sql.append(",PRSTART, PRSTOP");
		// if (aggrQuery.getPeriod() != null) {
		// if (tempQuery != null && tempQuery.getTimerel() != null) {
		// switch (tempQuery.getTimerel()) {
		// case NGSIConstants.TIME_REL_BEFORE:
		// sql.append(",($");
		// sql.append(dollarCount);
		// sql.append("::text::timestamp - LEAST(TEAI." + tempProp + "))::interval");
		// break;
		// case NGSIConstants.TIME_REL_AFTER:
		// sql.append(",(GREATEST(TEAI." + tempProp + ") - $");
		// sql.append(dollarCount);
		// sql.append("::text::timestamp)::interval");
		// break;
		// // sql.append(",TEAI." + tempProp);
		// // break;
		// }
		// } else {
		// // sql.append(",TEAI." + tempProp);

		// }
		// }
		sql.append(" ORDER BY ID");
		if (aggrQuery.getPeriod() != null) {
			sql.append(", PR");
		}
		sql.append("), ");
		sql.append(
				"attribute_arrays as (SELECT id, SCOPES, E_TYPES, CREATEDAT, MODIFIEDAT, DELETEDAT, attributeid, jsonb_build_array(jsonb_strip_nulls(jsonb_build_object('@type', jsonb_build_array(ATTRTYPE),");
		for (String aggrFunction : aggrQuery.getAggrFunctions()) {
			switch (aggrFunction) {
				case NGSIConstants.AGGR_METH_SUM:
					sql.append('\'');
					sql.append(NGSIConstants.NGSI_LD_SUM);
					sql.append('\'');
					sql.append(
							",CASE WHEN (JSONB_AGG(X.SUMDATA) FILTER (WHERE X.SUMdata #>>'{@list,0,@value}' is not null)) is not null then ");
					sql.append("JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT('");
					sql.append(NGSIConstants.JSON_LD_LIST);
					sql.append(
							"',JSONB_AGG(X.SUMDATA) FILTER (WHERE X.SUMdata #>>'{@list,0,@value}' is not null))) else null end");
					break;
				case NGSIConstants.AGGR_METH_MIN:
					sql.append('\'');
					sql.append(NGSIConstants.NGSI_LD_MIN);
					sql.append('\'');
					sql.append(
							",CASE WHEN (JSONB_AGG(X.MINdata) FILTER (WHERE X.MINdata #>>'{@list,0,@value}' is not null)) is not null then ");
					sql.append("JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT('");
					sql.append(NGSIConstants.JSON_LD_LIST);
					sql.append(
							"',JSONB_AGG(X.MINDATA) FILTER (WHERE X.MINdata #>>'{@list,0,@value}' is not null))) else null end");
					break;
				case NGSIConstants.AGGR_METH_MAX:
					sql.append('\'');
					sql.append(NGSIConstants.NGSI_LD_MAX);
					sql.append('\'');
					sql.append(
							",CASE WHEN (JSONB_AGG(X.MAXDATA) FILTER (WHERE X.MAXdata #>>'{@list,0,@value}' is not null)) is not null then ");
					sql.append("JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT('");
					sql.append(NGSIConstants.JSON_LD_LIST);
					sql.append(
							"',JSONB_AGG(X.MAXDATA) FILTER (WHERE X.MAXdata #>>'{@list,0,@value}' is not null))) else null end");
					break;
				case NGSIConstants.AGGR_METH_AVG:
					sql.append('\'');
					sql.append(NGSIConstants.NGSI_LD_AVG);
					sql.append('\'');
					sql.append(
							",CASE WHEN (JSONB_AGG(X.AVGDATA) FILTER (WHERE X.AVGdata #>>'{@list,0,@value}' is not null)) is not null then ");
					sql.append("JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT('");
					sql.append(NGSIConstants.JSON_LD_LIST);
					sql.append(
							"',JSONB_AGG(X.AVGDATA) FILTER (WHERE X.AVGdata #>>'{@list,0,@value}' is not null))) else null end");
					break;
				case NGSIConstants.AGGR_METH_STDDEV:
					sql.append('\'');
					sql.append(NGSIConstants.NGSI_LD_STDDEV);
					sql.append('\'');
					sql.append(
							",CASE WHEN (JSONB_AGG(X.STDDEVDATA) FILTER (WHERE X.STDDEVdata #>>'{@list,0,@value}' is not null)) is not null then ");
					sql.append("JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT('");
					sql.append(NGSIConstants.JSON_LD_LIST);
					sql.append(
							"',JSONB_AGG(X.STDDEVDATA) FILTER (WHERE X.STDDEVdata #>>'{@list,0,@value}' is not null))) else null end");
					break;
				case NGSIConstants.AGGR_METH_SUMSQ:
					sql.append('\'');
					sql.append(NGSIConstants.NGSI_LD_SUMSQ);
					sql.append('\'');
					sql.append(
							",CASE WHEN (JSONB_AGG(X.SUMSQDATA) FILTER (WHERE X.SUMSQdata #>>'{@list,0,@value}' is not null)) is not null then ");
					sql.append("JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT('");
					sql.append(NGSIConstants.JSON_LD_LIST);
					sql.append(
							"',JSONB_AGG(X.SUMSQDATA) FILTER (WHERE X.SUMSQdata #>>'{@list,0,@value}' is not null))) else null end");
					break;
				case NGSIConstants.AGGR_METH_TOTAL_COUNT:
					sql.append('\'');
					sql.append(NGSIConstants.NGSI_LD_TOTALCOUNT);
					sql.append('\'');
					sql.append(
							",CASE WHEN (JSONB_AGG(X.TOTALCOUNTDATA) FILTER (WHERE X.TOTALCOUNTdata #>>'{@list,0,@value}' is not null)) is not null then ");
					sql.append("JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT('");
					sql.append(NGSIConstants.JSON_LD_LIST);
					sql.append(
							"',JSONB_AGG(X.TOTALCOUNTDATA) FILTER (WHERE X.TOTALCOUNTdata #>>'{@list,0,@value}' is not null))) else null end");
					break;
				case NGSIConstants.AGGR_METH_DISTINCT_COUNT:
					sql.append('\'');
					sql.append(NGSIConstants.NGSI_LD_DISTINCTCOUNT);
					sql.append('\'');
					sql.append(
							",CASE WHEN (JSONB_AGG(X.DISTINCTCOUNTDATA) FILTER (WHERE X.DISTINCTCOUNTdata #>>'{@list,0,@value}' is not null)) is not null then ");
					sql.append("JSONB_BUILD_ARRAY(JSONB_BUILD_OBJECT('");
					sql.append(NGSIConstants.JSON_LD_LIST);
					sql.append(
							"',JSONB_AGG(X.DISTINCTCOUNTDATA) FILTER (WHERE X.DISTINCTCOUNTdata #>>'{@list,0,@value}' is not null))) else null end");
					break;
				default:
					break;
			}
			sql.append(',');
		}
		sql.setLength(sql.length() - 1);
		sql.append(
				"))) as data_array, ATTRTYPE FROM aggr X GROUP BY id, SCOPES, E_TYPES, CREATEDAT, MODIFIEDAT, DELETEDAT, attributeid, ATTRTYPE)");
		return dollarCount;
	}

	private int generateTimestampForAttr(StringBuilder sql, int dollarCount, TemporalQueryTerm tempQuery,
			AggrTerm aggrQuery) {
		if (aggrQuery.getPeriod() != null) {
			sql.append(",JSONB_BUILD_OBJECT('" + NGSIConstants.JSON_LD_VALUE + "', to_char(pr.period, "
					+ TIMESTAMP_FORMAT + ")), ");
			sql.append("JSONB_BUILD_OBJECT('" + NGSIConstants.JSON_LD_VALUE + "', to_char(pr.period + ");
			sql.append("$");
			sql.append(dollarCount);
			sql.append("::text::interval");
			sql.append(", " + TIMESTAMP_FORMAT + "))");
			return 1;
		} else {
			sql.append(",JSONB_BUILD_OBJECT('" + NGSIConstants.JSON_LD_VALUE + "', to_char(PRSTART, "
					+ TIMESTAMP_FORMAT + ")), ");
			sql.append("JSONB_BUILD_OBJECT('" + NGSIConstants.JSON_LD_VALUE + "', to_char(PRSTOP ");
			sql.append(", " + TIMESTAMP_FORMAT + "))");
			return 0;
		}
	}

	public Uni<QueryResult> query(String tenant,
			List<Tuple3<String[], TypeQueryTerm, String>> idsAndTypeAndIdPattern, AttrsQueryTerm attrsQuery,
			QQueryTerm qQuery, GeoQueryTerm geoQuery, ScopeQueryTerm scopeQuery, Context context, int limit, int offset,
			DataSetIdTerm dataSetIdTerm, String join, int joinLevel, String qToken, PickTerm pickTerm,
			OmitTerm omitTerm, String queryChecksum, boolean splitEntities,
			boolean regEmptyOrNoRegEntryAndNoLinkedQuery, boolean noRootLevelRegEntryAndLinkedQuery, String typePattern,
			boolean localOnly, boolean forceEntitymapCreation, boolean tokenProvided, boolean count,
			OrderByTerm orderBy, boolean metadata, TemporalQueryTerm tempQuery, AggrTerm aggrQuery, int n, int offsetN,
			String nOrder) {
		// it's a large string
		StringBuilder sql = new StringBuilder(2560);
		int dollar = 1;
		Tuple tuple = Tuple.tuple();

		String timeProp = tempQuery != null ? tempQuery.getTimeProperty() : "";
		switch (timeProp) {
			case NGSIConstants.QUERY_PARAMETER_CREATED_AT:
			case NGSIConstants.QUERY_PARAMETER_MODIFIED_AT:
			case NGSIConstants.QUERY_PARAMETER_OBSERVED_AT:
			case NGSIConstants.QUERY_PARAMETER_DELETED_AT:
				break;
			default:
				timeProp = NGSIConstants.QUERY_PARAMETER_OBSERVED_AT;
				break;
		}

		boolean windowFunction = tempQuery != null || attrsQuery != null || qQuery != null || geoQuery != null
				|| dataSetIdTerm != null || pickTerm != null || omitTerm != null;

		if (windowFunction) {
			sql.append("WITH RECURSIVE result_builder AS (( ");
		}
		dollar = addEntityInfoPart(sql, tuple, dollar, idsAndTypeAndIdPattern, scopeQuery, limit, offset, false);
		try {
			dollar = addAttributesPart(sql, tuple, dollar, tempQuery, attrsQuery, qQuery, geoQuery, dataSetIdTerm,
					pickTerm, omitTerm, timeProp, n, offsetN, nOrder, aggrQuery);
		} catch (ResponseException e) {
			return Uni.createFrom().failure(e);
		}
		if (aggrQuery != null) {
			dollar = addAggrQuery(sql, aggrQuery, tempQuery, dollar, tuple, timeProp);
		}
		if (windowFunction) {
			sql.append(", aggregated_batch AS (");
		}
		dollar = addResultPart(sql, tuple, dollar, limit, windowFunction, aggrQuery);
		if (windowFunction) {
			sql.append(
					"""
													), batch_stats AS (
												          SELECT COUNT(*) as found_count,
												                 MAX(createdat) as last_createdat
												          FROM aggregated_batch
												      ),
												      batch_with_last_id AS (
												          SELECT ab.*,
												                 bs.found_count,
												                 bs.last_createdat,
												                 -- Calculate last_id_for_createdat in one pass
												                 MAX(CASE WHEN ab.createdat = bs.last_createdat THEN ab.id ELSE NULL END) OVER() as last_id_for_createdat
												          FROM aggregated_batch ab
												          CROSS JOIN batch_stats bs
												      )
												      SELECT id, e_types, createdat, modifiedat, deletedat, scopes, attribute_ids, attribute_data_arrays,
												             found_count, last_createdat, last_id_for_createdat
												      FROM batch_with_last_id
												  )
																		 			UNION ALL
																		   (
												WITH previous_results AS (SELECT * FROM result_builder),
												prev_stats AS (
							  SELECT
							    MAX(found_count) as found_count,
							    MAX(last_createdat) as last_createdat,
							    MAX(last_id_for_createdat) as last_id_for_createdat
							  FROM previous_results
							),
																		""");
			dollar = addEntityInfoPart(sql, tuple, dollar, idsAndTypeAndIdPattern, scopeQuery, limit, 0, true);
			try {
				dollar = addAttributesPart(sql, tuple, dollar, tempQuery, attrsQuery, qQuery, geoQuery,
						dataSetIdTerm, pickTerm, omitTerm, timeProp, n, offsetN, nOrder, aggrQuery);
			} catch (ResponseException e) {
				return Uni.createFrom().failure(e);
			}
			if (aggrQuery != null) {
				dollar = addAggrQuery(sql, aggrQuery, tempQuery, dollar, tuple, timeProp);
			}
			sql.append(", aggregated_batch AS (");
			dollar = addResultPart(sql, tuple, dollar, limit, windowFunction, aggrQuery);
			sql.append(
					"), batch_stats AS (SELECT COUNT(*) as current_batch_count, MAX(createdat) as current_last_createdat FROM aggregated_batch), combined_stats AS (SELECT ps.found_count + bs.current_batch_count as total_found_count, bs.current_last_createdat as last_createdat FROM prev_stats ps CROSS JOIN batch_stats bs),");
			sql.append(
					"final_batch AS (SELECT ab.*, cs.total_found_count as found_count, cs.last_createdat, MAX(CASE WHEN ab.createdat = cs.last_createdat THEN ab.id ELSE NULL END) OVER() as last_id_for_createdat FROM aggregated_batch ab CROSS JOIN combined_stats cs CROSS JOIN prev_stats ps WHERE ps.found_count < ");
			sql.append(limit);
			sql.append(
					") SELECT id, e_types, createdat, modifiedat, deletedat, scopes, attribute_ids, attribute_data_arrays, found_count, last_createdat, last_id_for_createdat FROM final_batch");
			sql.append(
					")) SELECT id, e_types, createdat, modifiedat, deletedat, scopes, attribute_ids, attribute_data_arrays FROM result_builder LIMIT ");
			sql.append(limit);

		}

		// connectionManager
		// .executeQuery(tenant, "EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) " +
		// sql.toString(), tuple, false)
		// .onItem().transform(rows -> {
		// for (Row row : rows) {
		// System.out.println(row.getString(0));
		// }
		// return null;
		// }).subscribe().with(t -> {
		// });
		// System.out.println(sql.toString());
		// System.out.println(tuple.deepToString());
		return connectionManager.executeQuery(tenant, sql.toString(), tuple, false).onItem().transform(rows -> {

			QueryResult result = new QueryResult(tenant);
			if (limit == 0 && count) {
				result.setCount(rows.iterator().next().getLong(0));
			} else {
				RowIterator<Row> it = rows.iterator();
				Row next = null;
				List<Map<String, Object>> resultData = new ArrayList<Map<String, Object>>(rows.size());
				Map<String, Object> entity;
				while (it.hasNext()) {
					// id, e_types, createdat, modifiedat, deletedat, scopes, attribute_ids,
					// attribute_data_arrays
					next = it.next();
					String id = next.getString(0);
					String[] types = next.getArrayOfStrings(1);
					LocalDateTime createdAt = next.getLocalDateTime(2);

					LocalDateTime modifiedAt = next.getLocalDateTime(3);
					LocalDateTime deletedAt = next.getLocalDateTime(4);
					String[] scopes = next.getArrayOfStrings(5);
					String[] attribIds = next.getArrayOfStrings(6);

					List attribData = next.getJsonArray(7).getList();

					entity = new HashMap<>(5 + attribIds.length);
					entity.put(NGSIConstants.JSON_LD_ID, id);
					entity.put(NGSIConstants.JSON_LD_TYPE, Lists.newArrayList(types));
					entity.put(NGSIConstants.NGSI_LD_CREATED_AT, generateDateTime(createdAt));
					entity.put(NGSIConstants.NGSI_LD_MODIFIED_AT, generateDateTime(modifiedAt));
					if (deletedAt != null) {
						entity.put(NGSIConstants.NGSI_LD_DELETED_AT, generateDateTime(deletedAt));
					}
					if (scopes != null) {
						entity.put(NGSIConstants.NGSI_LD_SCOPE, getScope(scopes));
					}
					if (aggrQuery == null) {
						for (int i = 0; i < attribIds.length; i++) {
							String attribId = attribIds[i];
							entity.put(attribId, attribData.get(i));
						}
					} else {
						for (int i = 0; i < attribIds.length; i++) {
							String attribId = attribIds[i];
							if (NGSIConstants.ENTITY_BASE_PROPS.contains(attribId)) {
								entity.put(attribId, attribData.get(i));
								continue;
							}
							List<Map<String, List<Map<String, List>>>> attribEntry = (List<Map<String, List<Map<String, List>>>>) attribData
									.get(i);
							if (attribEntry.get(0).size() == 1) {
								continue;
							}
							if ((aggrQuery.getAggrFunctions().contains(NGSIConstants.AGGR_METH_MAX)
									|| aggrQuery.getAggrFunctions().contains(NGSIConstants.AGGR_METH_MIN))) {
								postProcessMinOrMaxResults(attribEntry);
							}
							entity.put(attribId, attribEntry);
						}
					}

					resultData.add(entity);
				}

				if (count) {
					Long resultCount = next.getLong(1);
					result.setCount(resultCount);
					long leftAfter = resultCount - (offset + limit);
					if (leftAfter < 0) {
						leftAfter = 0;
					}
					result.setResultsLeftAfter(leftAfter);
				} else {
					if (resultData.size() < limit) {
						result.setResultsLeftAfter(0l);
					} else {
						result.setResultsLeftAfter((long) limit);
					}

				}
				long leftBefore = offset;

				result.setResultsLeftBefore(leftBefore);
				result.setLimit(limit);
				result.setOffset(offset);
				result.setData(resultData);
			}

			return result;
		});
	}

	private List<Map<String, String>> generateDateTime(LocalDateTime dbDate) {
		List<Map<String, String>> result = new ArrayList<>(1);
		Map<String, String> dateEntry = new HashMap<>(2);
		dateEntry.put(NGSIConstants.JSON_LD_TYPE, NGSIConstants.NGSI_LD_DATE_TIME);
		dateEntry.put(NGSIConstants.JSON_LD_VALUE, dbDate.toString() + 'Z');
		result.add(dateEntry);
		return result;
	}

	private int addResultPart(StringBuilder sql, Tuple tuple, int dollar, int limit, boolean windowFunction,
			AggrTerm aggrTerm) {
		if (aggrTerm != null) {
			sql.append("SELECT * FROM (");
		}
		sql.append(
				" SELECT id, e_types, createdat, modifiedat, deletedAt, scopes, array_agg(attributeid) FILTER (WHERE data_array IS NOT NULL");
		if (aggrTerm != null) {
			sql.append(" AND data_array != '[{}]'::jsonb");
		}
		sql.append(") as attribute_ids, jsonb_agg(data_array) FILTER (WHERE data_array IS NOT NULL");
		if (aggrTerm != null) {
			sql.append(" AND data_array != '[{}]'::jsonb");
		}
		sql.append(
				") as attribute_data_arrays FROM attribute_arrays GROUP BY id, createdat, e_types, createdat, modifiedat, deletedat, scopes");
		if (aggrTerm != null) {
			sql.append(") filtered WHERE attribute_ids IS NOT NULL");
		}
		if (!windowFunction) {
			sql.append(" ORDER BY createdat DESC, id ASC");
		}
		sql.append(" LIMIT ");
		sql.append(limit);
		return dollar;
	}

	private int addAttributesPart(StringBuilder sql, Tuple tuple, int dollar, TemporalQueryTerm tempQuery,
			AttrsQueryTerm attrsQuery, QQueryTerm qQuery, GeoQueryTerm geoQuery, DataSetIdTerm dataSetIdTerm,
			PickTerm pickTerm, OmitTerm omitTerm, String timeProp, int n, int offsetN, String nOrder, AggrTerm aggrTerm)
			throws ResponseException {
		if (aggrTerm != null) {
			sql.append("attribute_arrays_temp");
		} else {
			sql.append("attribute_arrays");
		}
		sql.append(
				" AS (SELECT ei.id, ei.createdat, ei.e_types, ei.modifiedat, ei.deletedat, ei.scopes, teai.attributeid, (array_agg(teai.data ORDER BY teai.");
		sql.append(timeProp);
		sql.append(' ');
		sql.append(nOrder);
		sql.append("))");
		if (n > 0) {
			sql.append("[");
			sql.append(1 + offsetN);
			sql.append(":");
			sql.append(n);
			sql.append(']');
		}
		sql.append(
				" as data_array");
		if (aggrTerm != null) {
			sql.append(", MIN(teai.");
			sql.append(timeProp);
			sql.append(") as PRSTART, MAX(teai.");
			sql.append(timeProp);
			sql.append(") as PRSTOP, MAX(teai.data #>> '{@type,0}') as ATTRTYPE");
		}
		sql.append(
				" FROM entityInfos ei INNER JOIN temporalentityattrinstance teai ON teai.temporalentity_id = ei.id WHERE 1=1 AND ");
		if (attrsQuery != null) {
			dollar = attrsQuery.toTempSql(sql, tuple, dollar, dataSetIdTerm);
			sql.append(" AND ");
		}
		if (tempQuery != null) {
			dollar = tempQuery.toSql(sql, tuple, dollar);
			sql.append(" AND ");
		}
		if (geoQuery != null) {
			dollar = geoQuery.toTempSql(sql, tuple, dollar);
			sql.append(" AND ");
		}
		if (dataSetIdTerm != null) {
			dollar = dataSetIdTerm.toTempSql(sql, tuple, dollar);
			sql.append(" AND ");
		}

		sql.setLength(sql.length() - 5);
		sql.append(
				" GROUP BY ei.id, ei.createdat, ei.e_types, ei.createdat, ei.modifiedat, ei.deletedat, ei.scopes, teai.attributeid)");

		return dollar;
	}

	private int addEntityInfoPart(StringBuilder sql, Tuple tuple, int dollar,
			List<Tuple3<String[], TypeQueryTerm, String>> idsAndTypeAndIdPattern, ScopeQueryTerm scopeQuery, int limit,
			int offset, boolean windowFunction) {
		if (!windowFunction) {
			sql.append("WITH ");
		}
		sql.append(
				"entityInfos AS (SELECT id, e_types, createdat, modifiedat, deletedat, scopes FROM temporalentity WHERE ");
		if (windowFunction) {
			sql.append("""
					(
					               createdat > (SELECT last_createdat FROM prev_stats)
					               OR (
					                   createdat = (SELECT last_createdat FROM prev_stats)
					                   AND id > (SELECT last_id_for_createdat FROM prev_stats)
					               )
					) AND
					 """);
		}
		if (idsAndTypeAndIdPattern != null && idsAndTypeAndIdPattern.size() > 0) {
			sql.append('(');
			for (Tuple3<String[], TypeQueryTerm, String> t : idsAndTypeAndIdPattern) {
				TypeQueryTerm typeQuery = t.getItem2();
				String[] entityIds = t.getItem1();
				String idPattern = t.getItem3();
				sql.append(" (1=1");

				if (typeQuery != null) {
					sql.append(" AND ");
					dollar = typeQuery.toSql(sql, tuple, dollar);
				}
				if (entityIds != null) {
					sql.append(" AND id IN (");
					for (String id : entityIds) {
						sql.append('$');
						sql.append(dollar);
						sql.append(',');
						tuple.addString(id);
						dollar++;
					}
					sql.setCharAt(sql.length() - 1, ')');
				}
				if (idPattern != null) {
					sql.append(" AND id ~ $");
					sql.append(dollar);
					dollar++;
					tuple.addString(idPattern);
				}
				sql.append(") OR ");
			}
			sql.setLength(sql.length() - 3);
			sql.append(')');
		}

		if (scopeQuery != null) {
			scopeQuery.toSql(sql);
		}
		sql.append(" ORDER BY createdat DESC, id ASC OFFSET ");
		sql.append(offset);
		sql.append(" LIMIT ");
		if (windowFunction) {
			sql.append("CASE WHEN (SELECT found_count FROM prev_stats) < ");
			sql.append(limit / 3);
			sql.append(" THEN ");
			sql.append(limit * 4);
			sql.append(" ELSE ");
			sql.append(limit * 2);
			sql.append(" END");
		} else {
			sql.append(limit);
		}
		sql.append(
				"), ");
		return dollar;
	}

	private void postProcessMinOrMaxResults(List<Map<String, List<Map<String, List>>>> attribValue) {
		for (Map<String, List<Map<String, List>>> listEntry : attribValue) {

			List<Map<String, List>> maxes = listEntry.get(NGSIConstants.NGSI_LD_MAX);
			if (maxes != null) {
				for (Map<String, List> max : maxes) {
					List<Map<String, List<Map<String, Object>>>> subMaxes = max
							.get(JsonLdConsts.LIST);
					for (Map<String, List<Map<String, Object>>> subMax : subMaxes) {
						List<Map<String, Object>> realValues = subMax.get(JsonLdConsts.LIST);
						String potentialValue = realValues.get(0).get(JsonLdConsts.VALUE)
								.toString();
						if (NumberUtils.isCreatable(potentialValue)) {
							realValues.get(0).put(JsonLdConsts.VALUE,
									NumberUtils.createNumber(potentialValue));
						}

					}
				}
			}
			List<Map<String, List>> mins = listEntry.get(NGSIConstants.NGSI_LD_MIN);
			if (mins != null) {
				for (Map<String, List> min : mins) {
					List<Map<String, List<Map<String, Object>>>> subMins = min
							.get(JsonLdConsts.LIST);
					for (Map<String, List<Map<String, Object>>> subMin : subMins) {
						List<Map<String, Object>> realValues = subMin.get(JsonLdConsts.LIST);
						String potentialValue = realValues.get(0).get(JsonLdConsts.VALUE)
								.toString();
						if (NumberUtils.isCreatable(potentialValue)) {
							realValues.get(0).put(JsonLdConsts.VALUE,
									NumberUtils.createNumber(potentialValue));
						}

					}
				}
			}
		}
	}

}