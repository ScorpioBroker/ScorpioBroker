package eu.neclab.ngsildbroker.commons.datatypes.terms;

import com.github.jsonldjava.core.Context;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import io.vertx.core.json.JsonArray;
import io.vertx.mutiny.sqlclient.Tuple;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringJoiner;

public class DataSetIdTerm implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = 7407313014717911008L;
	Set<String> ids = Sets.newHashSet();

	public DataSetIdTerm() {
		// for serialization
	}

	public Set<String> getIds() {
		return ids;
	}

	public void addId(String id) {
		this.ids.add(id);
	}

	public void setIds(Set<String> ids) {
		this.ids = ids;
	}

	public void calculate(List<Map<String, Object>> queryResult) {
		for (Map<String, Object> entity : queryResult) {
			Iterator<Entry<String, Object>> it = entity.entrySet().iterator();
			while (it.hasNext()) {
				Entry<String, Object> attrs = it.next();
				String attrsName = attrs.getKey();
				if (NGSIConstants.ENTITY_BASE_PROPS.contains(attrsName)) {
					continue;
				}
				List<Map<String, Object>> attrsValueList = (List<Map<String, Object>>) attrs.getValue();
				Iterator<Map<String, Object>> it2 = attrsValueList.iterator();
				while (it2.hasNext()) {
					Map<String, Object> attrInstance = it2.next();
					Object datasetObj = attrInstance.get(NGSIConstants.NGSI_LD_DATA_SET_ID);
					if (datasetObj == null) {
						if (!ids.contains(NGSIConstants.JSON_LD_NONE)) {
							it2.remove();
						}
					} else {
						String datasetId = ((Map<String, String>) datasetObj).get(NGSIConstants.JSON_LD_ID);
						if (!ids.contains(NGSIConstants.JSON_LD_NONE)) {
							it2.remove();
						}
					}
				}
				if (attrsValueList.isEmpty()) {
					it.remove();
				}
			}
		}
	}
	public int toSql(StringBuilder query,StringBuilder followUp, Tuple tuple, int dollar, PickTerm pick, OmitTerm omit, AttrsQueryTerm attrsQuery) {
		query.append("EXISTS (SELECT FROM JSONB_EACH(ENTITY) as attrs, JSONB_ARRAY_ELEMENTS(attrs.value) as elem WHERE NOT attrs.key = ANY(ARRAY['");
		query.append(NGSIConstants.JSON_LD_ID);
		query.append("','");
		query.append(NGSIConstants.JSON_LD_TYPE);
		query.append("','");
		query.append(NGSIConstants.NGSI_LD_CREATED_AT);
		query.append("','");
		query.append(NGSIConstants.NGSI_LD_MODIFIED_AT);
		query.append("','");
		query.append(NGSIConstants.NGSI_LD_OBSERVED_AT);
		query.append("'])");
		
		
		
		followUp.append("EXISTS (SELECT FROM JSONB_EACH(ENTITY) as attrs, JSONB_ARRAY_ELEMENTS(attrs.value) as elem WHERE NOT attrs.key = ANY(ARRAY[''");
		followUp.append(NGSIConstants.JSON_LD_ID);
		followUp.append("'',''");
		followUp.append(NGSIConstants.JSON_LD_TYPE);
		followUp.append("'',''");
		followUp.append(NGSIConstants.NGSI_LD_CREATED_AT);
		followUp.append("'',''");
		followUp.append(NGSIConstants.NGSI_LD_MODIFIED_AT);
		followUp.append("'',''");
		followUp.append(NGSIConstants.NGSI_LD_OBSERVED_AT);
		followUp.append("''])");
		
		if(pick != null || omit != null || attrsQuery !=null) {
			Set<String> attrs = null;
			if(omit != null) {
				attrs = omit.getAllTopLevelAttribs(false);
			}
			if(pick != null) {
				attrs = pick.getAllTopLevelAttribs(true);
			}
			if(attrsQuery != null) {
				attrs = attrsQuery.getAttrs();
			}
			query.append(" AND ");
			followUp.append(" AND ");
			if(omit != null) {
				query.append("NOT ");
				followUp.append("NOT ");	
			}
			query.append("attrs.key = ANY(ARRAY[");
			followUp.append("attrs.key = ANY(ARRAY[''' || ");
			for(String attrib: attrs) {
				query.append('$');
				query.append(dollar);
				query.append(',');
				
				followUp.append('$');
				followUp.append(dollar);
				followUp.append(" || ''',''' || ");
				
				tuple.addString(attrib);
				dollar++;
			}
			followUp.setLength(followUp.length() - 8);
			query.setLength(query.length() - 1);
			query.append("])");
			followUp.append("])");
		}
		query.append(" AND (");
		followUp.append(" AND (");
		boolean onlyNone = false;
		if(ids.contains(NGSIConstants.JSON_LD_NONE)) {
			query.append("NOT elem ? '");
			query.append(NGSIConstants.NGSI_LD_DATA_SET_ID);
			query.append("'");
			
			followUp.append("NOT elem ? ''");
			followUp.append(NGSIConstants.NGSI_LD_DATA_SET_ID);
			followUp.append("''");
			onlyNone = true;
			
			if(ids.size() > 1) {
				query.append(" OR ");	
				followUp.append(" OR ");
				onlyNone = false;
			}
		}
		if(!onlyNone) {
			query.append("(elem ? '");
			query.append(NGSIConstants.NGSI_LD_DATA_SET_ID);
			query.append("' and elem #>> '{");
			query.append(NGSIConstants.NGSI_LD_DATA_SET_ID);
			query.append(",0,");
			query.append(NGSIConstants.JSON_LD_ID);
			query.append("}' = ANY(ARRAY[");
			
			followUp.append("(elem ? ''");
			followUp.append(NGSIConstants.NGSI_LD_DATA_SET_ID);
			followUp.append("'' and elem #>> ''{");
			followUp.append(NGSIConstants.NGSI_LD_DATA_SET_ID);
			followUp.append(",0,");
			followUp.append(NGSIConstants.JSON_LD_ID);
			followUp.append("}'' = ANY(ARRAY[''' || ");
			for(String id: ids) {
				if(NGSIConstants.JSON_LD_NONE.equals(id)) {
					continue;
				}
				query.append('$');
				query.append(dollar);
				query.append(',');
				followUp.append('$');
				followUp.append(dollar);
				followUp.append(" || ''',''' || ");
				dollar++;
				tuple.addString(id);
			}
			query.setLength(query.length() - 1);
			query.append("])");
			followUp.setLength(followUp.length() - 8);
			followUp.append("])");
		}
		query.append(")");
		followUp.append(")");
		followUp.append("))");
		query.append("))");
		
		return dollar;
	}
	
	public int toSqlConstructEntity(StringBuilder query, Tuple tuple, String tableToUse, int dollar) {
		query.append("JSONB_STRIP_NULLS(JSONB_OBJECT_AGG(");
		query.append(tableToUse);
		query.append(".KEY, CASE WHEN ");
		query.append(tableToUse);
		query.append(".KEY = ANY('{");
		query.append(NGSIConstants.JSON_LD_ID);
		query.append(',');
		query.append(NGSIConstants.JSON_LD_TYPE);
		query.append(',');
		query.append(NGSIConstants.NGSI_LD_SCOPE);
		query.append(',');
		query.append(NGSIConstants.NGSI_LD_CREATED_AT);
		query.append(',');
		query.append(NGSIConstants.NGSI_LD_MODIFIED_AT);
		query.append("}') THEN ");
		query.append(tableToUse);
		query.append(".VALUE ELSE (SELECT CASE WHEN jsonb_array_length(filtered.res) > 0 THEN filtered.res ELSE NULL::jsonb END FROM (SELECT jsonb_agg(val) as res FROM jsonb_array_elements(");
		query.append(tableToUse);
		query.append(".VALUE) as val where ");
		if(ids.remove(NGSIConstants.JSON_LD_NONE)) {
			query.append("NOT val ? '");
			query.append(NGSIConstants.NGSI_LD_DATA_SET_ID);
			query.append("'");
			if(!ids.isEmpty()) {
				query.append(" OR ");	
			}
		}
		if(!ids.isEmpty()) {
			query.append("val ? '");
			query.append(NGSIConstants.NGSI_LD_DATA_SET_ID);
			query.append("' and val #>> '{");
			query.append(NGSIConstants.NGSI_LD_DATA_SET_ID);
			query.append(",0,");
			query.append(NGSIConstants.JSON_LD_ID);
			query.append("}' = ANY($");
			query.append(dollar);
			query.append(")");
			tuple.addArrayOfString(ids.toArray(new String[0]));
			dollar++;
		}
		query.append(") as filtered) END ))");
		return dollar;
	}
	
	public int toSqlConstructEntity(StringBuilder query, StringBuilder followUp, Tuple tuple, String tableToUse, int dollar) {
		query.append("JSONB_STRIP_NULLS(JSONB_OBJECT_AGG(");
		query.append(tableToUse);
		query.append(".KEY, CASE WHEN ");
		query.append(tableToUse);
		query.append(".KEY = ANY('{");
		query.append(NGSIConstants.JSON_LD_ID);
		query.append(',');
		query.append(NGSIConstants.JSON_LD_TYPE);
		query.append(',');
		query.append(NGSIConstants.NGSI_LD_SCOPE);
		query.append(',');
		query.append(NGSIConstants.NGSI_LD_CREATED_AT);
		query.append(',');
		query.append(NGSIConstants.NGSI_LD_MODIFIED_AT);
		query.append("}') THEN ");
		query.append(tableToUse);
		query.append(".VALUE ELSE (SELECT CASE WHEN jsonb_array_length(filtered.res) > 0 THEN filtered.res ELSE NULL::jsonb END FROM (SELECT jsonb_agg(val) as res FROM jsonb_array_elements(");
		query.append(tableToUse);
		query.append(".VALUE) as val where ");
		
		followUp.append("JSONB_STRIP_NULLS(JSONB_OBJECT_AGG(");
		followUp.append(tableToUse);
		followUp.append(".KEY, CASE WHEN ");
		followUp.append(tableToUse);
		followUp.append(".KEY = ANY(''{");
		followUp.append(NGSIConstants.JSON_LD_ID);
		followUp.append(',');
		followUp.append(NGSIConstants.JSON_LD_TYPE);
		followUp.append(',');
		followUp.append(NGSIConstants.NGSI_LD_SCOPE);
		followUp.append(',');
		followUp.append(NGSIConstants.NGSI_LD_CREATED_AT);
		followUp.append(',');
		followUp.append(NGSIConstants.NGSI_LD_MODIFIED_AT);
		followUp.append("}'') THEN ");
		followUp.append(tableToUse);
		followUp.append(".VALUE ELSE (SELECT CASE WHEN jsonb_array_length(filtered.res) > 0 THEN filtered.res ELSE NULL::jsonb END FROM (SELECT jsonb_agg(val) as res FROM jsonb_array_elements(");
		followUp.append(tableToUse);
		followUp.append(".VALUE) as val where ");
		boolean onlyNone = false;
		if(ids.contains(NGSIConstants.JSON_LD_NONE)) {
			query.append("NOT val ? '");
			query.append(NGSIConstants.NGSI_LD_DATA_SET_ID);
			query.append("'");
			
			followUp.append("NOT val ? ''");
			followUp.append(NGSIConstants.NGSI_LD_DATA_SET_ID);
			followUp.append("''");
			onlyNone = true;
			if(ids.size() > 1) {
				query.append(" OR ");	
				followUp.append(" OR ");
				onlyNone = false;
			}
		}
		if(!onlyNone) {
			query.append("val ? '");
			query.append(NGSIConstants.NGSI_LD_DATA_SET_ID);
			query.append("' and val #>> '{");
			query.append(NGSIConstants.NGSI_LD_DATA_SET_ID);
			query.append(",0,");
			query.append(NGSIConstants.JSON_LD_ID);
			query.append("}' = ANY(ARRAY[");
			
			followUp.append("val ? ''");
			followUp.append(NGSIConstants.NGSI_LD_DATA_SET_ID);
			followUp.append("'' and val #>> ''{");
			followUp.append(NGSIConstants.NGSI_LD_DATA_SET_ID);
			followUp.append(",0,");
			followUp.append(NGSIConstants.JSON_LD_ID);
			followUp.append("}'' = ANY(ARRAY[''' || ");
			for(String id: ids) {
				if(NGSIConstants.JSON_LD_NONE.equals(id)) {
					continue;
				}
				query.append('$');
				query.append(dollar);
				query.append(',');
				followUp.append('$');
				followUp.append(dollar);
				followUp.append(" || ''',''' || ");
				dollar++;
				tuple.addString(id);
			}
			query.setLength(query.length() - 1);
			query.append("])");
			followUp.setLength(followUp.length() - 8);
			followUp.append("])");
		}
		query.append(") as filtered) END ))");
		followUp.append(") as filtered) END ))");
		return dollar;
	}

	public boolean calculateEntity(Map<String, Object> entity) {
		Iterator<Entry<String, Object>> it = entity.entrySet().iterator();
		while (it.hasNext()) {
			Entry<String, Object> attrs = it.next();
			String attrsName = attrs.getKey();
			if (NGSIConstants.ENTITY_BASE_PROPS.contains(attrsName)) {
				continue;
			}
			List<Map<String, Object>> attrsValueList = (List<Map<String, Object>>) attrs.getValue();
			Iterator<Map<String, Object>> it2 = attrsValueList.iterator();
			while (it2.hasNext()) {
				Map<String, Object> attrInstance = it2.next();
				Object datasetObj = attrInstance.get(NGSIConstants.NGSI_LD_DATA_SET_ID);
				if (datasetObj == null) {
					if (!ids.contains(NGSIConstants.JSON_LD_NONE)) {
						it2.remove();
					}
				} else {
					String datasetId = ((Map<String, String>) datasetObj).get(NGSIConstants.JSON_LD_ID);
					if (!ids.contains(NGSIConstants.JSON_LD_NONE)) {
						it2.remove();
					}
				}
			}
			if (attrsValueList.isEmpty()) {
				it.remove();
			}
		}
		return !entity.isEmpty();
	}

}
