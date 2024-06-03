package eu.neclab.ngsildbroker.commons.datatypes.terms;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.github.jsonldjava.core.Context;
import com.google.common.collect.Sets;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import io.vertx.mutiny.sqlclient.Tuple;

public class AttrsQueryTerm implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7407313014717911008L;
	Set<String> compactedAttrs = Sets.newHashSet();
	Set<String> attrs = Sets.newHashSet();
	Context context;

	AttrsQueryTerm() {
		// for serialization
	}

	public AttrsQueryTerm(Context context) {
		this.context = context;
	}

	public void addAttr(String attr) {
		attrs.add(context.expandIri(attr, false, true, null, null));
		compactedAttrs.add(attr);
	}

	public Set<String> getAttrs() {
		return attrs;
	}

	public Set<String> getCompactedAttrs() {
		return compactedAttrs;
	}

	public int toSql(StringBuilder query, Tuple tuple, int dollar) {
		query.append("ENTITY ?| $");
		query.append(dollar);
		dollar++;
		tuple.addArrayOfString(attrs.toArray(new String[0]));
		return dollar;
	}

	public int toSqlConstructEntity(StringBuilder query, Tuple tuple, int dollar, DataSetIdTerm datasetIdTerm) {

		query.append("JSONB_BUILD_OBJECT('");
		query.append(NGSIConstants.JSON_LD_ID);
		query.append("', ENTITY -> '");
		query.append(NGSIConstants.JSON_LD_ID);
		query.append("', '");
		query.append(NGSIConstants.JSON_LD_TYPE);
		query.append("', ENTITY -> '");
		query.append(NGSIConstants.JSON_LD_TYPE);
		query.append("', '");
		query.append(NGSIConstants.NGSI_LD_CREATED_AT);
		query.append("', ENTITY -> '");
		query.append(NGSIConstants.NGSI_LD_CREATED_AT);
		query.append("', '");
		query.append(NGSIConstants.NGSI_LD_MODIFIED_AT);
		query.append("', ENTITY -> '");
		query.append(NGSIConstants.NGSI_LD_MODIFIED_AT);
		query.append("')");

		if (datasetIdTerm == null) {
			for (String attrs : getAttrs()) {
				query.append(" || ");
				query.append("CASE WHEN ENTITY ? ");
				query.append("$");
				query.append(dollar);
				query.append(" THEN JSONB_BUILD_OBJECT($");
				query.append(dollar);
				query.append(", ENTITY->$");
				query.append(dollar);
				query.append(" ) ELSE '{}'::jsonb END");
				tuple.addString(attrs);
				dollar++;
			}
		} else {
			for (String attrs : getAttrs()) {
				query.append(" || ");
				query.append("CASE WHEN ENTITY ? ");
				query.append("$");
				query.append(dollar);
				query.append(" THEN (SELECT CASE WHEN jsonb_array_length(filtered.res) > 0 THEN JSONB_BUILD_OBJECT($");
				query.append(dollar);
				query.append(", filtered.res) ELSE '{}'::jsonb END FROM (SELECT jsonb_agg(val) as res FROM jsonb_array_elements(ENTITY -> $");
				query.append(dollar);
				tuple.addString(attrs);
				dollar++;
				query.append(") as val where ");
				if(datasetIdTerm.ids.remove(NGSIConstants.JSON_LD_NONE)) {
					query.append("NOT val ? '");
					query.append(NGSIConstants.NGSI_LD_DATA_SET_ID);
					query.append("'");
					if(!datasetIdTerm.ids.isEmpty()) {
						query.append(" OR ");	
					}
				}
				if(!datasetIdTerm.ids.isEmpty()) {
					query.append("val ? '");
					query.append(NGSIConstants.NGSI_LD_DATA_SET_ID);
					query.append("' and val #>> '{");
					query.append(NGSIConstants.NGSI_LD_DATA_SET_ID);
					query.append(",0,");
					query.append(NGSIConstants.JSON_LD_ID);
					query.append("}' = ANY($");
					query.append(dollar);
					query.append(")");
					tuple.addArrayOfString(datasetIdTerm.ids.toArray(new String[0]));
					dollar++;
				}
				query.append(") as filtered)");
			}
		}
		query.append(" || ");
		query.append("CASE WHEN ENTITY-> '");
		query.append(NGSIConstants.NGSI_LD_SCOPE);
		query.append("' IS NOT NULL THEN JSONB_BUILD_OBJECT('");
		query.append(NGSIConstants.NGSI_LD_SCOPE);
		query.append("' , ENTITY-> '");
		query.append(NGSIConstants.NGSI_LD_SCOPE);
		query.append("' ) ELSE '{}'::jsonb END");
		return dollar;
	}

	public void calculateQuery(List<Map<String, Object>> resultData) {
		for (Map<String, Object> entity : resultData) {
			Iterator<Entry<String, Object>> it = entity.entrySet().iterator();
			while (it.hasNext()) {
				String key = it.next().getKey();
				if (!NGSIConstants.ENTITY_BASE_PROPS.contains(key) && !attrs.contains(key)) {
					it.remove();
				}
			}
		}
	}

}
