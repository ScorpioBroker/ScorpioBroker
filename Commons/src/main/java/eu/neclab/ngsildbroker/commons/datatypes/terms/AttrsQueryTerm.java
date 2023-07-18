package eu.neclab.ngsildbroker.commons.datatypes.terms;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

import com.github.jsonldjava.core.Context;
import com.google.common.collect.Lists;
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

	public int toSqlConstructEntity(StringBuilder query, Tuple tuple, int dollar) {
		List<String> attrsList = Lists.newArrayList(getAttrs());
		attrsList.add(NGSIConstants.JSON_LD_ID);
		attrsList.add(NGSIConstants.JSON_LD_TYPE);
		attrsList.add(NGSIConstants.NGSI_LD_CREATED_AT);
		attrsList.add(NGSIConstants.NGSI_LD_MODIFIED_AT);
		for (String attrs : attrsList) {
			query.append("JSONB_BUILD_OBJECT($");
			query.append(dollar);
			query.append(", ENTITY->$");
			query.append(dollar);
			query.append(") ||");
			tuple.addString(attrs);
			dollar++;
		}
		query.append("CASE WHEN ENTITY-> $");
		query.append(dollar);
		query.append(" IS NOT NULL THEN JSONB_BUILD_OBJECT( $");
		query.append(dollar);
		query.append(" , ENTITY-> $");
		query.append(dollar);
		query.append(" ) ELSE '{}'::jsonb END");
		tuple.addString(NGSIConstants.NGSI_LD_SCOPE);
		dollar++;
		return dollar;
	}

}
