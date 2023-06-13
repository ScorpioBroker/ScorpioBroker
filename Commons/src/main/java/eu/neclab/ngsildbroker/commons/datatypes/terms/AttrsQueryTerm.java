package eu.neclab.ngsildbroker.commons.datatypes.terms;

import java.util.Set;

import com.github.jsonldjava.core.Context;
import com.google.common.collect.Sets;

import io.vertx.mutiny.sqlclient.Tuple;

public class AttrsQueryTerm {

	Set<String> compactedAttrs = Sets.newHashSet();
	Set<String> attrs = Sets.newHashSet();
	Context context;

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

}
