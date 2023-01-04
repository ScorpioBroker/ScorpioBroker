package eu.neclab.ngsildbroker.commons.datatypes.terms;

import java.util.Set;

import com.github.jsonldjava.core.Context;
import com.google.common.collect.Sets;

import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import io.smallrye.mutiny.tuples.Tuple2;

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

	public Tuple2<Character, String> toSql(Character startChar, Character prevResult) throws ResponseException {
		StringBuilder builder = new StringBuilder();
		builder.append(startChar);
		builder.append(" as (SELECT attr2iid.iid FROM ");
		if (prevResult != null) {
			builder.append(prevResult);
			builder.append(" LEFT JOIN attr2iid ON ");
			builder.append(prevResult);
			builder.append(".iid = attr2iid.iid WHERE ");

		} else {
			builder.append(" attr2iid WHERE ");
		}
		for (String attr : attrs) {
			builder.append("attr2iid.attr = '");
			builder.append(attr);
			builder.append("' OR ");
		}
		return Tuple2.of(startChar, builder.substring(0, builder.length() - 4));
	}

	public Set<String> getCompactedAttrs() {
		return compactedAttrs;
	}

}
