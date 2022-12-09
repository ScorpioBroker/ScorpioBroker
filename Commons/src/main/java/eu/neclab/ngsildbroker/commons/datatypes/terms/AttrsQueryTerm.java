package eu.neclab.ngsildbroker.commons.datatypes.terms;

import java.util.Set;

import com.github.jsonldjava.core.Context;
import com.google.common.collect.Sets;

import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import io.smallrye.mutiny.tuples.Tuple2;

public class AttrsQueryTerm {

	Set<String> attrs = Sets.newHashSet();
	Context context;

	public AttrsQueryTerm(Context context) {
		this.context = context;
	}

	public void addAttr(String attr) {
		attrs.add(context.expandIri(attr, false, true, null, null));
	}

	public Tuple2<Character, String> toSql(char startChar) throws ResponseException {
		StringBuilder builder = new StringBuilder();
		builder.append(startChar);
		builder.append(" as (SELECT iid FROM attr2iid WHERE ");
		for (String attr : attrs) {
			builder.append("attr = '");
			builder.append(attr);
			builder.append("' OR ");
		}
		return Tuple2.of(startChar, builder.substring(0, builder.length() - 4));
	}

}
