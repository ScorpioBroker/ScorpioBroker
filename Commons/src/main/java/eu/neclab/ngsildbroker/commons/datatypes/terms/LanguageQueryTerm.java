package eu.neclab.ngsildbroker.commons.datatypes.terms;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Set;

import com.google.common.collect.Lists;

import io.smallrye.mutiny.tuples.Tuple2;

public class LanguageQueryTerm implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3970320741960527943L;

	public LanguageQueryTerm() {
		// for serialization
	}

	ArrayList<Tuple2<Set<String>, Float>> entries = Lists.newArrayList();

	public ArrayList<Tuple2<Set<String>, Float>> getEntries() {
		return entries;
	}

	public void setEntries(ArrayList<Tuple2<Set<String>, Float>> entries) {
		this.entries = entries;
	}

	public void addTuple(Tuple2<Set<String>, Float> tuple) {
		this.entries.add(tuple);
	}

	public void sort() {
		this.entries.sort((o1, o2) -> {
			return o1.getItem2().compareTo(o2.getItem2());
		});
	}

	public void toRequestString(StringBuilder result) {
		result.append("lang=");
		for (Tuple2<Set<String>, Float> entry : entries) {
			result.append(String.join(",", entry.getItem1()));
			result.append(";q=");
			result.append(entry.getItem2());
			result.append(',');
		}
		result.setCharAt(result.length() - 1, '&');
	}

}
