package eu.neclab.ngsildbroker.commons.datatypes.terms;

import java.util.ArrayList;
import java.util.Set;

import io.smallrye.mutiny.tuples.Tuple2;

public class LanguageQueryTerm {

	ArrayList<Tuple2<Set<String>, Float>> entries;

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

}
