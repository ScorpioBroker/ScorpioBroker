package eu.neclab.ngsildbroker.commons.datatypes.terms;

import java.util.Set;

public class AggrTerm {

	private String period;
	private Set<String> aggrFunctions;

	public String getPeriod() {
		return period;
	}

	public void setPeriod(String period) {
		this.period = period;
	}

	public Set<String> getAggrFunctions() {
		return aggrFunctions;
	}

	public void setAggrFunctions(Set<String> aggrFunctions) {
		this.aggrFunctions = aggrFunctions;
	}

}
