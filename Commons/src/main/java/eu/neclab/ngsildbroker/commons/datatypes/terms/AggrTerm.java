package eu.neclab.ngsildbroker.commons.datatypes.terms;

import java.io.Serializable;
import java.util.Set;

public class AggrTerm  implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4795426642930378574L;
	private String period;
	private Set<String> aggrFunctions;

	AggrTerm() {
		// for serialization
	}

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
