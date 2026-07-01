package eu.neclab.ngsildbroker.commons.datatypes.terms;

import java.util.Map;
import java.util.Set;

import com.github.jsonldjava.core.Context;

public class CSFQueryTerm extends QQueryTerm {

	/**
	 *
	 */
	private static final long serialVersionUID = 707863320739851119L;

	CSFQueryTerm() {
		// for serialization
	}

	public CSFQueryTerm(Context context) {
		super(context);
	}

	/**
	 * Evaluates this context source filter against the expanded payload of a
	 * registration, in memory.
	 * <p>
	 * Unlike a regular q query - which cannot be resolved up front because the
	 * matching entities may be spread across several brokers - a CSF works on the
	 * values a registration declares about itself. Those are fully known locally,
	 * so the filter can be evaluated before any context source is contacted, which
	 * is why the evaluation lives here on {@link CSFQueryTerm} rather than on
	 * {@link QQueryTerm}.
	 *
	 * @param registration the expanded NGSI-LD CSourceRegistration payload
	 * @return {@code true} if the registration satisfies the filter
	 */
	public boolean eval(Map<String, Object> registration) {
		if (registration == null) {
			return false;
		}
		return evalTree(this, registration);
	}

	/**
	 * Walks the q tree (groups via firstChild, conjunction/disjunction via
	 * next/nextAnd) the same way {@link QQueryTerm#calculate(java.util.List)} does
	 * for entities, but evaluates each leaf against the registration map using the
	 * existing {@link QQueryTerm#calculate(Map, Set)} comparator.
	 */
	private static boolean evalTree(QQueryTerm term, Map<String, Object> registration) {
		boolean result;
		if (term.getFirstChild() != null && !term.isLinkedQ()) {
			result = evalTree(term.getFirstChild(), registration);
		} else {
			result = term.calculate(registration, (Set<String>) null);
		}
		if (term.hasNext()) {
			if (term.isNextAnd()) {
				result = result && evalTree(term.getNext(), registration);
			} else {
				result = result || evalTree(term.getNext(), registration);
			}
		}
		return result;
	}

}
