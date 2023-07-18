package eu.neclab.ngsildbroker.commons.datatypes.terms;

import java.io.Serializable;
import java.util.List;

import eu.neclab.ngsildbroker.commons.constants.DBConstants;

public class ScopeQueryTerm  implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3852848071227315655L;
	private static final String REGEX_PLUS = "[^\\/]+";
	private static final String REGEX_HASH = ".*";
	private String[] scopeLevels = null;
	private ScopeQueryTerm prev = null;
	private ScopeQueryTerm next = null;
	private boolean nextAnd = true;
	private ScopeQueryTerm firstChild = null;
	private ScopeQueryTerm parent = null;
	
	ScopeQueryTerm() {
		// for serialization
	}

	public String[] getScopeLevels() {
		return scopeLevels;
	}

	public void setScopeLevels(String[] scopeLevels) {
		this.scopeLevels = scopeLevels;
	}

	public boolean hasNext() {
		return next != null;
	}

	public boolean calculate(List<String[]> scopes) {
		boolean result = false;
		if (firstChild == null) {
			result = calculateMe(scopes);
		} else {
			result = firstChild.calculate(scopes);
		}
		if (hasNext()) {
			if (nextAnd) {
				result = result && next.calculate(scopes);
			} else {
				result = result || next.calculate(scopes);
			}
		}

		return result;
	}

	private boolean calculateMe(List<String[]> entryScopes) {
		for (String[] entryScope : entryScopes) {
			int lenEntryScope = entryScope.length - 1;
			boolean result = false;
			for (int i = 0; i < scopeLevels.length; i++) {
				String scopeLevel = scopeLevels[i];
				if (scopeLevel.equals("#")) {
					result = true;
					break;
				}
				if (i > lenEntryScope) {
					result = false;
					break;
				}
				if (i == lenEntryScope) {
					result = true;
				}
				if (scopeLevel.equals("+") || scopeLevel.equals(entryScope[i])) {
					continue;
				}
				result = false;
				break;
			}
			if (result) {
				return true;
			}
		}
		return false;
	}

	public void toSql(StringBuilder result) {
		if (firstChild != null) {
			result.append("(");
			firstChild.toSql(result);
			result.append(")");
		} else {
			result.append("matchScope(" + DBConstants.DBCOLUMN_SCOPE + "," + getSQLScopeQuery() + ")");
		}
		if (hasNext()) {
			if (nextAnd) {
				result.append(" and ");
			} else {
				result.append(" or ");
			}
			next.toSql(result);
		}
	}

	public String getSQLScopeQuery() {
		StringBuilder result = new StringBuilder("'^");
		for (String entry : scopeLevels) {
			switch (entry) {
			case "+":
				result.append("\\/");
				result.append(REGEX_PLUS);
				break;
			case "#":
				result.append(REGEX_HASH);
				break;
			default:
				result.append("\\/");
				result.append(entry);
				break;
			}
		}
		result.append("$'");
		return result.toString();
	}

	public ScopeQueryTerm getNext() {
		return next;
	}

	public void setNext(ScopeQueryTerm next) {
		this.next = next;
		next.parent = parent;
		next.prev = this;
	}

	public boolean isNextAnd() {
		return nextAnd;
	}

	public void setNextAnd(boolean nextAnd) {
		this.nextAnd = nextAnd;
	}

	public ScopeQueryTerm getFirstChild() {
		return firstChild;
	}

	public void setFirstChild(ScopeQueryTerm firstChild) {
		this.firstChild = firstChild;
	}

	public ScopeQueryTerm getParent() {
		return parent;
	}

	public void setParent(ScopeQueryTerm parent) {
		this.parent = parent;
	}

	public ScopeQueryTerm getPrev() {
		return prev;
	}

}
