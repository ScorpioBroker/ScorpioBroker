package eu.neclab.ngsildbroker.commons.datatypes.terms;

import java.util.List;

import io.smallrye.mutiny.tuples.Tuple2;

public class ScopeQueryTerm {

	private static final String REGEX_PLUS = "[^\\/]+";
	private static final String REGEX_HASH = ".*";
	private String[] scopeLevels = null;
	private ScopeQueryTerm prev = null;
	private ScopeQueryTerm next = null;
	private boolean nextAnd = true;
	private ScopeQueryTerm firstChild = null;
	private ScopeQueryTerm parent = null;

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

	public Tuple2<Character, String> toSql(char startChar) {
		StringBuilder builder = new StringBuilder();
		StringBuilder builderFinalLine = new StringBuilder();
		StringBuilder finalTables = new StringBuilder();
		char finalChar = toSql(builder, builderFinalLine, finalTables, startChar, "escope2iid");
		if (!finalTables.isEmpty()) {
			finalChar++;
			builder.append(',');
			builder.append(finalChar);
			builder.append(" as (SELECT escope2iid.iid AS iid FROM escope2iid,");
			builder.append(finalTables);
			builder.append(" WHERE ");
			builder.append(builderFinalLine);
		} else {
			finalChar++;
			builder.append(',');
			builder.append(finalChar);
			builder.append(" as (SELECT iid FROM ");
			builder.append((char) (finalChar - 1));
		}
		return Tuple2.of(finalChar, builder.toString());
	}

	private char toSql(StringBuilder result, StringBuilder resultFinalLine, StringBuilder finalTables, char currentChar,
			String sqlTable) {
		if (scopeLevels == null) {
			ScopeQueryTerm current = this;
			while (current.firstChild != null) {
				current = current.firstChild;
			}
			return current.next.toSql(result, resultFinalLine, finalTables, currentChar, sqlTable);
		} else {
			int andCounter = 1;
			result.append(currentChar);
			result.append(" as (SELECT ");
			result.append(sqlTable);
			result.append(".iid, ");
			result.append(sqlTable);
			result.append(".e_scope FROM ");
			result.append(sqlTable);
			result.append(" WHERE ");
			result.append(sqlTable);
			result.append(".e_scope ~ ");
			result.append(getSQLScopeQuery());
			if (hasNext()) {
				result.append(" or ");
			}
			ScopeQueryTerm current = this;

			while (current.hasNext() || current.firstChild != null) {
				if (current.firstChild != null) {
					resultFinalLine.append('(');
					currentChar = current.firstChild.toSql(result, resultFinalLine, finalTables, currentChar, sqlTable);
					resultFinalLine.append(')');
					break;
				}
				current = current.getNext();
				if (current.scopeLevels != null) {
					result.append(sqlTable);
					result.append(".e_scope");
					result.append(getSQLScopeQuery());
					andCounter++;
					if ((current.getPrev() != null && current.isNextAnd() != current.getPrev().isNextAnd())
							|| current.firstChild != null) {
						if (current.getPrev() != null && current.getPrev().isNextAnd()) {
							result.append(" GROUP BY ");
							result.append(sqlTable);
							result.append(".iid, ");
							result.append(sqlTable);
							result.append(".e_scope HAVING COUNT(");
							result.append(sqlTable);
							result.append(".e_scope)=");
							result.append(andCounter);
						}
						if (current.nextAnd) {
							sqlTable = currentChar + "";
						} else {
							resultFinalLine.append("escope2iid.iid=");
							resultFinalLine.append(currentChar);
							resultFinalLine.append(".iid");
							resultFinalLine.append(" or ");
							finalTables.append(currentChar);
							finalTables.append(',');
							sqlTable = "escope2iid";
						}
						result.append("),");
						andCounter = 0;
						currentChar++;
						if (current.hasNext() && current.next.getFirstChild() == null) {
							result.append(currentChar);
							result.append(" as (SELECT ");
							result.append(sqlTable);
							result.append(".iid, ");
							result.append(sqlTable);
							result.append(".e_scope FROM ");
							result.append(sqlTable);
							result.append(" WHERE ");
						}
					} else {
						if (current.hasNext()) {
							result.append(" or ");
						}
					}

				}

			}
			if (current.getPrev() != null && current.getPrev().isNextAnd()) {
				result.append(" GROUP BY ");
				result.append(sqlTable);
				result.append(".iid, ");
				result.append(sqlTable);
				result.append(".e_scope HAVING COUNT(");
				result.append(sqlTable);
				result.append(".e_scope)=");
				result.append(andCounter);
			}
			if (current.scopeLevels != null) {
				resultFinalLine.append("escope2iid.iid=");
				resultFinalLine.append(currentChar);
				resultFinalLine.append(".iid");
				finalTables.append(currentChar);
				result.append(')');
			}
			return currentChar;

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
