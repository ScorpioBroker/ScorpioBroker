package eu.neclab.ngsildbroker.commons.datatypes.terms;

import java.io.Serializable;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import com.fasterxml.jackson.annotation.JsonIdentityInfo;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.ObjectIdGenerators;
import com.github.jsonldjava.core.Context;
import com.google.common.collect.Sets;

import eu.neclab.ngsildbroker.commons.datatypes.BaseProperty;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import io.vertx.mutiny.sqlclient.Tuple;

@JsonIdentityInfo(generator = ObjectIdGenerators.PropertyGenerator.class, property = "id")
public class TypeQueryTerm implements Serializable {

	@JsonIgnore
	private static int idCount = 0;

	private static synchronized int nextId() {
		idCount++;
		return idCount;
	}

	protected int id = nextId();

	/**
	 * 
	 */
	private static final long serialVersionUID = -7987777238025017539L;
	private Context context;
	private TypeQueryTerm next = null;
	private TypeQueryTerm prev = null;
	private boolean nextAnd = true;
	private TypeQueryTerm firstChild = null;
	private TypeQueryTerm parent = null;
	private String type = null;
	private Set<String> allTypes = null;

	TypeQueryTerm() {
		// for serialization
	}

	public TypeQueryTerm(Context context) {
		this.context = context;

	}

	public boolean hasNext() {
		return next != null;
	}

	public boolean hasPrev() {
		return prev != null;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = context.expandIri(type, false, true, null, null);
	}

	public boolean calculate(BaseProperty property) throws ResponseException {
		ArrayList<BaseProperty> temp = new ArrayList<BaseProperty>();
		temp.add(property);
		return false;// calculate(temp);

	}

	public TypeQueryTerm getNext() {
		return next;
	}

	public TypeQueryTerm getPrev() {
		return prev;
	}

	public void setPrev(TypeQueryTerm prev) {
		this.prev = prev;
	}

	public void setNext(TypeQueryTerm next) {
		this.next = next;
		this.next.setParent(this.getParent());
		this.next.setPrev(this);
	}

	public boolean isNextAnd() {
		return nextAnd;
	}

	public void setNextAnd(boolean nextAnd) {
		this.nextAnd = nextAnd;
	}

	public TypeQueryTerm getFirstChild() {
		return firstChild;
	}

	public void setFirstChild(TypeQueryTerm firstChild) {
		this.firstChild = firstChild;
		this.firstChild.setParent(this);
	}

	public TypeQueryTerm getParent() {
		return parent;
	}

	public void setParent(TypeQueryTerm parent) {
		this.parent = parent;
	}

//	public boolean equals(Object obj, boolean ignoreKids) {
//		if (this == obj)
//			return true;
//		if (obj == null)
//			return false;
//		if (getClass() != obj.getClass())
//			return false;
//		TypeQueryTerm other = (TypeQueryTerm) obj;
//		if (attribute == null) {
//			if (other.attribute != null)
//				return false;
//		} else if (!attribute.equals(other.attribute))
//			return false;
//		if (!ignoreKids) {
//			if (firstChild == null) {
//				if (other.firstChild != null)
//					return false;
//			} else if (!firstChild.equals(other.firstChild))
//				return false;
//		}
//		if (next == null) {
//			if (other.next != null)
//				return false;
//		} else if (!next.equals(other.next))
//			return false;
//		if (nextAnd != other.nextAnd)
//			return false;
//		if (operant == null) {
//			if (other.operant != null)
//				return false;
//		} else if (!operant.equals(other.operant))
//			return false;
//		if (operator == null) {
//			if (other.operator != null)
//				return false;
//		} else if (!operator.equals(other.operator))
//			return false;
//		if (parent == null) {
//			if (other.parent != null)
//				return false;
//		} else if (!parent.equals(other.parent, true))
//			return false;
//		return true;
//	}

//	@Override
//	public boolean equals(Object obj) {
//		return equals(obj, false);
//	}

	public int toBroadSql(StringBuilder result, StringBuilder queryToStoreWherePart, Tuple tuple, int dollar) {
		result.append("e_types && ARRAY[");
		queryToStoreWherePart.append("e_types && ARRAY[''' || ");
		for (String type : allTypes) {
			result.append('$');
			result.append(dollar);
			result.append(',');

			queryToStoreWherePart.append('$');
			queryToStoreWherePart.append(dollar);
			queryToStoreWherePart.append(" || ''',''' || ");

			dollar++;
			tuple.addString(type);
		}
		queryToStoreWherePart.setLength(queryToStoreWherePart.length() - 8);
		queryToStoreWherePart.append("]::text[]");
		result.setCharAt(result.length() - 1, ']');
		result.append("::text[]");
		return dollar;
	}

	public int toSql(StringBuilder result, Tuple tuple, int dollar) {
		if (type == null || type.isEmpty()) {
			TypeQueryTerm current = this;
			while (current.firstChild != null) {
				current = current.firstChild;
			}
			current.next.toSql(result, tuple, dollar);
		} else {
			result.append("(e_types ");
			if (next != null && nextAnd) {
				result.append("@> ");
			} else {
				result.append("&& ");
			}
			result.append("ARRAY[$");
			result.append(dollar);
			dollar++;
			tuple.addString(type);
			if (!hasNext()) {
				result.append("]::text[])");
				return dollar;
			}
			TypeQueryTerm current = this;
			Boolean childPresentFlag = false;
			while (current.hasNext() || current.firstChild != null) {
				if (current.firstChild != null) {
					result.append("]::text[]");
					if (current.prev.nextAnd) {
						result.append(" AND (");
					} else {
						result.append(" OR (");
					}
					childPresentFlag = true;
					dollar = current.firstChild.toSql(result, tuple, dollar);
					result.append(")");
					break;
				}
				current = current.getNext();
				if (current.type != null && !current.type.isEmpty()) {
					if (current.prev.nextAnd != current.nextAnd && current.next != null) {
						result.append("]");
						if (current.prev.nextAnd) {
							result.append(" AND ");
						} else {
							result.append(" OR ");
						}
						result.append("e_types ");
						if (current.nextAnd) {
							result.append("@> ");
						} else {
							result.append("&& ");
						}
						result.append("ARRAY[$");
					} else {
						result.append(",$");
					}
					result.append(dollar);
					dollar++;
					tuple.addString(current.type);
				}
			}

			if (childPresentFlag) {
				result.append(") ");
			} else {
				result.append("]::text[]) ");
			}
		}
		return dollar;
	}

	public int toSql(StringBuilder result, StringBuilder followUp, Tuple tuple, int dollar) {
		if (type == null || type.isEmpty()) {
			TypeQueryTerm current = this;
			while (current.firstChild != null) {
				current = current.firstChild;
			}
			dollar = current.toSql(result, tuple, dollar);
		} else {
			result.append("(e_types ");
			followUp.append("(e_types ");
			if (next != null && nextAnd) {
				result.append("@> ");
				followUp.append("@> ");
			} else {
				result.append("&& ");
				followUp.append("&& ");
			}
			result.append("ARRAY[$");
			result.append(dollar);
			followUp.append("ARRAY[''' || $");
			followUp.append(dollar);
			followUp.append("||'''");
			dollar++;
			tuple.addString(type);
			if (!hasNext()) {
				result.append("]::text[])");
				followUp.append("]::text[])");
				return dollar;
			}
			TypeQueryTerm current = this;
			Boolean childPresentFlag = false;
			while (current.hasNext() || current.firstChild != null) {
				if (current.firstChild != null) {
					result.append("]::text[]");
					followUp.append("]::text[]");
					if (current.prev.nextAnd) {
						result.append(" AND (");
						followUp.append(" AND (");
					} else {
						result.append(" OR (");
						followUp.append(" OR (");
					}
					childPresentFlag = true;
					dollar = current.firstChild.toSql(result, followUp, tuple, dollar);
					result.append(")");
					followUp.append(")");
					break;
				}
				current = current.getNext();
				if (current.type != null && !current.type.isEmpty()) {
					if (current.prev.nextAnd != current.nextAnd && current.next != null) {
						result.append("]");
						followUp.append("]");
						if (current.prev.nextAnd) {
							result.append(" AND ");
							followUp.append(" AND ");
						} else {
							result.append(" OR ");
							followUp.append(" OR ");
						}
						result.append("e_types ");
						followUp.append("e_types ");
						if (current.nextAnd) {
							result.append("@> ");
							followUp.append("@> ");
						} else {
							result.append("&& ");
							followUp.append("&& ");
						}
						result.append("ARRAY[$");
						followUp.append("ARRAY[' || $");
					} else {
						result.append(",$");
						followUp.append(",' || $");
					}
					result.append(dollar);
					followUp.append(dollar);
					followUp.append(" || '");
					dollar++;
					tuple.addString(current.type);
				}
			}

			if (childPresentFlag) {
				result.append(") ");
				followUp.append(") ");
			} else {
				result.append("]::text[]) ");
				followUp.append("]::text[]) ");
			}
		}
		return dollar;
	}

	public TypeQueryTerm getDuplicateAndRemoveNotKnownTypes(String[] entityTypes) {
		Set<String> lookup = Sets.newHashSet(entityTypes);
		return getDuplicateAndRemoveNotKnownTypes(lookup);
	}

	public TypeQueryTerm getDuplicateAndRemoveNotKnownTypes(Set<String> lookup) {
		TypeQueryTerm result = new TypeQueryTerm(context);
		if (this.type == null || lookup.contains(this.type)) {
			result.type = this.type;
			result.context = this.context;
			result.nextAnd = this.nextAnd;
			if (this.firstChild != null) {
				result.firstChild = firstChild.getDuplicateAndRemoveNotKnownTypes(lookup);
			}
			if (this.hasNext()) {
				result.next = next.getDuplicateAndRemoveNotKnownTypes(lookup);
			}
			return result;
		} else {
			if (hasNext()) {
				return next.getDuplicateAndRemoveNotKnownTypes(lookup);
			} else {
				return null;
			}
		}
	}

	public String getTypeQuery() {
		StringBuilder result = new StringBuilder();
		getTypeQuery(result);
		return result.toString();
	}

	public void getTypeQuery(StringBuilder result) {
		if (type != null) {
			result.append(context.compactIri(type));
		} else {
			if (firstChild != null && firstChild.hasNext()) {
				result.append('(');
				firstChild.getTypeQuery(result);
				result.append(')');
			}
		}
		if (hasNext()) {
			if (nextAnd) {
				result.append(';');
			} else {
				result.append('|');
			}
			next.getTypeQuery(result);
		}

	}

	public Set<String> getAllTypes() {
		return allTypes;
	}

	public void setAllTypes(Set<String> allTypes) {
		this.allTypes = allTypes;
	}

	public void toRequestString(StringBuilder result, Context context) {
		if (firstChild != null) {
			result.append('(');
			firstChild.toRequestString(result, context);
			result.append(')');
		} else {
			result.append(URLEncoder.encode(context.compactIri(type), StandardCharsets.UTF_8));
			if (hasNext()) {
				if (isNextAnd()) {
					result.append(';');
				} else {
					result.append('|');
				}
				next.toRequestString(result, context);
			}
		}

	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public boolean calculate(List<String> types) {
		if (firstChild != null) {
			return firstChild.calculate(types);
		} else {
			boolean result = types.contains(type);
			if (hasNext()) {
				if (isNextAnd()) {
					result = result && next.calculate(types);
				} else {
					result = result || next.calculate(types);
				}
			}
			return result;
		}
	}

}
