package eu.neclab.ngsildbroker.commons.datatypes.terms;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.github.jsonldjava.core.Context;
import com.google.common.collect.Sets;

import eu.neclab.ngsildbroker.commons.datatypes.BaseEntry;
import eu.neclab.ngsildbroker.commons.datatypes.BaseProperty;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import io.smallrye.mutiny.tuples.Tuple2;

public class TypeQueryTerm {

	private Context linkHeaders;
	private TypeQueryTerm next = null;
	private TypeQueryTerm prev = null;
	private boolean nextAnd = true;
	private TypeQueryTerm firstChild = null;
	private TypeQueryTerm parent = null;
	private String type = null;

	public TypeQueryTerm(Context context) {
		this.linkHeaders = context;

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
		this.type = linkHeaders.expandIri(type, false, true, null, null);
	}

	public boolean calculate(BaseProperty property) throws ResponseException {
		ArrayList<BaseProperty> temp = new ArrayList<BaseProperty>();
		temp.add(property);
		return false;// calculate(temp);

	}

//	/**
//	 * 
//	 * @param properties
//	 * @return
//	 * @throws ResponseException
//	 */
//	public boolean calculate(List<BaseProperty> properties) throws ResponseException {
//		boolean result = false;
//		if (firstChild == null) {
//			result = calculate(properties, attribute, operator, operant);
//		} else {
//			result = firstChild.calculate(properties);
//		}
//		if (hasNext()) {
//			if (nextAnd) {
//				result = result && next.calculate(properties);
//			} else {
//				result = result || next.calculate(properties);
//			}
//		}
//
//		return result;
//	}
//
//	@SuppressWarnings("rawtypes") // rawtypes are fine here and intentionally used
//	private boolean calculate(List<BaseProperty> properties, String attribute, String operator, String operant)
//			throws ResponseException {
//
//		if (!attribute.matches(URI) && attribute.contains(".")) {
//			String[] splittedAttrib = attribute.split("\\.");
//			ArrayList<BaseProperty> newProps = new ArrayList<BaseProperty>();
//			String expanded = expandAttributeName(splittedAttrib[0]);
//			if (expanded == null) {
//				return false;
//			}
//			List<BaseProperty> potentialMatches = getMatchingProperties(properties, expanded);
//			if (potentialMatches == null) {
//				return false;
//			}
//			for (BaseProperty potentialMatch : potentialMatches) {
//				newProps.addAll(getSubAttributes(potentialMatch));
//			}
//			newProps.addAll(potentialMatches);
//
//			String newAttrib;
//			if (splittedAttrib.length > 2) {
//				newAttrib = String.join(".", Arrays.copyOfRange(splittedAttrib, 1, splittedAttrib.length - 1));
//			} else {
//				newAttrib = splittedAttrib[1];
//			}
//			return calculate(newProps, newAttrib, operator, operant);
//		} else {
//			String[] compound = null;
//			if (attribute.contains("[")) {
//				compound = attribute.split("\\[");
//				attribute = compound[0];
//				compound = Arrays.copyOfRange(compound, 1, compound.length);
//			}
//			String myAttribName = expandAttributeName(attribute);
//			if (myAttribName == null) {
//				return false;
//			}
//			boolean finalReturnValue = false;
//			int index = NGSIConstants.SPECIAL_PROPERTIES.indexOf(myAttribName);
//			Object value;
//			List<BaseProperty> myProperties;
//			if (index == -1) {
//				myProperties = getMatchingProperties(properties, myAttribName);
//				if (myProperties == null) {
//					return false;
//				}
//			} else {
//				myProperties = properties;
//			}
//			for (BaseProperty myProperty : myProperties) {
//				Iterator<?> it = myProperty.getEntries().values().iterator();
//				while (it.hasNext()) {
//					BaseEntry next = (BaseEntry) it.next();
//					switch (index) {
//						case 0:
//							// NGSI_LD_CREATED_AT
//							value = next.getCreatedAt();
//							break;
//						case 1:
//							// NGSI_LD_OBSERVED_AT
//							value = next.getObservedAt();
//							break;
//						case 2:
//							// NGSI_LD_MODIFIED_AT
//							value = next.getModifiedAt();
//							break;
//						case 3:
//							// NGSI_LD_DATA_SET_ID
//							value = next.getCreatedAt();
//						case 4:
//							// NGSI_LD_UNIT_CODE
//							if (next instanceof PropertyEntry) {
//								value = ((PropertyEntry) next).getUnitCode();
//							}
//						default:
//
//							value = getValue(next);
//							if (compound != null) {
//								value = getCompoundValue(value, compound);
//							}
//							break;
//					}
//					if (value == null) {
//						break;
//					}
//					operant = operant.replace("\"", "");
//					if (TIME_PROPS.contains(myAttribName)) {
//						try {
//							operant = SerializationTools.date2Long(operant).toString();
//						} catch (Exception e) {
//							throw new ResponseException(ErrorType.BadRequestData, e.getMessage());
//						}
//					}
//					if (operant.matches(RANGE)) {
//						String[] range = operant.split("\\.\\.");
//
//						switch (operator) {
//							case "==":
//
//								if (compare(range[0], value) >= 0 && compare(range[1], value) <= 0) {
//									return true;
//								}
//								break;
//							case "!=":
//								if (compare(range[0], value) <= 0 && compare(range[1], value) >= 0) {
//									return true;
//								}
//								break;
//						}
//
//						return false;
//
//					} else if (operant.matches(LIST)) {
//						List<String> listOfOperants = Arrays.asList(operant.split(","));
//						if (!(value instanceof List)) {
//							return false;
//						}
//						@SuppressWarnings("unchecked") // check above
//						List<Object> myList = (List<Object>) value;
//						switch (operator) {
//							case "!=":
//								for (String listOperant : listOfOperants) {
//									if (myList.contains(listOperant)) {
//										return false;
//									}
//								}
//								return true;
//							case "==":
//								for (String listOperant : listOfOperants) {
//									if (myList.contains(listOperant)) {
//										return true;
//									}
//								}
//								return false;
//							default:
//								return false;
//						}
//					} else {
//						switch (operator) {
//							case "==":
//								if (value instanceof List) {
//									return listContains((List) value, operant);
//								}
//								if (operant.equals(value.toString())) {
//									return true;
//								}
//								break;
//							case "!=":
//								finalReturnValue = true;
//								if (value instanceof List) {
//									return !listContains((List) value, operant);
//								}
//								if (operant.equals(value.toString())) {
//									return false;
//								}
//								break;
//							case ">=":
//
//								if (compare(operant, value) >= 0) {
//									return true;
//								}
//								break;
//							case "<=":
//								if (compare(operant, value) <= 0) {
//									return true;
//								}
//								break;
//							case ">":
//								if (compare(operant, value) > 0) {
//									return true;
//								}
//								break;
//							case "<":
//								if (compare(operant, value) < 0) {
//									return true;
//								}
//								break;
//							case "~=":
//								if (value.toString().matches(operant)) {
//									return true;
//								}
//								break;
//							case "!~=":
//								finalReturnValue = true;
//								if (value.toString().matches(operant)) {
//									return false;
//								}
//								break;
//						}
//
//					}
//
//				}
//
//			}
//			return finalReturnValue;
//
//		}
//
//	}

	private int compare(String operant, Object value) {
		try {
			if (value instanceof Integer || value instanceof Long || value instanceof Double) {
				return Double.compare(Double.parseDouble(value.toString()), Double.parseDouble(operant));
			} else {
				return value.toString().compareTo(operant);
			}
		} catch (NumberFormatException e) {
			return -1;
		}

	}

	@SuppressWarnings("rawtypes")
	private boolean listContains(List value, String operant) {
		for (Object entry : value) {
			if (entry.toString().equals(operant)) {
				return true;
			}
		}
		return false;
	}

	private Collection<? extends BaseProperty> getSubAttributes(BaseProperty potentialMatch) {
		ArrayList<BaseProperty> result = new ArrayList<BaseProperty>();
		Iterator<? extends BaseEntry> it = potentialMatch.getEntries().values().iterator();
		while (it.hasNext()) {
			BaseEntry next = it.next();
			if (next.getRelationships() != null) {
				result.addAll(next.getRelationships());
			}
			if (next.getProperties() != null) {
				result.addAll(next.getProperties());
			}
		}
		return result;
	}

	/*
	 * private List<Property> getAllNGSIBaseProperties(BaseProperty prop) {
	 * ArrayList<Property> result = new ArrayList<Property>(); try { if
	 * (prop.getCreatedAt() != -1l) { Property createdAtProp = new Property();
	 * createdAtProp.setId(new URI(NGSIConstants.NGSI_LD_CREATED_AT));
	 * createdAtProp.setSingleEntry(new PropertyEntry("createdAt",
	 * prop.getCreatedAt())); result.add(createdAtProp); } if (prop.getObservedAt()
	 * != -1l) { Property observedAtProp = new Property(); observedAtProp.setId(new
	 * URI(NGSIConstants.NGSI_LD_OBSERVED_AT)); observedAtProp.setSingleEntry(new
	 * PropertyEntry("observerAt", prop.getObservedAt()));
	 * result.add(observedAtProp); }
	 * 
	 * if (prop.getModifiedAt() != -1l) { Property modifiedAtProp = new Property();
	 * modifiedAtProp.setId(new URI(NGSIConstants.NGSI_LD_MODIFIED_AT));
	 * modifiedAtProp.setSingleEntry(new PropertyEntry("modifiedAt",
	 * prop.getModifiedAt())); result.add(modifiedAtProp); } if (prop instanceof
	 * Property) { Property realProp = (Property) prop; if (realProp.getUnitCode()
	 * != null && realProp.getUnitCode().equals("")) { Property unitCodeProp = new
	 * Property(); unitCodeProp.setId(new URI(NGSIConstants.NGSI_LD_UNIT_CODE));
	 * unitCodeProp.setSingleEntry(new PropertyEntry("unitCode",
	 * realProp.getUnitCode())); result.add(unitCodeProp); } } } catch
	 * (URISyntaxException e) { // Left Empty intentionally. Should never happen
	 * since the URI constants are // controlled } return result; }
	 */

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

	public Tuple2<Character, String> toSql(char startChar) {
		StringBuilder builder = new StringBuilder();
		StringBuilder builderFinalLine = new StringBuilder();
		StringBuilder finalTables = new StringBuilder();
		char finalChar = toSql(builder, builderFinalLine, finalTables, startChar, "etype2iid");
		if (!finalTables.isEmpty()) {
			finalChar++;
			builder.append(',');
			builder.append(finalChar);
			builder.append(" as (SELECT etype2iid.iid AS iid FROM etype2iid,");
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
		if (type == null || type.isEmpty()) {
			TypeQueryTerm current = this;
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
			result.append(".e_type FROM ");
			result.append(sqlTable);
			result.append(" WHERE ");
			result.append(sqlTable);
			result.append(".e_type='");
			result.append(type);
			result.append('\'');
			if (hasNext()) {
				result.append(" or ");
			}
			TypeQueryTerm current = this;

			while (current.hasNext() || current.firstChild != null) {
				if (current.firstChild != null) {
					resultFinalLine.append('(');
					currentChar = current.firstChild.toSql(result, resultFinalLine, finalTables, currentChar, sqlTable);
					resultFinalLine.append(')');
					break;
				}
				current = current.getNext();
				if (current.type != null && !current.type.isEmpty()) {
					result.append(sqlTable);
					result.append(".e_type='");
					result.append(current.type);
					result.append('\'');
					andCounter++;
					if ((current.getPrev() != null && current.isNextAnd() != current.getPrev().isNextAnd())
							|| current.firstChild != null) {
						if (current.getPrev() != null && current.getPrev().isNextAnd()) {
							result.append(" GROUP BY ");
							result.append(sqlTable);
							result.append(".iid, ");
							result.append(sqlTable);
							result.append(".e_type HAVING COUNT(");
							result.append(sqlTable);
							result.append(".e_type)=");
							result.append(andCounter);
						}
						if (current.nextAnd) {
							sqlTable = currentChar + "";
						} else {
							resultFinalLine.append("etype2iid.iid=");
							resultFinalLine.append(currentChar);
							resultFinalLine.append(".iid");
							resultFinalLine.append(" or ");
							finalTables.append(currentChar);
							finalTables.append(',');
							sqlTable = "etype2iid";
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
							result.append(".e_type FROM ");
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
				result.append(".e_type HAVING COUNT(");
				result.append(sqlTable);
				result.append(".e_type)=");
				result.append(andCounter);
			}
			if (current.type != null) {
				resultFinalLine.append("etype2iid.iid=");
				resultFinalLine.append(currentChar);
				resultFinalLine.append(".iid");
				finalTables.append(currentChar);
				result.append(')');
			}
			return currentChar;

		}
	}

	public TypeQueryTerm getDuplicateAndRemoveNotKnownTypes(String[] entityTypes) {
		Set<String> lookup = Sets.newHashSet(entityTypes);
		return getDuplicateAndRemoveNotKnownTypes(lookup);
	}

	public TypeQueryTerm getDuplicateAndRemoveNotKnownTypes(Set<String> lookup) {
		TypeQueryTerm result = new TypeQueryTerm(linkHeaders);
		if (this.type == null || lookup.contains(this.type)) {
			result.type = this.type;
			result.linkHeaders = this.linkHeaders;
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
			result.append(linkHeaders.compactIri(type));
		} else {
			if (firstChild != null && firstChild.hasNext()) {
				result.append('(');
				firstChild.next.getTypeQuery(result);
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
}
