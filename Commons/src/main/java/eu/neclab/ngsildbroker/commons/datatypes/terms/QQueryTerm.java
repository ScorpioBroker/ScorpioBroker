package eu.neclab.ngsildbroker.commons.datatypes.terms;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.github.jsonldjava.core.Context;
import com.google.common.collect.Sets;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.BaseEntry;
import eu.neclab.ngsildbroker.commons.datatypes.BaseProperty;
import eu.neclab.ngsildbroker.commons.datatypes.PropertyEntry;
import eu.neclab.ngsildbroker.commons.datatypes.RelationshipEntry;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;
import io.vertx.mutiny.sqlclient.Tuple;

public class QQueryTerm implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7136298831314992504L;
	private static final String RANGE = ".+\\.\\..+";
	private static final String LIST = ".+(,.+)+";
	private static final String URI = "\\w+:(\\/?\\/?)[^\\s^;]+";
	private static final String DATETIME = "\\d\\d\\d\\d-\\d\\d-\\d\\dT\\d\\d:\\d\\d:\\d\\d([,.]\\d{1,6})?Z";
	private static final String DATE = "\\d\\d\\d\\d-\\d\\d-\\d\\d";
	private static final String TIME = "\\d\\d:\\d\\d:\\d\\d(,\\d\\d\\d\\d\\d\\d)?Z";
	private static final List<String> TIME_PROPS = Arrays.asList(NGSIConstants.NGSI_LD_OBSERVED_AT,
			NGSIConstants.NGSI_LD_CREATED_AT, NGSIConstants.NGSI_LD_MODIFIED_AT);
	private Context linkHeaders;
	private QQueryTerm next = null;
	private boolean nextAnd = true;
	private QQueryTerm firstChild = null;
	private QQueryTerm parent = null;
	private String attribute = "";
	private String operator = "";
	private String operant = "";
	private String expandedOpt = "";

	public String getExpandedOpt() {
		return expandedOpt;
	}

	public void setExpandedOpt(String expandedOpt) {
		this.expandedOpt = expandedOpt;
	}

	private Set<String> allAttribs = Sets.newHashSet();

	QQueryTerm() {
		// for serialization
	}

	public QQueryTerm(Context context) {
		this.linkHeaders = context;

	}

	public boolean hasNext() {
		return next != null;
	}

	public boolean calculate(BaseProperty property) throws ResponseException {
		ArrayList<BaseProperty> temp = new ArrayList<BaseProperty>();
		temp.add(property);
		return calculate(temp);

	}

	/**
	 * 
	 * @param properties
	 * @return
	 * @throws ResponseException
	 */
	public boolean calculate(List<BaseProperty> properties) {
		boolean result = false;
		if (firstChild == null) {
			result = calculate(properties, attribute, operator, operant);
		} else {
			result = firstChild.calculate(properties);
		}
		if (hasNext()) {
			if (nextAnd) {
				result = result && next.calculate(properties);
			} else {
				result = result || next.calculate(properties);
			}
		}

		return result;
	}

	@SuppressWarnings("rawtypes") // rawtypes are fine here and intentionally used
	private boolean calculate(List<BaseProperty> properties, String attribute, String operator, String operant) {

		if (!attribute.matches(URI) && attribute.contains(".")) {
			String[] splittedAttrib = attribute.split("\\.");
			ArrayList<BaseProperty> newProps = new ArrayList<BaseProperty>();
			String expanded = expandAttributeName(splittedAttrib[0]);
			if (expanded == null) {
				return false;
			}
			List<BaseProperty> potentialMatches = getMatchingProperties(properties, expanded);
			if (potentialMatches == null) {
				return false;
			}
			for (BaseProperty potentialMatch : potentialMatches) {
				newProps.addAll(getSubAttributes(potentialMatch));
			}
			newProps.addAll(potentialMatches);

			String newAttrib;
			if (splittedAttrib.length > 2) {
				newAttrib = String.join(".", Arrays.copyOfRange(splittedAttrib, 1, splittedAttrib.length - 1));
			} else {
				newAttrib = splittedAttrib[1];
			}
			return calculate(newProps, newAttrib, operator, operant);
		} else {
			String[] compound = null;
			if (attribute.contains("[")) {
				compound = attribute.split("\\[");
				attribute = compound[0];
				compound = Arrays.copyOfRange(compound, 1, compound.length);
			}
			String myAttribName = expandAttributeName(attribute);
			if (myAttribName == null) {
				return false;
			}
			boolean finalReturnValue = false;
			int index = NGSIConstants.SPECIAL_PROPERTIES.indexOf(myAttribName);
			Object value;
			List<BaseProperty> myProperties;
			if (index == -1) {
				myProperties = getMatchingProperties(properties, myAttribName);
				if (myProperties == null) {
					return false;
				}
			} else {
				myProperties = properties;
			}
			if ((operant == null || operant.isEmpty()) && myProperties != null && !myProperties.isEmpty()) {
				return true;
			}
			for (BaseProperty myProperty : myProperties) {
				Iterator<?> it = myProperty.getEntries().values().iterator();
				while (it.hasNext()) {
					BaseEntry next = (BaseEntry) it.next();
					switch (index) {
					case 0:
						// NGSI_LD_CREATED_AT
						value = next.getCreatedAt();
						break;
					case 1:
						// NGSI_LD_OBSERVED_AT
						value = next.getObservedAt();
						break;
					case 2:
						// NGSI_LD_MODIFIED_AT
						value = next.getModifiedAt();
						break;
					case 3:
						// NGSI_LD_DATA_SET_ID
						value = next.getCreatedAt();
					case 4:
						// NGSI_LD_UNIT_CODE
						if (next instanceof PropertyEntry) {
							value = ((PropertyEntry) next).getUnitCode();
						}
					default:

						value = getValue(next);
						if (compound != null) {
							value = getCompoundValue(value, compound);
						}
						break;
					}
					if (value == null) {
						break;
					}
					operant = operant.replace("\"", "");
					if (TIME_PROPS.contains(myAttribName)) {
						try {
							operant = SerializationTools.date2Long(operant).toString();
						} catch (Exception e) {
							return false;
						}
					}
					if (operant.matches(RANGE)) {
						String[] range = operant.split("\\.\\.");

						switch (operator) {
						case "==":

							if (compare(range[0], value, operator) >= 0 && compare(range[1], value, operator) <= 0) {
								return true;
							}
							break;
						case "!=":
							if (compare(range[0], value, operator) <= 0 && compare(range[1], value, operator) >= 0) {
								return true;
							}
							break;
						}

						return false;

					} else if (operant.matches(LIST)) {
						List<String> listOfOperants = Arrays.asList(operant.split(","));
						if (!(value instanceof List)) {
							return false;
						}
						@SuppressWarnings("unchecked") // check above
						List<Object> myList = (List<Object>) value;
						switch (operator) {
						case "!=":
							for (String listOperant : listOfOperants) {
								if (myList.contains(listOperant)) {
									return false;
								}
							}
							return true;
						case "==":
							for (String listOperant : listOfOperants) {
								if (myList.contains(listOperant)) {
									return true;
								}
							}
							return false;
						default:
							return false;
						}
					} else {
						switch (operator) {
						case "==":
							if (value instanceof List) {
								return listContains((List) value, operant);
							}
							if (operant.equals(value.toString())) {
								return true;
							}
							break;
						case "!=":
							finalReturnValue = true;
							if (value instanceof List) {
								return !listContains((List) value, operant);
							}
							if (operant.equals(value.toString())) {
								return false;
							}
							break;
						case ">=":

							if (compare(operant, value, operator) >= 0) {
								return true;
							}
							break;
						case "<=":
							if (compare(operant, value, operator) <= 0) {
								return true;
							}
							break;
						case ">":
							if (compare(operant, value, operator) > 0) {
								return true;
							}
							break;
						case "<":
							if (compare(operant, value, operator) < 0) {
								return true;
							}
							break;
						case "~=":
							if (value.toString().matches(operant)) {
								return true;
							}
							break;
						case "!~=":
							finalReturnValue = true;
							if (value.toString().matches(operant)) {
								return false;
							}
							break;
						}

					}

				}

			}
			return finalReturnValue;

		}

	}

	private int compare(String operant, Object value, String operator) {
		if (value instanceof List l1) {
			int r = -1;
			for (Object obj : l1) {
				r = compare(operant, obj, operator);
				boolean found = false;
				switch (operator) {
				case "==":
					if (r == 0) {
						found = true;
					}
					break;
				case "!=":
					if (r != 0) {
						found = true;
					}
					break;
				case ">=":
					if (r >= 0) {
						found = true;
					}
					break;
				case "<=":
					if (r <= 0) {
						found = true;
					}
					break;
				case ">":
					if (r > 0) {
						found = true;
					}
					break;
				case "<":
					if (r < 0) {
						found = true;
					}
					break;
				case "~=":
					if (obj.toString().matches(operant)) {
						found = true;
						r = 0;
					}
					break;
				case "!~=":
					if (!obj.toString().matches(operant)) {
						found = true;
						r = -1;
					}
					break;
				}
				if (found) {
					return r;
				}
			}
			return r;

		} else {
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

	@SuppressWarnings("rawtypes") // Intentional usage of raw type here.
	private Object getCompoundValue(Object value, String[] compound) {
		if (!(value instanceof Map)) {
			return null;
		}

		Map complexValue = (Map) value;
		String firstElement = expandAttributeName(compound[0].replaceAll("\\]", "").replaceAll("\\[", ""));
		Object potentialResult = complexValue.get(firstElement);
		if (potentialResult == null) {
			return null;
		}
		if (potentialResult instanceof List) {
			potentialResult = ((List) potentialResult).get(0);
		}
		if (compound.length == 1) {
			return potentialResult;
		}

		return getCompoundValue(potentialResult, Arrays.copyOfRange(compound, 1, compound.length));
	}

	@SuppressWarnings("rawtypes") // Intentional usage of raw type here.
	private Object getValue(BaseEntry myEntry) {
		Object value = null;
		if (myEntry instanceof PropertyEntry) {
			value = ((PropertyEntry) myEntry).getValue();
//			if (value instanceof List) {
//				value = ((List) value).get(0);
//			}
		} else if (myEntry instanceof RelationshipEntry) {
			value = ((RelationshipEntry) myEntry).getObject();
		}
		return value;
	}

	private List<BaseProperty> getMatchingProperties(List<BaseProperty> properties, String myAttribName) {
		ArrayList<BaseProperty> result = new ArrayList<BaseProperty>();
		if (properties == null || properties.isEmpty()) {
			return null;
		}

		for (BaseProperty property : properties) {
			if (property.getId().toString().equals(myAttribName)) {
				result.add(property);
			}
		}
		if (result.isEmpty()) {
			return null;
		}
		return result;
	}

	private String expandAttributeName(String attribute) {
		return linkHeaders.expandIri(attribute, false, true, null, null);
	}

	public QQueryTerm getNext() {
		return next;
	}

	public void setNext(QQueryTerm next) {
		this.next = next;
		this.next.setParent(this.getParent());
	}

	public boolean isNextAnd() {
		return nextAnd;
	}

	public void setNextAnd(boolean nextAnd) {
		this.nextAnd = nextAnd;
	}

	public QQueryTerm getFirstChild() {
		return firstChild;
	}

	public void setFirstChild(QQueryTerm firstChild) {
		this.firstChild = firstChild;
		this.firstChild.setParent(this);
	}

	public String getAttribute() {
		return attribute;
	}

	public void setAttribute(String attribute) {
		this.attribute = attribute.strip();
	}

	public String getOperator() {
		return operator;
	}

	public void setOperator(String operator) {
		this.operator = operator;
	}

	public String getOperant() {
		return operant;
	}

	public void setOperant(String operant) throws ResponseException {
		operant = operant.strip();

		if (operant.matches(URI) && !operant.matches(TIME)) { // uri and time patterns are ambiguous in the abnf grammar
			this.operant = "\"" + operant + "\"";
		} else if (operant.startsWith("'") && operant.endsWith("'")) {
			this.operant = "\"" + operant.substring(1, operant.length() - 1) + "\"";
		} else {
			this.operant = operant;
		}
		switch (operator) {
		case NGSIConstants.QUERY_GREATEREQ:
			if (operant.matches(LIST) || operant.matches(RANGE)) {
				throw new ResponseException(ErrorType.BadRequestData, "invalid operant for greater equal");
			}
			break;
		case NGSIConstants.QUERY_LESSEQ:
			if (operant.matches(LIST) || operant.matches(RANGE)) {
				throw new ResponseException(ErrorType.BadRequestData, "invalid operant for less equal");
			}
			break;
		case NGSIConstants.QUERY_GREATER:
			if (operant.matches(LIST) || operant.matches(RANGE)) {
				throw new ResponseException(ErrorType.BadRequestData, "invalid operant for greater");
			}
			break;
		case NGSIConstants.QUERY_LESS:
			if (operant.matches(LIST) || operant.matches(RANGE)) {
				throw new ResponseException(ErrorType.BadRequestData, "invalid operant for less");
			}
			break;
		case NGSIConstants.QUERY_PATTERNOP:
			if (operant.matches(LIST) || operant.matches(RANGE)) {
				throw new ResponseException(ErrorType.BadRequestData, "invalid operant for pattern operation");
			}
			break;
		case NGSIConstants.QUERY_NOTPATTERNOP:
			if (operant.matches(LIST) || operant.matches(RANGE)) {
				throw new ResponseException(ErrorType.BadRequestData, "invalid operant for not pattern operation");
			}
			break;
		}

	}

	public QQueryTerm getParent() {
		return parent;
	}

	public void setParent(QQueryTerm parent) {
		this.parent = parent;
	}

	@Override
	public String toString() {
		return "QueryTerm [next=" + next + ", nextAnd=" + nextAnd + ", firstChild=" + firstChild + ", attribute="
				+ attribute + ", operator=" + operator + ", operant=" + operant + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((attribute == null) ? 0 : attribute.hashCode());
		result = prime * result + ((firstChild == null) ? 0 : firstChild.hashCode());
		result = prime * result + ((next == null) ? 0 : next.hashCode());
		result = prime * result + (nextAnd ? 1231 : 1237);
		result = prime * result + ((operant == null) ? 0 : operant.hashCode());
		result = prime * result + ((operator == null) ? 0 : operator.hashCode());
		result = prime * result + ((parent == null) ? 0 : parent.hashCode());
		return result;
	}

	public boolean equals(Object obj, boolean ignoreKids) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		QQueryTerm other = (QQueryTerm) obj;
		if (attribute == null) {
			if (other.attribute != null)
				return false;
		} else if (!attribute.equals(other.attribute))
			return false;
		if (!ignoreKids) {
			if (firstChild == null) {
				if (other.firstChild != null)
					return false;
			} else if (!firstChild.equals(other.firstChild))
				return false;
		}
		if (next == null) {
			if (other.next != null)
				return false;
		} else if (!next.equals(other.next))
			return false;
		if (nextAnd != other.nextAnd)
			return false;
		if (operant == null) {
			if (other.operant != null)
				return false;
		} else if (!operant.equals(other.operant))
			return false;
		if (operator == null) {
			if (other.operator != null)
				return false;
		} else if (!operator.equals(other.operator))
			return false;
		if (parent == null) {
			if (other.parent != null)
				return false;
		} else if (!parent.equals(other.parent, true))
			return false;
		return true;
	}

	@Override
	public boolean equals(Object obj) {
		return equals(obj, false);
	}

	private int getAttribQuery(StringBuilder result, int dollarCount, Tuple tuple) {
		result.append("ENTITY ? $");
		result.append(dollarCount);

		String[] splitted = getAttribute().split("\\[");
		if (splitted.length > 1) {
			splitted[1] = splitted[1].substring(0, splitted[1].length() - 1);
		}
		String[] subAttribPath = splitted.length == 1 ? null : splitted[1].split("\\.");
		String[] attribPath = splitted[0].split("\\.");
		String attribName = linkHeaders.expandIri(attribPath[0], false, true, null, null);
		if (attribName.equals("@id")) {
			result.append(" AND entity ->> $");
			result.append(dollarCount);
			dollarCount++;
			tuple.addString(attribName);
			result.append(" ~ $");
			result.append(dollarCount);
			dollarCount++;
			tuple.addString(operant);
		} else {
			result.append(" AND EXISTS (SELECT TRUE FROM JSONB_ARRAY_ELEMENTS(ENTITY -> $");
			result.append(dollarCount);
			dollarCount++;
			tuple.addString(attribName);
			result.append(") AS toplevel ");
			if ((operator != null && !operator.isEmpty()) || attribPath.length > 1
					|| (subAttribPath != null && subAttribPath.length > 0)) {
				result.append("WHERE ");
				dollarCount = commonWherePart(attribPath, subAttribPath, "toplevel", dollarCount, tuple, result, this);
			} else {
				result.append(')');
			}
			// result.append(")");
		}
		return dollarCount;
	}

	public int toSql(StringBuilder result, int dollarCount, Tuple tuple) {
		if (firstChild != null) {
			result.append("(");
			dollarCount = firstChild.toSql(result, dollarCount, tuple);
			result.append(")");
		} else {
			dollarCount = getAttribQuery(result, dollarCount, tuple);
		}
		if (hasNext()) {
			if (nextAnd) {
				result.append(" and ");
			} else {
				result.append(" or ");
			}
			dollarCount = next.toSql(result, dollarCount, tuple);
		}
		return dollarCount;
	}

	private ArrayList<String> getAttribPathArray(String attribute) {
		ArrayList<String> attribPath = new ArrayList<String>();
		if (attribute.contains("[") && attribute.contains(".")) {
			if (attribute.contains(".")) {
				for (String subPart : attribute.split("\\.")) {
					if (subPart.contains("[")) {
						for (String subParts : subPart.split("\\[")) {
							// subParts = subParts.replaceAll("\\]", "");
							attribPath.add(expandAttributeName(subParts));
						}
					} else {
						attribPath.add(expandAttributeName(subPart));
					}
				}
			}
		} else if (attribute.contains("[")) {
			for (String subPart : attribute.split("\\[")) {
				subPart = subPart.replaceAll("\\]", "");
				attribPath.addAll(getAttribPathArray(subPart));
			}
		} else if (attribute.matches(URI)) {
			attribPath.add(expandAttributeName(attribute));
		} else if (attribute.contains(".")) {
			for (String subPart : attribute.split("\\.")) {
				attribPath.addAll(getAttribPathArray(subPart));
			}
		} else {
			attribPath.add(expandAttributeName(attribute));
		}
		return attribPath;
	}

	private int applyOperator(StringBuilder attributeFilterProperty, int dollarCount, Tuple tuple,
			Boolean needExpanded) {
		String finalOperant;
		if (needExpanded) {
			finalOperant = expandedOpt;
		} else {
			finalOperant = operant;
		}
		String typecast = "jsonb";
//		if (operant.matches(DATETIME)) {
//			typecast = "timestamp";
//		} else if (operant.matches(DATE)) {
//			typecast = "date";
//		} else if (operant.matches(TIME)) {
//			typecast = "time";
//		}
		switch (operator) {
		case NGSIConstants.QUERY_UNEQUAL:
		case NGSIConstants.QUERY_EQUAL:
			if (finalOperant.matches(LIST)) {
				if (operator.equals(NGSIConstants.QUERY_UNEQUAL)) {
					attributeFilterProperty.append(" not");
				}
				attributeFilterProperty.append(" in (");
				for (String listItem : finalOperant.split(",")) {
					attributeFilterProperty.append("$");
					attributeFilterProperty.append(dollarCount);
					dollarCount++;
					addItemToTupel(tuple, listItem, attributeFilterProperty);
					attributeFilterProperty.append("::");
					attributeFilterProperty.append(typecast);
					attributeFilterProperty.append(',');

				}
				attributeFilterProperty.setCharAt(attributeFilterProperty.length() - 1, ')');
			} else if (finalOperant.matches(RANGE)) {
				String[] myRange = finalOperant.split("\\.\\.");
				if (operator.equals(NGSIConstants.QUERY_UNEQUAL)) {
					attributeFilterProperty.append(" not");
				}
				attributeFilterProperty.append(" between $");
				attributeFilterProperty.append(dollarCount);
				addItemToTupel(tuple, myRange[0], attributeFilterProperty);
				attributeFilterProperty.append("::");
				attributeFilterProperty.append(typecast);
				attributeFilterProperty.append(" and $");
				attributeFilterProperty.append(dollarCount + 1);
				addItemToTupel(tuple, myRange[1], attributeFilterProperty);
				attributeFilterProperty.append("::" + typecast);
				dollarCount += 2;

			} else {
				if (operator.equals(NGSIConstants.QUERY_UNEQUAL)) {
					attributeFilterProperty.append(" != $");
				} else {
					attributeFilterProperty.append(" = $");
				}
				attributeFilterProperty.append(dollarCount);
				dollarCount++;
				addItemToTupel(tuple, finalOperant, attributeFilterProperty);
				attributeFilterProperty.append("::" + typecast);

			}

			break;
		case NGSIConstants.QUERY_GREATEREQ:
			attributeFilterProperty.append(" >= $");
			attributeFilterProperty.append(dollarCount);
			dollarCount++;
			addItemToTupel(tuple, finalOperant, attributeFilterProperty);
			attributeFilterProperty.append("::");
			attributeFilterProperty.append(typecast);

			break;
		case NGSIConstants.QUERY_LESSEQ:
			attributeFilterProperty.append(" <= $");
			attributeFilterProperty.append(dollarCount);
			dollarCount++;
			addItemToTupel(tuple, finalOperant, attributeFilterProperty);
			attributeFilterProperty.append("::");
			attributeFilterProperty.append(typecast);

			break;
		case NGSIConstants.QUERY_GREATER:
			attributeFilterProperty.append(" > $");
			attributeFilterProperty.append(dollarCount);
			dollarCount++;
			addItemToTupel(tuple, finalOperant, attributeFilterProperty);
			attributeFilterProperty.append("::");
			attributeFilterProperty.append(typecast);
			break;
		case NGSIConstants.QUERY_LESS:
			attributeFilterProperty.append(" < $");
			attributeFilterProperty.append(dollarCount);
			dollarCount++;
			addItemToTupel(tuple, finalOperant, attributeFilterProperty);
			attributeFilterProperty.append("::");
			attributeFilterProperty.append(typecast);
			break;
		case NGSIConstants.QUERY_PATTERNOP:
			attributeFilterProperty.append("::text ~ $");
			attributeFilterProperty.append(dollarCount);
			// attributeFilterProperty.append("'");
			dollarCount++;
			tuple.addString(finalOperant);
			// addItemToTupel(tuple, operant);
			break;
		case NGSIConstants.QUERY_NOTPATTERNOP:
			attributeFilterProperty.append("::text !~ $");
			attributeFilterProperty.append(dollarCount);
			// attributeFilterProperty.append("'");
			dollarCount++;
			tuple.addString(finalOperant);
			// addItemToTupel(tuple, operant);
			break;
		}
		return dollarCount;
	}

	private void addItemToTupel(Tuple tuple, String listItem, StringBuilder sql) {
		try {
			double tmp = Double.parseDouble(listItem);
			sql.insert(sql.lastIndexOf("$"),"TO_JSONB(");
			sql.append(")");
			tuple.addDouble(tmp);
		} catch (NumberFormatException e) {
			if (listItem.equalsIgnoreCase("true") || listItem.equalsIgnoreCase("false")) {
				tuple.addBoolean(Boolean.parseBoolean(listItem));
			} else {
				if (!listItem.matches(DATETIME)) {
					if (listItem.charAt(0) != '"' || listItem.charAt(listItem.length() - 1) != '"') {
						listItem = '"' + listItem + '"';
					}
					sql.append("::text");
				}

				tuple.addString(listItem);
			}
		}

	}

	public Set<String> getAllAttibs() {
		return allAttribs;

	}

	public void addAttrib(String attrib) {
		allAttribs.add(linkHeaders.expandIri(attrib, false, true, null, null));
	}

	private int temporalSqlWherePart(StringBuilder sql, int dollarCount, Tuple tuple, QQueryTerm current,
			TemporalQueryTerm tempQuery) {
//		sql.append("(TEAI.ATTRIBUTEID=$");
//		sql.append(dollarCount);
//		dollarCount++;
		String[] splitted = current.getAttribute().split("\\[");
		if (splitted.length > 1) {
			splitted[1] = splitted[1].substring(0, splitted[1].length() - 1);
		}
		String[] subAttribPath = splitted.length == 1 ? null : splitted[1].split("\\.");
		String[] attribPath = splitted[0].split("\\.");
//		String attribName = linkHeaders.expandIri(attribPath[0], false, true, null, null);
//		tuple.addString(attribName);
//		if (tempQuery != null) {
//			sql.append(" AND TEAI.");
//			dollarCount = tempQuery.toSql(sql, tuple, dollarCount);
//		}
		String currentSqlAttrib = "TEAI.data";
//		if (!current.getOperator().isEmpty() || attribPath.length > 1 || subAttribPath != null) {
//			sql.append(" AND ");
//		}

		return commonWherePart(attribPath, subAttribPath, currentSqlAttrib, dollarCount, tuple, sql, current);
	}

	private int commonWherePart(String[] attribPath, String[] subAttribPath, String currentSqlAttrib, int dollarCount,
			Tuple tuple, StringBuilder sql, QQueryTerm current) {
		char currentChar = 'a';
		String prefix = "dataarray";

		for (int i = 1; i < attribPath.length; i++) {
			sql.append("EXISTS(SELECT TRUE FROM JSONB_ARRAY_ELEMENTS(");
			sql.append(currentSqlAttrib);
			sql.append(" -> $");
			sql.append(dollarCount);
			tuple.addString(linkHeaders.expandIri(attribPath[i], false, true, null, null));
			dollarCount++;
			currentSqlAttrib = prefix + currentChar;
			currentChar++;
			sql.append(") AS ");
			sql.append(currentSqlAttrib);
			sql.append(" WHERE ");
		}

		if (subAttribPath == null) {

			if (!current.getOperator().isEmpty()) {
				sql.append(" CASE WHEN (");
				sql.append(currentSqlAttrib);
				sql.append(" #>'{");
				sql.append(NGSIConstants.JSON_LD_ID);
				sql.append("}') ");
				dollarCount = applyOperator(sql, dollarCount, tuple, false);
				sql.append(" THEN true");

				sql.append(" WHEN ");
				sql.append(currentSqlAttrib);
				sql.append(" #>>'{");
				sql.append(NGSIConstants.JSON_LD_TYPE);
				sql.append(",0}' = '");
				sql.append(NGSIConstants.NGSI_LD_PROPERTY);
				sql.append("' THEN EXISTS (SELECT TRUE FROM JSONB_ARRAY_ELEMENTS(");
				sql.append(currentSqlAttrib);
				sql.append(" ->'");
				sql.append(NGSIConstants.NGSI_LD_HAS_VALUE);
				sql.append("') AS mostInnerValue WHERE (mostInnerValue->'");
				sql.append(NGSIConstants.JSON_LD_VALUE);
				sql.append("')");
				dollarCount = applyOperator(sql, dollarCount, tuple, false);
				sql.append(") WHEN ");

				sql.append(currentSqlAttrib);
				sql.append(" #>>'{");
				sql.append(NGSIConstants.JSON_LD_TYPE);
				sql.append(",0}' = '");
				sql.append(NGSIConstants.NGSI_LD_RELATIONSHIP);
				sql.append("' THEN EXISTS (SELECT TRUE FROM JSONB_ARRAY_ELEMENTS(");
				sql.append(currentSqlAttrib);
				sql.append(" ->'");
				sql.append(NGSIConstants.NGSI_LD_HAS_OBJECT);
				sql.append("') AS mostInnerValue WHERE (mostInnerValue->'");
				sql.append(NGSIConstants.JSON_LD_ID);
				sql.append("')");
				dollarCount = applyOperator(sql, dollarCount, tuple, false);
				sql.append(") WHEN ");

				sql.append(currentSqlAttrib);
				sql.append(" #>>'{");
				sql.append(NGSIConstants.JSON_LD_TYPE);
				sql.append(",0}' = '");
				sql.append(NGSIConstants.NGSI_LD_VocabProperty);
				sql.append("' THEN EXISTS (SELECT TRUE FROM JSONB_ARRAY_ELEMENTS(");
				sql.append(currentSqlAttrib);
				sql.append(" ->'");
				sql.append(NGSIConstants.NGSI_LD_HAS_VOCAB);
				sql.append("') AS mostInnerValue WHERE (mostInnerValue->'");
				sql.append(NGSIConstants.JSON_LD_ID);
				sql.append("')");
				dollarCount = applyOperator(sql, dollarCount, tuple, true);
				sql.append(") WHEN ");

				sql.append(currentSqlAttrib);
				sql.append(" #>>'{");
				sql.append(NGSIConstants.JSON_LD_TYPE);
				sql.append(",0}' = '");
				sql.append(NGSIConstants.NGSI_LD_ListProperty);
				sql.append("' THEN EXISTS (SELECT TRUE FROM JSONB_ARRAY_ELEMENTS(");
				sql.append(currentSqlAttrib);
				sql.append(" ->'");
				sql.append(NGSIConstants.NGSI_LD_HAS_LIST);
				sql.append("') AS mostInnerValue WHERE (mostInnerValue->'");
				sql.append(NGSIConstants.JSON_LD_ID);
				sql.append("')");
				dollarCount = applyOperator(sql, dollarCount, tuple, true);
				sql.append(") WHEN ");

				sql.append(currentSqlAttrib);
				sql.append(" #>>'{");
				sql.append(NGSIConstants.JSON_LD_TYPE);
				sql.append(",0}' = '");
				sql.append(NGSIConstants.NGSI_LD_LISTRELATIONSHIP);
				sql.append("' THEN EXISTS (SELECT TRUE FROM JSONB_ARRAY_ELEMENTS(");
				sql.append(currentSqlAttrib);
				sql.append(" ->'");
				sql.append(NGSIConstants.NGSI_LD_HAS_OBJECT_LIST);
				sql.append("') AS mostInnerValue WHERE (mostInnerValue->'");
				sql.append(NGSIConstants.JSON_LD_ID);
				sql.append("')");
				dollarCount = applyOperator(sql, dollarCount, tuple, false);
				sql.append(") WHEN ");

				sql.append(currentSqlAttrib);
				sql.append(" #>>'{");
				sql.append(NGSIConstants.JSON_LD_TYPE);
				sql.append("}' = '");
				sql.append(NGSIConstants.NGSI_LD_DATE_TIME);
				sql.append("' THEN (");
				sql.append(currentSqlAttrib);
				sql.append(" ->'");
				sql.append(NGSIConstants.JSON_LD_VALUE);
				sql.append("')");
				dollarCount = applyOperator(sql, dollarCount, tuple, false);
				sql.append(" WHEN ");
				sql.append(currentSqlAttrib);
				sql.append(" #>>'{");
				sql.append(NGSIConstants.JSON_LD_TYPE);
				sql.append(",0}' = '");
				sql.append(NGSIConstants.NGSI_LD_DATE_TIME);
				sql.append("' THEN (");
				sql.append(currentSqlAttrib);
				sql.append(" ->'");
				sql.append(NGSIConstants.JSON_LD_VALUE);
				sql.append("')");
				dollarCount = applyOperator(sql, dollarCount, tuple, false);

				sql.append(" ELSE FALSE END ");

			} else {
				sql.setLength(sql.length() - " WHERE ".length());
			}
		} else {
			sql.append(" CASE WHEN ");
			sql.append(currentSqlAttrib);
			sql.append(" #>>'{");
			sql.append(NGSIConstants.JSON_LD_TYPE);
			sql.append(",0}' = '");
			sql.append(NGSIConstants.NGSI_LD_PROPERTY);
			sql.append("' THEN EXISTS (SELECT TRUE FROM JSONB_ARRAY_ELEMENTS(");
			sql.append(currentSqlAttrib);
			sql.append(" ->'");
			sql.append(NGSIConstants.NGSI_LD_HAS_VALUE);
			sql.append("') AS mostInnerValue");
			String currentSqlAttrib2 = "mostInnerValue";
			prefix = "mostInnerValue";
			currentChar = 'a';
			for (int i = 0; i < subAttribPath.length; i++) {
				sql.append(" WHERE EXISTS(SELECT TRUE FROM JSONB_ARRAY_ELEMENTS(");
				sql.append(currentSqlAttrib2);
				sql.append(" -> $");
				sql.append(dollarCount);
				tuple.addString(linkHeaders.expandIri(subAttribPath[i], false, true, null, null));
				dollarCount++;
				currentSqlAttrib2 = prefix + currentChar;
				currentChar++;
				sql.append(") AS " + currentSqlAttrib2);
			}
//			sql.append("EXISTS (SELECT TRUE FROM JSONB_ARRAY_ELEMENTS(");
//			sql.append(currentSqlAttrib2);
//			sql.append(") AS ");
//			dollarCount++;
//			currentSqlAttrib2 = prefix + currentChar;
//			sql.append(currentSqlAttrib2);
			if (!current.getOperator().isEmpty()) {
				sql.append(" WHERE ");
				sql.append(currentSqlAttrib2);
				if(current.getOperator().equals(NGSIConstants.QUERY_PATTERNOP)){
					sql.append("->>'");
				}else {
					sql.append("->'");
				}
				sql.append(NGSIConstants.JSON_LD_VALUE);
				sql.append("'");
				dollarCount = applyOperator(sql, dollarCount, tuple, false);
			}

			for (int i = 0; i < subAttribPath.length; i++) {
				sql.append(") ");
			}
			sql.append(") WHEN ");
			sql.append(currentSqlAttrib);
			sql.append(" #>>'{");
			sql.append(NGSIConstants.JSON_LD_TYPE);
			sql.append(",0}' = '");
			sql.append(NGSIConstants.NGSI_LD_LANGPROPERTY);
			sql.append("' THEN EXISTS (SELECT TRUE FROM JSONB_ARRAY_ELEMENTS(");
			sql.append(currentSqlAttrib);
			sql.append(" ->'");
			sql.append(NGSIConstants.NGSI_LD_HAS_LANGUAGE_MAP);
			sql.append("') AS LANGPROP");
			if (!current.getOperator().isEmpty()) {
				sql.append(" WHERE ");
				if (!subAttribPath[0].equals("*")) {
					sql.append("LANGPROP ->> '@language'=$");
					sql.append(dollarCount);
					dollarCount++;
					tuple.addString(subAttribPath[0]);
					sql.append(" AND ");
				}
				if(current.getOperator().equals(NGSIConstants.QUERY_PATTERNOP)){
					sql.append("LANGPROP ->> '");
				}else {
					sql.append("LANGPROP -> '");
				}
				sql.append(NGSIConstants.JSON_LD_VALUE);
				sql.append("'");
				dollarCount = current.applyOperator(sql, dollarCount, tuple, false);
			}

			sql.append(") WHEN ");
			sql.append(currentSqlAttrib);
			sql.append(" #>>'{");
			sql.append(NGSIConstants.JSON_LD_TYPE);
			sql.append(",0}' = '");
			sql.append(NGSIConstants.NGSI_LD_JSON_PROPERTY);
			sql.append("' THEN EXISTS (SELECT TRUE FROM JSONB_ARRAY_ELEMENTS(");
			sql.append(currentSqlAttrib);
			sql.append(" ->'");
			sql.append(NGSIConstants.NGSI_LD_HAS_JSON);
			sql.append("') AS ");
			sql.append(currentSqlAttrib2);
			sql.append(" WHERE ");
			sql.append(currentSqlAttrib2);
			sql.append(" -> '");
			sql.append(NGSIConstants.JSON_LD_VALUE);
			sql.append("' ->> $");
			sql.append(dollarCount);
			dollarCount++;
			tuple.addString(subAttribPath[0]);
			sql.append("=");
			sql.append(" $");
			sql.append(dollarCount);
			dollarCount++;
			tuple.addString(current.operant);
			sql.append(")  ELSE FALSE END ");
		}
		sql.append(") ");
		for (int i = 1; i < attribPath.length; i++) {
			sql.append(") ");
		}
		return dollarCount;

	}

	public int[] toTempSql(StringBuilder sql, int dollarCount, Tuple tuple, int charCount, String prevIdList,
			TemporalQueryTerm tempQueryTerm) {
		sql.append("filtered");
		sql.append(charCount);
		charCount++;
		sql.append(" as (SELECT DISTINCT TEAI.TEMPORALENTITY_ID as id FROM (");
		sql.append(prevIdList);
		sql.append(" LEFT JOIN TEMPORALENTITYATTRINSTANCE ON ");
		sql.append(prevIdList);
		sql.append(".id = TEMPORALENTITYATTRINSTANCE.TEMPORALENTITY_ID) AS TEAI WHERE ");
		dollarCount = temporalSqlWherePart(sql, dollarCount, tuple, this, tempQueryTerm);
		if (firstChild != null) {
			sql.append("), ");
			return firstChild.toTempSql(sql, dollarCount, tuple, charCount, "filtered" + (charCount - 1), null);
		}
		QQueryTerm current = this;
		while (current.hasNext()) {
			if (!current.nextAnd) {
				sql.append(" OR ");
				dollarCount = temporalSqlWherePart(sql, dollarCount, tuple, current.next, null);
				current = current.next;
			} else {
				sql.append("), ");
				return current.next.toTempSql(sql, dollarCount, tuple, charCount, "filtered" + (charCount - 1), null);
			}
		}
		sql.append("), ");
		return new int[] { dollarCount, charCount };
	}

	public int toTempSql(StringBuilder sql, StringBuilder laterSql, Tuple tuple, int dollarCount) {
		QQueryTerm current = this;

		while (current != null) {
			if (firstChild != null) {
				laterSql.append("(");
				dollarCount = firstChild.toTempSql(sql, laterSql, tuple, dollarCount);
				laterSql.append(")");
			}
			laterSql.append("$");
			laterSql.append(dollarCount);
			laterSql.append(" = any(attribs)");
			tuple.addString(linkHeaders.expandIri(current.attribute, false, true, null, null));
			if (current.hasNext()) {
				if (current.isNextAnd()) {
					laterSql.append(" AND ");
				} else {
					laterSql.append(" OR ");
				}
			}
			dollarCount++;
			if (current.getOperant() != null) {
				sql.append(" WHEN TEAI.attributeid=$");
				sql.append(dollarCount - 1);
				sql.append(" THEN (");

				dollarCount = temporalSqlWherePart(sql, dollarCount, tuple, current, null);
			}

			current = current.getNext();
		}

		return dollarCount;
	}

	public Context getLinkHeaders() {
		return linkHeaders;
	}

	public void setLinkHeaders(Context linkHeaders) {
		this.linkHeaders = linkHeaders;
	}

}
