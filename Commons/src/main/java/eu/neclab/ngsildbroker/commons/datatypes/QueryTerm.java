package eu.neclab.ngsildbroker.commons.datatypes;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import eu.neclab.ngsildbroker.commons.constants.DBConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.BadRequestException;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.ngsiqueries.ParamsResolver;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;

public class QueryTerm {

	private static final String RANGE = ".+\\.\\..+";
	private static final String LIST = ".+(,.+)+";
	private static final String URI = "\\w+:(\\/?\\/?)[^\\s^;]+";
	private static final String DATETIME = "\\d\\d\\d\\d-\\d\\d-\\d\\dT\\d\\d:\\d\\d:\\d\\d(,\\d\\d\\d\\d\\d\\d)?Z";
	private static final String DATE = "\\d\\d\\d\\d-\\d\\d-\\d\\d";
	private static final String TIME = "\\d\\d:\\d\\d:\\d\\d(,\\d\\d\\d\\d\\d\\d)?Z";

	private static final List<String> TIME_PROPS = Arrays.asList(NGSIConstants.NGSI_LD_OBSERVED_AT,
			NGSIConstants.NGSI_LD_CREATED_AT, NGSIConstants.NGSI_LD_MODIFIED_AT);
	private List<Object> linkHeaders;
	private QueryTerm next = null;
	private boolean nextAnd = true;
	private QueryTerm firstChild = null;
	private QueryTerm parent = null;
	private String attribute = "";
	private String operator = "";
	private String operant = "";

	ParamsResolver paramsResolver;

	public QueryTerm(List<Object> linkHeaders, ParamsResolver paramsResolver) {
		this.linkHeaders = linkHeaders;
		this.paramsResolver = paramsResolver;
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
	public boolean calculate(List<BaseProperty> properties) throws ResponseException {
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

	private boolean calculate(List<BaseProperty> properties, String attribute, String operator, String operant)
			throws ResponseException {

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
			for (BaseProperty myProperty : myProperties) {
				Iterator it = myProperty.getEntries().values().iterator();
				while (it.hasNext()) {
					BaseEntry next = (BaseEntry) it.next();
					boolean skip = false;
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
							throw new ResponseException(ErrorType.BadRequestData, e.getMessage());
						}
					}
					if (operant.matches(RANGE)) {
						String[] range = operant.split("\\.\\.");

						switch (operator) {
						case "==":
							if (range[0].compareTo(value.toString()) <= 0
									&& range[1].compareTo(value.toString()) >= 0) {
								return true;
							}
							break;
						case "!=":
							if (range[0].compareTo(value.toString()) <= 0
									&& range[1].compareTo(value.toString()) <= 0) {
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
							if (value.toString().compareTo(operant) >= 0) {
								return true;
							}
							break;
						case "<=":
							if (value.toString().compareTo(operant) <= 0) {
								return true;
							}
							break;
						case ">":
							if (value.toString().compareTo(operant) > 0) {
								return true;
							}
							break;
						case "<":
							if (value.toString().compareTo(operant) < 0) {
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
		Iterator it = potentialMatch.getEntries().values().iterator();
		while (it.hasNext()) {
			BaseEntry next = (BaseEntry) it.next();
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

	private Object getCompoundValue(Object value, String[] compound) throws ResponseException {
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

	private Object getValue(BaseEntry myEntry) {
		Object value = null;
		if (myEntry instanceof PropertyEntry) {
			value = ((PropertyEntry) myEntry).getValue();
			if (value instanceof List) {
				value = ((List) value).get(0);
			}
		} else if (myEntry instanceof RelationshipEntry) {
			value = ((RelationshipEntry) myEntry).getObject().toString();
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

	private String expandAttributeName(String attribute) throws ResponseException {

		return paramsResolver.expandAttribute(attribute, linkHeaders);

	}

	public QueryTerm getNext() {
		return next;
	}

	public void setNext(QueryTerm next) {
		this.next = next;
		this.next.setParent(this.getParent());
	}

	public boolean isNextAnd() {
		return nextAnd;
	}

	public void setNextAnd(boolean nextAnd) {
		this.nextAnd = nextAnd;
	}

	public QueryTerm getFirstChild() {
		return firstChild;
	}

	public void setFirstChild(QueryTerm firstChild) {
		this.firstChild = firstChild;
		this.firstChild.setParent(this);
	}

	public String getAttribute() {
		return attribute;
	}

	public void setAttribute(String attribute) {
		this.attribute = attribute;
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

	public void setOperant(String operant) {
		if (operant.matches(URI) && !operant.matches(TIME)) { // uri and time patterns are ambiguous in the abnf grammar
			this.operant = "\"" + operant + "\"";
		} else {
			this.operant = operant;
		}

	}

	public QueryTerm getParent() {
		return parent;
	}

	public void setParent(QueryTerm parent) {
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
		QueryTerm other = (QueryTerm) obj;
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

	public String toSql() throws ResponseException {
		StringBuilder builder = new StringBuilder();
		toSql(builder, false);
		// builder.append(";");
		return builder.toString();
	}

	public String toSql(boolean temporalEntityFormat) throws ResponseException {
		StringBuilder builder = new StringBuilder();
		toSql(builder, temporalEntityFormat);
		// builder.append(";");
		return builder.toString();
	}

	private void toSql(StringBuilder result, boolean temporalEntityMode) throws ResponseException {
		if (firstChild != null) {
			result.append("(");
			firstChild.toSql(result, temporalEntityMode);
			result.append(")");
		} else {
			if (temporalEntityMode) {
				getAttribQueryForTemporalEntity(result);
			} else {
				getAttribQueryV2(result);
			}
		}
		if (hasNext()) {
			if (nextAnd) {
				result.append(" and ");
			} else {
				result.append(" or ");
			}
			next.toSql(result, temporalEntityMode);
		}
	}

	private void getAttribQuery(StringBuilder result) throws ResponseException {
		ArrayList<String> attribPath = getAttribPathArray();

		StringBuilder testAttributeExistsProperty = new StringBuilder("");
		StringBuilder testAttributeExistsRelationship = new StringBuilder("");
		StringBuilder attributeFilterProperty = new StringBuilder("");
		StringBuilder attributeFilterRelationship = new StringBuilder("");
		StringBuilder testValueTypeForPatternOp = new StringBuilder("");
		StringBuilder testValueTypeForDateTime = new StringBuilder("");

		String reservedDbColumn = null;
		if (attribPath.size() == 1) {
			// if not mapped, returns null
			reservedDbColumn = DBConstants.NGSILD_TO_SQL_RESERVED_PROPERTIES_MAPPING.get(attribPath.get(0));
		}

		// do not use createdAt/modifiedAt db columns if value (operant) is not a
		// date/time value
		if (reservedDbColumn != null
				&& (reservedDbColumn.equals(DBConstants.DBCOLUMN_CREATED_AT)
						|| reservedDbColumn.equals(DBConstants.DBCOLUMN_MODIFIED_AT))
				&& !(operant.matches(DATE) || operant.matches(TIME) || operant.matches(DATETIME))) {
			reservedDbColumn = null;
		}

		if (reservedDbColumn == null) {
			testAttributeExistsProperty.append("data@>'{\"");
			testAttributeExistsRelationship.append("data@>'{\"");
			attributeFilterProperty.append("(data#");
			attributeFilterRelationship.append("data#");
			testValueTypeForPatternOp.append("jsonb_typeof(data#>'{");
			testValueTypeForDateTime.append("data#>>'{");
			if (operator.equals(NGSIConstants.QUERY_PATTERNOP) || operator.equals(NGSIConstants.QUERY_NOTPATTERNOP)
					|| operant.matches(DATE) || operant.matches(TIME) || operant.matches(DATETIME)) {
				attributeFilterProperty.append(">>");
				attributeFilterRelationship.append(">>");
			} else {
				attributeFilterProperty.append(">");
				attributeFilterRelationship.append(">");
			}
			attributeFilterProperty.append("'{");
			attributeFilterRelationship.append("'{");

			int iElem = 0;
			String lastAttribute = "";
			for (String subPath : attribPath) {
				// in compoundAttrName filter, Property test only applies to the top level
				// element
				if (!attribute.contains("[") || attribute.contains("[") && iElem == 0) {
					testAttributeExistsProperty.append(subPath);
					testAttributeExistsProperty.append("\":[{\"");
					testAttributeExistsRelationship.append(subPath);
					testAttributeExistsRelationship.append("\":[{\"");
				}
				attributeFilterProperty.append(subPath);
				attributeFilterProperty.append(",0,");
				attributeFilterRelationship.append(subPath);
				attributeFilterRelationship.append(",0,");
				testValueTypeForPatternOp.append(subPath);
				testValueTypeForPatternOp.append(",0,");
				testValueTypeForDateTime.append(subPath);
				testValueTypeForDateTime.append(",0,");
				// in compoundAttrName filter, hasValue/hasObject is in the top level element
				if (attribute.contains("[") && iElem == 0) {
					attributeFilterProperty.append(NGSIConstants.NGSI_LD_HAS_VALUE + ",0,");
					attributeFilterRelationship.append(NGSIConstants.NGSI_LD_HAS_OBJECT + ",0,");
					testValueTypeForPatternOp.append(NGSIConstants.NGSI_LD_HAS_VALUE + ",0,");
					testValueTypeForDateTime.append(NGSIConstants.NGSI_LD_HAS_VALUE + ",0,");
				}
				iElem++;
				lastAttribute = subPath;
			}

			// createdAt/modifiedAt/observedAt type is DateTime (without array brackets)
			if (lastAttribute.equals(NGSIConstants.NGSI_LD_CREATED_AT)
					|| lastAttribute.equals(NGSIConstants.NGSI_LD_MODIFIED_AT)
					|| lastAttribute.equals(NGSIConstants.NGSI_LD_OBSERVED_AT)) {
				testAttributeExistsProperty.append("@type\":\"" + NGSIConstants.NGSI_LD_DATE_TIME + "\"");
			} else {
				testAttributeExistsProperty.append("@type\":[\"" + NGSIConstants.NGSI_LD_PROPERTY + "\"]");
			}
			testAttributeExistsRelationship.append("@type\":[\"" + NGSIConstants.NGSI_LD_RELATIONSHIP + "\"]");
			for (int i = 0; i < attribPath.size(); i++) {
				if (!attribute.contains("[") || attribute.contains("[") && i == 0) {
					testAttributeExistsProperty.append("}]");
					testAttributeExistsRelationship.append("}]");
				}
			}
			testAttributeExistsProperty.append("}'");
			testAttributeExistsRelationship.append("}'");
			// in compoundAttrName, hasValue is at the top level element.
			// createdAt/modifiedAt/observedAt properties do not have a hasValue element
			if (!attribute.contains("[") && !lastAttribute.equals(NGSIConstants.NGSI_LD_CREATED_AT)
					&& !lastAttribute.equals(NGSIConstants.NGSI_LD_MODIFIED_AT)
					&& !lastAttribute.equals(NGSIConstants.NGSI_LD_OBSERVED_AT)) {
				attributeFilterProperty.append(NGSIConstants.NGSI_LD_HAS_VALUE + ",0,");
				attributeFilterRelationship.append(NGSIConstants.NGSI_LD_HAS_OBJECT + ",0,");
				testValueTypeForPatternOp.append(NGSIConstants.NGSI_LD_HAS_VALUE + ",0,");
				testValueTypeForDateTime.append(NGSIConstants.NGSI_LD_HAS_VALUE + ",0,");
			}
			attributeFilterProperty.append("@value}')");
			attributeFilterRelationship.append("@id}'");
			testValueTypeForPatternOp.append("@value}') = 'string'");
			testValueTypeForDateTime.append("@type}' = ");
			if (operant.matches(DATETIME)) {
				attributeFilterProperty.append("::timestamp ");
				testValueTypeForDateTime.append("'" + NGSIConstants.NGSI_LD_DATE_TIME + "'");
			} else if (operant.matches(DATE)) {
				attributeFilterProperty.append("::date ");
				testValueTypeForDateTime.append("'" + NGSIConstants.NGSI_LD_DATE + "'");
			} else if (operant.matches(TIME)) {
				attributeFilterProperty.append("::time ");
				testValueTypeForDateTime.append("'" + NGSIConstants.NGSI_LD_TIME + "'");
			}

		} else {
			attributeFilterProperty.append(reservedDbColumn);
		}

		boolean useRelClause = applyOperator(attributeFilterProperty, attributeFilterRelationship);

		if (reservedDbColumn == null) {
			if (useRelClause) {
				result.append("((" + testAttributeExistsProperty.toString() + " and "
						+ attributeFilterProperty.toString() + ") or (" + testAttributeExistsRelationship.toString()
						+ " and " + attributeFilterRelationship.toString() + "))");
			} else {
				result.append(
						"(" + testAttributeExistsProperty.toString() + " and " + attributeFilterProperty.toString());
				if (operator.equals(NGSIConstants.QUERY_PATTERNOP)
						|| operator.equals(NGSIConstants.QUERY_NOTPATTERNOP)) {
					result.append(" and " + testValueTypeForPatternOp.toString());
				}
				if (operant.matches(DATE) || operant.matches(TIME) || operant.matches(DATETIME)) {
					result.append(" and " + testValueTypeForDateTime.toString());
				}
				result.append(")");
			}
		} else {
			result.append("(" + attributeFilterProperty.toString() + ")");
		}

	}

	private void getAttribQueryV2(StringBuilder result) throws ResponseException {
		ArrayList<String> attribPath = getAttribPathArray();

		StringBuilder attributeFilterProperty = new StringBuilder("");

		String reservedDbColumn = null;
		if (attribPath.size() == 1) {
			// if not mapped, returns null
			reservedDbColumn = DBConstants.NGSILD_TO_SQL_RESERVED_PROPERTIES_MAPPING.get(attribPath.get(0));
		}

		// do not use createdAt/modifiedAt db columns if value (operant) is not a
		// date/time value
		if (reservedDbColumn != null
				&& (reservedDbColumn.equals(DBConstants.DBCOLUMN_CREATED_AT)
						|| reservedDbColumn.equals(DBConstants.DBCOLUMN_MODIFIED_AT))
				&& !(operant.matches(DATE) || operant.matches(TIME) || operant.matches(DATETIME))) {
			reservedDbColumn = null;
		}
		if (reservedDbColumn != null) {
			attributeFilterProperty.append(reservedDbColumn);
			applyOperator(attributeFilterProperty);
		} else {
			/*
			 * EXISTS (SELECT FROM
			 * jsonb_array_elements(data#>'{https://uri.etsi.org/ngsi-ld/default-context/
			 * friend}') as x WHERE x#> '{https://uri.etsi.org/ngsi-ld/hasObject,0,@id}' =
			 * '"urn:person:Victoria"' OR x#>
			 * '{https://uri.etsi.org/ngsi-ld/hasValue,0,@id}' = '"urn:person:Victoria"')
			 */

			int iElem = 0;
			String currentSet = "data";
			char charcount = 'a';
			String lastAttrib = null;
			for (String subPath : attribPath) {
				attributeFilterProperty.append("EXISTS (SELECT FROM jsonb_array_elements(" + currentSet + "#>'{");
				attributeFilterProperty.append(subPath);
				if (attribute.contains("[")&&attribute.contains(".") && iElem == 1) {
					attributeFilterProperty.append(",0," + NGSIConstants.NGSI_LD_HAS_VALUE);
				}
				else if (attribute.contains("[") &&!attribute.contains(".") && iElem == 0) {
					attributeFilterProperty.append(",0," + NGSIConstants.NGSI_LD_HAS_VALUE);
				}
				attributeFilterProperty.append("}') as ");
				attributeFilterProperty.append(charcount);
				currentSet = "" + charcount;
				attributeFilterProperty.append(" WHERE ");
				charcount++;
				iElem++;
				lastAttrib = subPath;
			}

			// x#> '{https://uri.etsi.org/ngsi-ld/hasObject,0,@id}'
			charcount--;
			if (operator.equals(NGSIConstants.QUERY_EQUAL) || operator.equals(NGSIConstants.QUERY_UNEQUAL)
					|| operator.equals(NGSIConstants.QUERY_PATTERNOP)
					|| operator.equals(NGSIConstants.QUERY_NOTPATTERNOP)) {
				attributeFilterProperty.append(charcount);
				attributeFilterProperty.append("#> '{");
				attributeFilterProperty.append("https://uri.etsi.org/ngsi-ld/hasObject,0,@id}'");
				applyOperator(attributeFilterProperty);
				attributeFilterProperty.append(" OR ");
			}
			attributeFilterProperty.append('(');
			attributeFilterProperty.append(charcount);
			attributeFilterProperty.append("#>");
			if (operator.equals(NGSIConstants.QUERY_PATTERNOP) || operator.equals(NGSIConstants.QUERY_NOTPATTERNOP)
					|| operant.matches(DATE) || operant.matches(TIME) || operant.matches(DATETIME)) {
				attributeFilterProperty.append(">");
			}
			attributeFilterProperty.append(" '{");
			attributeFilterProperty.append("https://uri.etsi.org/ngsi-ld/hasValue,0,@value}')");
			if (operant.matches(DATETIME)) {
				attributeFilterProperty.append("::timestamp ");
			} else if (operant.matches(DATE)) {
				attributeFilterProperty.append("::date ");
			} else if (operant.matches(TIME)) {
				attributeFilterProperty.append("::time ");
			}
			applyOperator(attributeFilterProperty);
			attributeFilterProperty.append(" OR ");
			if (TIME_PROPS.contains(lastAttrib)) {
				attributeFilterProperty.append('(');
				attributeFilterProperty.append((char) (charcount - 1));
				attributeFilterProperty.append("#>>");
				attributeFilterProperty.append(" '{");
				attributeFilterProperty.append(lastAttrib);
				attributeFilterProperty.append(",0,@value}')");

			} else if (lastAttrib.equals(NGSIConstants.NGSI_LD_DATA_SET_ID)) {
				attributeFilterProperty.append('(');
				attributeFilterProperty.append(charcount);
				attributeFilterProperty.append("#>");
				if (operator.equals(NGSIConstants.QUERY_PATTERNOP) || operator.equals(NGSIConstants.QUERY_NOTPATTERNOP)
						|| operant.matches(DATE) || operant.matches(TIME) || operant.matches(DATETIME)) {
					attributeFilterProperty.append(">");
				}
				attributeFilterProperty.append(" '{");
				attributeFilterProperty.append("@id}')");
			} else {
				attributeFilterProperty.append('(');
				attributeFilterProperty.append(charcount);
				attributeFilterProperty.append("#>");
				if (operator.equals(NGSIConstants.QUERY_PATTERNOP) || operator.equals(NGSIConstants.QUERY_NOTPATTERNOP)
						|| operant.matches(DATE) || operant.matches(TIME) || operant.matches(DATETIME)) {
					attributeFilterProperty.append(">");
				}
				attributeFilterProperty.append(" '{");
				attributeFilterProperty.append("@value}')");

			}

			if (operant.matches(DATETIME)) {
				attributeFilterProperty.append("::timestamp ");
			} else if (operant.matches(DATE)) {
				attributeFilterProperty.append("::date ");
			} else if (operant.matches(TIME)) {
				attributeFilterProperty.append("::time ");
			}
			applyOperator(attributeFilterProperty);
			for (int i = 0; i < attribPath.size(); i++) {
				attributeFilterProperty.append(')');
			}
		}

		result.append("(" + attributeFilterProperty.toString() + ")");

	}

	private ArrayList<String> getAttribPathArray() throws ResponseException {
		ArrayList<String> attribPath = new ArrayList<String>();
		if (attribute.contains("[") && attribute.contains(".")) {
			if (attribute.contains(".")) {
				for (String subPart : attribute.split("\\.")) {
					if (subPart.contains("[")) {
						for (String subParts : subPart.split("\\[")) {
							subParts = subParts.replaceAll("\\]", "");
							attribPath.add(expandAttributeName(subParts));
						}
					} else {
						attribPath.add(expandAttributeName(subPart));
					}
				}
			}
		}
		else if (attribute.contains("[")) {
			for (String subPart : attribute.split("\\[")) {
				subPart = subPart.replaceAll("\\]", "");
				attribPath.add(expandAttributeName(subPart));
			}
		} else if (attribute.matches(URI)) {
			attribPath.add(expandAttributeName(attribute));
		} else if (attribute.contains(".")) {
			for (String subPart : attribute.split("\\.")) {
				attribPath.add(expandAttributeName(subPart));
			}
		} else {
			attribPath.add(expandAttributeName(attribute));
		}
		return attribPath;
	}

	private boolean applyOperator(StringBuilder attributeFilterProperty) throws BadRequestException {
		boolean useRelClause = false;

		String typecast = "jsonb";
		if (operant.matches(DATETIME)) {
			typecast = "timestamp";
		} else if (operant.matches(DATE)) {
			typecast = "date";
		} else if (operant.matches(TIME)) {
			typecast = "time";
		}

		switch (operator) {
		case NGSIConstants.QUERY_EQUAL:
			if (operant.matches(LIST)) {
				attributeFilterProperty.append(" in (");
				for (String listItem : operant.split(",")) {
					attributeFilterProperty.append("'" + listItem + "'::" + typecast + ",");
				}
				attributeFilterProperty.setCharAt(attributeFilterProperty.length() - 1, ')');
			} else if (operant.matches(RANGE)) {
				String[] myRange = operant.split("\\.\\.");
				attributeFilterProperty.append(
						" between '" + myRange[0] + "'::" + typecast + " and '" + myRange[1] + "'::" + typecast);
			} else {
				attributeFilterProperty.append(" = '" + operant + "'::" + typecast);

			}
			useRelClause = !(operant.matches(DATE) || operant.matches(TIME) || operant.matches(DATETIME));
			break;
		case NGSIConstants.QUERY_UNEQUAL:
			if (operant.matches(LIST)) {
				attributeFilterProperty.append(" not in (");
				for (String listItem : operant.split(",")) {
					attributeFilterProperty.append("'" + listItem + "'::" + typecast + ",");
				}
				attributeFilterProperty.setCharAt(attributeFilterProperty.length() - 1, ')');
			} else if (operant.matches(RANGE)) {
				String[] myRange = operant.split("\\.\\.");
				attributeFilterProperty.append(
						" not between '" + myRange[0] + "'::" + typecast + " and '" + myRange[1] + "'::" + typecast);
			} else {
				attributeFilterProperty.append(" <> '" + operant + "'::" + typecast);
			}
			useRelClause = !(operant.matches(DATE) || operant.matches(TIME) || operant.matches(DATETIME));
			break;
		case NGSIConstants.QUERY_GREATEREQ:
			if (operant.matches(LIST)) {
				throw new BadRequestException();
			}
			if (operant.matches(RANGE)) {
				throw new BadRequestException();
			}
			attributeFilterProperty.append(" >= '" + operant + "'::" + typecast);
			break;
		case NGSIConstants.QUERY_LESSEQ:
			if (operant.matches(LIST)) {
				throw new BadRequestException();
			}
			if (operant.matches(RANGE)) {
				throw new BadRequestException();
			}
			attributeFilterProperty.append(" <= '" + operant + "'::" + typecast);
			break;
		case NGSIConstants.QUERY_GREATER:
			if (operant.matches(LIST)) {
				throw new BadRequestException();
			}
			if (operant.matches(RANGE)) {
				throw new BadRequestException();
			}
			attributeFilterProperty.append(" > '" + operant + "'::" + typecast);
			break;
		case NGSIConstants.QUERY_LESS:
			if (operant.matches(LIST)) {
				throw new BadRequestException();
			}
			if (operant.matches(RANGE)) {
				throw new BadRequestException();
			}
			attributeFilterProperty.append(" < '" + operant + "'::" + typecast);
			break;
		case NGSIConstants.QUERY_PATTERNOP:
			if (operant.matches(LIST)) {
				throw new BadRequestException();
			}
			if (operant.matches(RANGE)) {
				throw new BadRequestException();
			}
			attributeFilterProperty.append(" ~ '" + operant + "'");
			break;
		case NGSIConstants.QUERY_NOTPATTERNOP:
			if (operant.matches(LIST)) {
				throw new BadRequestException();
			}
			if (operant.matches(RANGE)) {
				throw new BadRequestException();
			}
			attributeFilterProperty.append(" !~ '" + operant + "'");
			break;
		default:
			throw new BadRequestException();
		}
		return useRelClause;
	}

	private boolean applyOperator(StringBuilder attributeFilterProperty, StringBuilder attributeFilterRelationship)
			throws BadRequestException {
		boolean useRelClause = false;

		String typecast = "jsonb";
		if (operant.matches(DATETIME)) {
			typecast = "timestamp";
		} else if (operant.matches(DATE)) {
			typecast = "date";
		} else if (operant.matches(TIME)) {
			typecast = "time";
		}

		switch (operator) {
		case NGSIConstants.QUERY_EQUAL:
			if (operant.matches(LIST)) {
				attributeFilterProperty.append(" in (");
				attributeFilterRelationship.append(" in (");
				for (String listItem : operant.split(",")) {
					attributeFilterProperty.append("'" + listItem + "'::" + typecast + ",");
					attributeFilterRelationship.append("'" + listItem + "'::" + typecast + ",");
				}
				attributeFilterProperty.setCharAt(attributeFilterProperty.length() - 1, ')');
				attributeFilterRelationship.setCharAt(attributeFilterRelationship.length() - 1, ')');
			} else if (operant.matches(RANGE)) {
				String[] myRange = operant.split("\\.\\.");
				attributeFilterProperty.append(
						" between '" + myRange[0] + "'::" + typecast + " and '" + myRange[1] + "'::" + typecast);
				attributeFilterRelationship.append(
						" between '" + myRange[0] + "'::" + typecast + " and '" + myRange[1] + "'::" + typecast);
			} else {
				attributeFilterProperty.append(" = '" + operant + "'::" + typecast);
				attributeFilterRelationship.append(" = '" + operant + "'::" + typecast);

			}
			useRelClause = !(operant.matches(DATE) || operant.matches(TIME) || operant.matches(DATETIME));
			break;
		case NGSIConstants.QUERY_UNEQUAL:
			if (operant.matches(LIST)) {
				attributeFilterProperty.append(" not in (");
				attributeFilterRelationship.append(" not in (");
				for (String listItem : operant.split(",")) {
					attributeFilterProperty.append("'" + listItem + "'::" + typecast + ",");
					attributeFilterRelationship.append("'" + listItem + "'::" + typecast + ",");
				}
				attributeFilterProperty.setCharAt(attributeFilterProperty.length() - 1, ')');
				attributeFilterRelationship.setCharAt(attributeFilterRelationship.length() - 1, ')');
			} else if (operant.matches(RANGE)) {
				String[] myRange = operant.split("\\.\\.");
				attributeFilterProperty.append(
						" not between '" + myRange[0] + "'::" + typecast + " and '" + myRange[1] + "'::" + typecast);
				attributeFilterRelationship.append(
						" not between '" + myRange[0] + "'::" + typecast + " and '" + myRange[1] + "'::" + typecast);
			} else {
				attributeFilterProperty.append(" <> '" + operant + "'::" + typecast);
				attributeFilterRelationship.append(" <> '" + operant + "'::" + typecast);

			}
			useRelClause = !(operant.matches(DATE) || operant.matches(TIME) || operant.matches(DATETIME));
			break;
		case NGSIConstants.QUERY_GREATEREQ:
			if (operant.matches(LIST)) {
				throw new BadRequestException();
			}
			if (operant.matches(RANGE)) {
				throw new BadRequestException();
			}
			attributeFilterProperty.append(" >= '" + operant + "'::" + typecast);
			break;
		case NGSIConstants.QUERY_LESSEQ:
			if (operant.matches(LIST)) {
				throw new BadRequestException();
			}
			if (operant.matches(RANGE)) {
				throw new BadRequestException();
			}
			attributeFilterProperty.append(" <= '" + operant + "'::" + typecast);
			break;
		case NGSIConstants.QUERY_GREATER:
			if (operant.matches(LIST)) {
				throw new BadRequestException();
			}
			if (operant.matches(RANGE)) {
				throw new BadRequestException();
			}
			attributeFilterProperty.append(" > '" + operant + "'::" + typecast);
			break;
		case NGSIConstants.QUERY_LESS:
			if (operant.matches(LIST)) {
				throw new BadRequestException();
			}
			if (operant.matches(RANGE)) {
				throw new BadRequestException();
			}
			attributeFilterProperty.append(" < '" + operant + "'::" + typecast);
			break;
		case NGSIConstants.QUERY_PATTERNOP:
			if (operant.matches(LIST)) {
				throw new BadRequestException();
			}
			if (operant.matches(RANGE)) {
				throw new BadRequestException();
			}
			attributeFilterProperty.append(" ~ '" + operant + "'");
			break;
		case NGSIConstants.QUERY_NOTPATTERNOP:
			if (operant.matches(LIST)) {
				throw new BadRequestException();
			}
			if (operant.matches(RANGE)) {
				throw new BadRequestException();
			}
			attributeFilterProperty.append(" !~ '" + operant + "'");
			break;
		default:
			throw new BadRequestException();
		}
		return useRelClause;
	}

	private void getAttribQueryForTemporalEntity(StringBuilder result) throws ResponseException {
		ArrayList<String> attribPath = getAttribPathArray();
		//https://uri.etsi.org/ngsi-ld/default-context/abstractionLevel,0
		/*
		 * String attribId = null; for (String subPath : attribPath) { attribId =
		 * subPath; break; // sub-properties are not supported yet in HistoryManager }
		 */

		int iElem = 0;
		String currentSet = "m.attrdata";
		char charcount = 'a';
		String lastAttrib = null;
		for (String subPath : attribPath) {
			result.append("EXISTS (SELECT FROM jsonb_array_elements(" + currentSet + "#>'{");
			result.append(subPath);
			if (attribute.contains("[") && iElem == 0) {
				result.append(",0," + NGSIConstants.NGSI_LD_HAS_VALUE);
			}
			result.append("}') as ");
			result.append(charcount);
			currentSet = "" + charcount;
			result.append(" WHERE ");
			charcount++;
			iElem++;
			lastAttrib = subPath;
		}

		// x#> '{https://uri.etsi.org/ngsi-ld/hasObject,0,@id}'
		charcount--;
		if (operator.equals(NGSIConstants.QUERY_EQUAL) || operator.equals(NGSIConstants.QUERY_UNEQUAL)
				|| operator.equals(NGSIConstants.QUERY_PATTERNOP)
				|| operator.equals(NGSIConstants.QUERY_NOTPATTERNOP)) {
			result.append(charcount);
			result.append("#> '{");
			result.append("https://uri.etsi.org/ngsi-ld/hasObject,0,@id}'");
			applyOperator(result);
			result.append(" OR ");
		}
		result.append('(');
		result.append(charcount);
		result.append("#>");
		if (operator.equals(NGSIConstants.QUERY_PATTERNOP) || operator.equals(NGSIConstants.QUERY_NOTPATTERNOP)
				|| operant.matches(DATE) || operant.matches(TIME) || operant.matches(DATETIME)) {
			result.append(">");
		}
		result.append(" '{");
		result.append("https://uri.etsi.org/ngsi-ld/hasValue,0,@value}')");
		if (operant.matches(DATETIME)) {
			result.append("::timestamp ");
		} else if (operant.matches(DATE)) {
			result.append("::date ");
		} else if (operant.matches(TIME)) {
			result.append("::time ");
		}
		applyOperator(result);
		result.append(" OR ");
		if (TIME_PROPS.contains(lastAttrib)) {
			result.append('(');
			result.append((char) (charcount - 1));
			result.append("#>>");
			result.append(" '{");
			result.append(lastAttrib);
			result.append(",0,@value}')");

		} else if (lastAttrib.equals(NGSIConstants.NGSI_LD_DATA_SET_ID) || lastAttrib.equals(NGSIConstants.NGSI_LD_INSTANCE_ID)) {
			result.append('(');
			result.append(charcount);
			result.append("#>");
			if (operator.equals(NGSIConstants.QUERY_PATTERNOP) || operator.equals(NGSIConstants.QUERY_NOTPATTERNOP)
					|| operant.matches(DATE) || operant.matches(TIME) || operant.matches(DATETIME)) {
				result.append(">");
			}
			result.append(" '{");
			result.append("@id}')");
		} else {
			result.append('(');
			result.append(charcount);
			result.append("#>");
			if (operator.equals(NGSIConstants.QUERY_PATTERNOP) || operator.equals(NGSIConstants.QUERY_NOTPATTERNOP)
					|| operant.matches(DATE) || operant.matches(TIME) || operant.matches(DATETIME)) {
				result.append(">");
			}
			result.append(" '{");
			result.append("@value}')");

		}

		if (operant.matches(DATETIME)) {
			result.append("::timestamp ");
		} else if (operant.matches(DATE)) {
			result.append("::date ");
		} else if (operant.matches(TIME)) {
			result.append("::time ");
		}
		applyOperator(result);
		for (int i = 0; i < attribPath.size(); i++) {
			result.append(')');
		}

		
		/*
		 * StringBuilder attributeFilterProperty = new StringBuilder("(m.attrdata#");
		 * StringBuilder attributeFilterRelationship = new StringBuilder("m.attrdata#");
		 * String testValueTypeForPatternOp = new String( "jsonb_typeof(m.attrdata#>'{"
		 * + attribId + ",0," + NGSIConstants.NGSI_LD_HAS_VALUE +
		 * ",0,@value}') = 'string'"); StringBuilder testValueTypeForDateTime = new
		 * StringBuilder( "m.attrdata#>>'{" + attribId + ",0," +
		 * NGSIConstants.NGSI_LD_HAS_VALUE + ",0,@type}' = ");
		 * 
		 * if (operator.equals(NGSIConstants.QUERY_PATTERNOP) ||
		 * operator.equals(NGSIConstants.QUERY_NOTPATTERNOP) || operant.matches(DATE) ||
		 * operant.matches(TIME) || operant.matches(DATETIME)) {
		 * attributeFilterProperty.append(">>");
		 * attributeFilterRelationship.append(">>"); } else {
		 * attributeFilterProperty.append(">"); attributeFilterRelationship.append(">");
		 * }
		 * 
		 * attributeFilterProperty.append("'{" + attribId + ",0," +
		 * NGSIConstants.NGSI_LD_HAS_VALUE + ",0,@value}')");
		 * attributeFilterRelationship.append("'{" + attribId + ",0," +
		 * NGSIConstants.NGSI_LD_HAS_OBJECT + ",0,@id}'"); if
		 * (operant.matches(DATETIME)) { attributeFilterProperty.append("::timestamp ");
		 * testValueTypeForDateTime.append("'" + NGSIConstants.NGSI_LD_DATE_TIME + "'");
		 * } else if (operant.matches(DATE)) {
		 * attributeFilterProperty.append("::date ");
		 * testValueTypeForDateTime.append("'" + NGSIConstants.NGSI_LD_DATE + "'"); }
		 * else if (operant.matches(TIME)) { attributeFilterProperty.append("::time ");
		 * testValueTypeForDateTime.append("'" + NGSIConstants.NGSI_LD_TIME + "'"); }
		 * 
		 * 
		 * boolean useRelClause = applyOperator(attributeFilterProperty,
		 * attributeFilterRelationship); if (useRelClause) { result.append("((" +
		 * attributeFilterProperty.toString() + ") or (" +
		 * attributeFilterRelationship.toString() + "))"); } else { result.append("(" +
		 * attributeFilterProperty.toString()); if
		 * (operator.equals(NGSIConstants.QUERY_PATTERNOP) ||
		 * operator.equals(NGSIConstants.QUERY_NOTPATTERNOP)) { result.append(" and " +
		 * testValueTypeForPatternOp); } if (operant.matches(DATE) ||
		 * operant.matches(TIME) || operant.matches(DATETIME)) { result.append(" and " +
		 * testValueTypeForDateTime.toString()); } result.append(")"); }
		 */
	}

	// Only for testing;
	public void setParamsResolver(ParamsResolver paramsResolver) {
		this.paramsResolver = paramsResolver;
		if (this.hasNext()) {
			next.setParamsResolver(paramsResolver);
		}
		if (this.getFirstChild() != null) {
			this.getFirstChild().setParamsResolver(paramsResolver);
		}
	}

}
