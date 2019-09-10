package eu.neclab.ngsildbroker.commons.datatypes;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import eu.neclab.ngsildbroker.commons.constants.DBConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
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

	private boolean calculate(List<BaseProperty> properties, String attribute, String operator, String operant) throws ResponseException {
		if (!attribute.matches(URI) && attribute.contains(".")) {
			String[] splittedAttrib = attribute.split("\\.");
			ArrayList<BaseProperty> newProps = new ArrayList<BaseProperty>();
			String expanded = expandAttributeName(splittedAttrib[0]);
			if (expanded == null) {
				return false;
			}
			BaseProperty prop = getProperty(properties, expanded);
			if (prop == null) {
				return false;
			}

			if (prop.getProperties() != null) {
				newProps.addAll(prop.getProperties());
			}
			if (prop.getRelationships() != null) {
				newProps.addAll(prop.getRelationships());
			}
			newProps.addAll(getAllNGSIBaseProperties(prop));

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
			}
			String myAttribName = expandAttributeName(attribute);
			if (myAttribName == null) {
				return false;
			}
			BaseProperty myProperty = getProperty(properties, myAttribName);
			List<Object> value = null;
			operant = operant.replace("\"", "");
			if (myProperty == null) {
				return false;
			}
			if (TIME_PROPS.contains(myAttribName)) {
				operant = SerializationTools.date2Long(operant).toString();
			}
			value = getValue(myProperty);
			if (value == null) {
				return false;
			}
			if (compound != null) {
				value = getCompoundValue(value, compound);
				if (value == null) {
					return false;
				}
			}

			if (operant.matches(RANGE)) {
				String[] range = operant.split("\\.\\.");
				for (Object item : value) {
					switch (operator) {
					case "==":
						if (range[0].compareTo(item.toString()) <= 0 && range[1].compareTo(item.toString()) >= 0) {
							return true;
						}
						break;
					case "!=":
						if (range[0].compareTo(item.toString()) <= 0 && range[1].compareTo(item.toString()) <= 0) {
							return true;
						}
						break;
					}
				}
				return false;

			} else if (operant.matches(LIST)) {
				List<String> listOfOperants = Arrays.asList(operant.split(","));

				for (String listOperant : listOfOperants) {
					if (!value.contains(listOperant)) {
						return false;
					}
				}
				return true;

			} else {
				boolean finalReturnValue = false;
				for (Object item : value) {
					switch (operator) {
					case "==":
						if (operant.equals(item.toString())) {
							return true;
						}
						break;
					case "!=":
						finalReturnValue = true;
						if (operant.equals(item.toString())) {
							return false;
						}
						break;
					case ">=":
						if (item.toString().compareTo(operant) >= 0) {
							return true;
						}
						break;
					case "<=":
						if (item.toString().compareTo(operant) <= 0) {
							return true;
						}
						break;
					case ">":
						if (item.toString().compareTo(operant) > 0) {
							return true;
						}
						break;
					case "<":
						if (item.toString().compareTo(operant) < 0) {
							return true;
						}
						break;
					case "~=":
						if (item.toString().matches(operant)) {
							return true;
						}
						break;
					case "!~=":
						finalReturnValue = true;
						if (item.toString().matches(operant)) {
							return false;
						}
						break;
					}
				}
				return finalReturnValue;

			}
		}

	}

	private List<Property> getAllNGSIBaseProperties(BaseProperty prop) {
		ArrayList<Property> result = new ArrayList<Property>();
		try {
			if (prop.getCreatedAt() != -1l) {
				Property createdAtProp = new Property();
				createdAtProp.setId(new URI(NGSIConstants.NGSI_LD_CREATED_AT));
				createdAtProp.setSingleValue(prop.getCreatedAt());
				result.add(createdAtProp);
			}
			if (prop.getObservedAt() != -1l) {
				Property observedAtProp = new Property();
				observedAtProp.setId(new URI(NGSIConstants.NGSI_LD_OBSERVED_AT));
				observedAtProp.setSingleValue(prop.getObservedAt());
				result.add(observedAtProp);
			}

			if (prop.getModifiedAt() != -1l) {
				Property modifiedAtProp = new Property();
				modifiedAtProp.setId(new URI(NGSIConstants.NGSI_LD_MODIFIED_AT));
				modifiedAtProp.setSingleValue(prop.getModifiedAt());
				result.add(modifiedAtProp);
			}
			if (prop instanceof Property) {
				Property realProp = (Property) prop;
				if (realProp.getUnitCode() != null && realProp.getUnitCode().equals("")) {
					Property unitCodeProp = new Property();
					unitCodeProp.setId(new URI(NGSIConstants.NGSI_LD_UNIT_CODE));
					unitCodeProp.setSingleValue(realProp.getUnitCode());
					result.add(unitCodeProp);
				}
			}
		} catch (URISyntaxException e) {
			// Left Empty intentionally. Should never happen since the URI constants are
			// controlled
		}
		return result;
	}

	private List<Object> getCompoundValue(List<Object> value, String[] compound) throws ResponseException {
		List<Object> toSearch = value;
		for (int i = 1; i < compound.length; i++) {
			ArrayList<Object> newToSearch = new ArrayList<Object>();
			for (Object obj : toSearch) {
				if (obj instanceof Map) {
					@SuppressWarnings("unchecked")
					Map<String, List<Object>> temp = (Map<String, List<Object>>) obj;
					String key = expandAttributeName(compound[i].replaceAll("\\]", ""));
					if (temp.containsKey(key)) {
						newToSearch.addAll(temp.get(key));
					}
				} else {
					continue;
				}
			}
			toSearch = newToSearch;
		}
		if (toSearch.isEmpty()) {
			return null;
		}
		return toSearch;
	}

	private List<Object> getValue(BaseProperty myProperty) {
		if (myProperty instanceof Property) {
			return ((Property) myProperty).getValue();
		} else if (myProperty instanceof Relationship) {
			List<Object> result = new ArrayList<Object>();
			result.add(((Relationship) myProperty).getObject().toString());
			return result;
		}
		return null;
	}

	private BaseProperty getProperty(List<BaseProperty> properties, String myAttribName) {
		if (properties == null || properties.isEmpty()) {
			return null;
		}

		for (BaseProperty property : properties) {
			if (property.getId().toString().equals(myAttribName)) {
				return property;
			}
		}

		return null;
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
			if (temporalEntityMode)
				getAttribQueryForTemporalEntity(result);
			else
				getAttribQuery(result);
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

	private ArrayList<String> getAttribPathArray() throws ResponseException {
		ArrayList<String> attribPath = new ArrayList<String>();
		if (attribute.contains("[")) {
			for (String subPart : attribute.split("\\[")) {
				subPart = subPart.replaceAll("\\]", "");
				attribPath.add(expandAttributeName(subPart));
			}
		} else if (attribute.matches(URI)) {
			attribPath.add(attribute);
		} else if (attribute.contains(".")) {
			for (String subPart : attribute.split("\\.")) {
				attribPath.add(expandAttributeName(subPart));
			}
		} else {
			attribPath.add(expandAttributeName(attribute));
		}
		return attribPath;
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
		StringBuilder testAttribute = new StringBuilder("teai.attributeid = '");
		StringBuilder testAttributeExistsProperty = new StringBuilder(
				"teai.data@>'{\"@type\":[\"" + NGSIConstants.NGSI_LD_PROPERTY + "\"]}'");
		StringBuilder testAttributeExistsRelationship = new StringBuilder(
				"teai.data@>'{\"@type\":[\"" + NGSIConstants.NGSI_LD_RELATIONSHIP + "\"]}'");
		StringBuilder attributeFilterProperty = new StringBuilder("(teai.data#");
		StringBuilder attributeFilterRelationship = new StringBuilder("teai.data#");
		String testValueTypeForPatternOp = new String(
				"jsonb_typeof(teai.data#>'{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,@value}') = 'string'");
		StringBuilder testValueTypeForDateTime = new StringBuilder(
				"data#>>'{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,@type}' = ");

		if (operator.equals(NGSIConstants.QUERY_PATTERNOP) || operator.equals(NGSIConstants.QUERY_NOTPATTERNOP)
				|| operant.matches(DATE) || operant.matches(TIME) || operant.matches(DATETIME)) {
			attributeFilterProperty.append(">>");
			attributeFilterRelationship.append(">>");
		} else {
			attributeFilterProperty.append(">");
			attributeFilterRelationship.append(">");
		}

		attributeFilterProperty.append("'{" + NGSIConstants.NGSI_LD_HAS_VALUE + ",0,@value}')");
		attributeFilterRelationship.append("'{" + NGSIConstants.NGSI_LD_HAS_OBJECT + ",0,@id}'");
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

		for (String subPath : attribPath) {
			testAttribute.append(subPath);
			break; // sub-properties are not supported yet in HistoryManager
		}
		testAttribute.append("'");
		boolean useRelClause = applyOperator(attributeFilterProperty, attributeFilterRelationship);
		if (useRelClause) {
			result.append("((" + testAttribute.toString() + " and " + testAttributeExistsProperty.toString() + " and "
					+ attributeFilterProperty.toString() + ") or (" + testAttribute.toString() + " and "
					+ testAttributeExistsRelationship.toString() + " and " + attributeFilterRelationship.toString()
					+ "))");
		} else {
			result.append("(" + testAttribute.toString() + " and " + testAttributeExistsProperty.toString() + " and "
					+ attributeFilterProperty.toString());
			if (operator.equals(NGSIConstants.QUERY_PATTERNOP) || operator.equals(NGSIConstants.QUERY_NOTPATTERNOP)) {
				result.append(" and " + testValueTypeForPatternOp);
			}
			if (operant.matches(DATE) || operant.matches(TIME) || operant.matches(DATETIME)) {
				result.append(" and " + testValueTypeForDateTime.toString());
			}
			result.append(")");
		}

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
