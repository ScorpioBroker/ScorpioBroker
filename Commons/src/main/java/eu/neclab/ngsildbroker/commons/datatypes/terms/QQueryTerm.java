package eu.neclab.ngsildbroker.commons.datatypes.terms;

import java.io.Serializable;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIdentityInfo;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.ObjectIdGenerators;
import com.github.jsonldjava.core.Context;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.BaseEntry;
import eu.neclab.ngsildbroker.commons.datatypes.BaseProperty;
import eu.neclab.ngsildbroker.commons.datatypes.EntityCache;
import eu.neclab.ngsildbroker.commons.datatypes.PropertyEntry;
import eu.neclab.ngsildbroker.commons.datatypes.RelationshipEntry;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;
import io.smallrye.mutiny.tuples.Tuple2;
import io.vertx.mutiny.sqlclient.Tuple;

@JsonIdentityInfo(generator = ObjectIdGenerators.PropertyGenerator.class, property = "id")
public class QQueryTerm implements Serializable {

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
	private static final long serialVersionUID = -7136298831314992504L;
	private static final String RANGE = ".+\\.\\..+";
	private static final String LIST = ".+(,.+)+";
	private static final String URI = "\\w+:(\\/?\\/?)[^\\s^;]+";
	private static final String DATETIME = "\\d\\d\\d\\d-\\d\\d-\\d\\dT\\d\\d:\\d\\d:\\d\\d([,.]\\d{1,6})?Z";
	// private static final String DATE = "\\d\\d\\d\\d-\\d\\d-\\d\\d";
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
	private boolean isLinkedQ = false;
	private Set<String> linkedEntityTypes = Sets.newHashSet();
	private String linkedAttrName = null;
	private boolean hasLinkedQ = false;
	private Map<Integer, Map<String, Set<String>>> joinLevel2AttribAndTypes = Maps.newHashMap();
	private int maxJoinLevel = 0;

	public int getMaxJoinLevel() {
		return maxJoinLevel;
	}

	public void addJoinLevel2AttribAndTypes(Integer joinLevel, String attrib, Set<String> types) {
		Map<String, Set<String>> attrib2Types = joinLevel2AttribAndTypes.get(joinLevel);
		if (attrib2Types == null) {
			attrib2Types = Maps.newHashMap();
			joinLevel2AttribAndTypes.put(joinLevel, attrib2Types);
		}
		Set<String> currentTypes = attrib2Types.get(attrib);
		if (currentTypes == null) {
			attrib2Types.put(attrib, types);
		} else {
			if (types != null) {
				currentTypes.addAll(types);
			}

		}
		if (joinLevel > maxJoinLevel) {
			maxJoinLevel = joinLevel;
		}

	}

	public Map<Integer, Map<String, Set<String>>> getJoinLevel2AttribAndTypes() {
		return joinLevel2AttribAndTypes;
	}

	public Set<String> getLinkedEntityTypes() {
		return linkedEntityTypes;
	}

	public void addLinkedEntityType(String linkedEntityType) {
		this.linkedEntityTypes.add(linkedEntityType);
	}

	public String getLinkedAttrName() {
		return linkedAttrName;
	}

	public void setLinkedAttrName(String linkedAttrName) {
		this.linkedAttrName = linkedAttrName;
	}

	public boolean isLinkedQ() {
		return isLinkedQ;
	}

	public void setLinkedQ(boolean isLinkedQ) {
		this.isLinkedQ = isLinkedQ;
	}

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

	@SuppressWarnings("unchecked")
	private Object getElemByPath(Object entity, String[] path, int index, String operant) {
		if (entity == null || index >= path.length) {
			if (entity instanceof List<?> list && list.get(0) instanceof Map<?, ?> map) {
				if (map.containsKey(NGSIConstants.NGSI_LD_HAS_VALUE)) {
					return getElemByPath(map, new String[] { NGSIConstants.JSON_LD_VALUE }, 0, operant);
				}
				if (map.containsKey(NGSIConstants.NGSI_LD_HAS_OBJECT)) {
					return getElemByPath(map, new String[] { NGSIConstants.JSON_LD_ID }, 0, operant);
				}
				if (map.containsKey(NGSIConstants.JSON_LD_VALUE)) {
					return getElemByPath(map.get(NGSIConstants.JSON_LD_VALUE), path, index + 1, operant);
				}
				if (map.containsKey(NGSIConstants.JSON_LD_ID)) {
					return map.get(NGSIConstants.JSON_LD_ID);
				}
			}
			return entity;
		}
		if (entity instanceof List<?> list) {
			Object result = null;
			for (Object item : list) {
				Object tmpRes = getElemByPath(item, path, index, operant);
				if (tmpRes != null && tmpRes.toString().replace("\"", "").equals(operant.replace("\"", ""))) {
					return tmpRes;
				} else if (tmpRes != null) {
					result = tmpRes;
				}
			}
			return result;
		} else if (entity instanceof Map) {
			Map<String, Object> map = (Map<String, Object>) entity;
			Object value;
			if (map.containsKey(path[index])) {
				value = map.get(path[index++]);
			} else if (map.containsKey(NGSIConstants.NGSI_LD_HAS_VALUE)) {
				value = map.get(NGSIConstants.NGSI_LD_HAS_VALUE);
			} else if (map.containsKey(NGSIConstants.JSON_LD_LANGUAGE)) {
				if (path[index].contains(map.get(NGSIConstants.JSON_LD_LANGUAGE).toString())) {
					value = map.get(NGSIConstants.JSON_LD_VALUE);
					index++;
				} else
					value = null;
			} else if (map.containsKey(NGSIConstants.JSON_LD_VALUE)) {
				return getElemByPath(map.get(NGSIConstants.JSON_LD_VALUE), path, index, operant);
			} else if (map.containsKey(NGSIConstants.JSON_LD_LIST)) {
				value = map.get(NGSIConstants.JSON_LD_LIST);
			} else if (map.containsKey(NGSIConstants.NGSI_LD_HAS_LIST)) {
				value = map.get(NGSIConstants.NGSI_LD_HAS_LIST);
			} else if (map.containsKey(NGSIConstants.NGSI_LD_HAS_OBJECT)) {
				value = map.get(NGSIConstants.NGSI_LD_HAS_OBJECT);
			} else if (map.containsKey(NGSIConstants.NGSI_LD_HAS_OBJECT_LIST)) {
				value = map.get(NGSIConstants.NGSI_LD_HAS_OBJECT_LIST);
			} else if (map.containsKey(NGSIConstants.NGSI_LD_HAS_VOCAB)) {
				value = map.get(NGSIConstants.NGSI_LD_HAS_VOCAB);
			} else if (map.containsKey(NGSIConstants.NGSI_LD_HAS_JSON)) {
				value = map.get(NGSIConstants.NGSI_LD_HAS_JSON);
			} else if (map.containsKey(NGSIConstants.NGSI_LD_HAS_LANGUAGE_MAP)) {
				value = map.get(NGSIConstants.NGSI_LD_HAS_LANGUAGE_MAP);
			} else if (map.containsKey(NGSIConstants.NGSI_LD_HAS_KEY)) {
				value = map.get(NGSIConstants.NGSI_LD_HAS_KEY);
			} else {
				value = map.get(path[index]);
				return getElemByPath(value, path, index + 1, operant);
			}
			return getElemByPath(value, path, index, operant);
		} else {
			return entity; // If obj is neither List nor Map
		}
	}

	public boolean calculate(Map<String, Object> entity, Set<String> jsonKeys) {
		return calculate(entity, this.attribute, this.operator, this.operant, jsonKeys);
	}

	public boolean calculate(Map<String, Object> entity, String attribute, String operator, String operant,
			Set<String> jsonKeys) {

		boolean finalReturnValue = false;
//        if (!attribute.matches(URI) && attribute.contains(".")) {

		String[] splittedAttrib = attribute.split("[\\[\\].]");
		List<String> doNotExpandAttrs = new ArrayList<>();
		if (jsonKeys != null && !jsonKeys.isEmpty()) {
			for (String jsonkey : jsonKeys) {
				doNotExpandAttrs
						.addAll(Arrays.asList(attribute.replaceFirst(jsonkey + "\\[", "").split("]")[0].split("\\.")));
			}
		}
		List<String> expandedAttrib = new ArrayList<>();
		for (String attrs : splittedAttrib) {
			if (doNotExpandAttrs.contains(attrs)) {
				expandedAttrib.add(attrs);
			} else {
				expandedAttrib.add(expandAttributeName(attrs));
			}
		}
		Object value = getElemByPath(entity, expandedAttrib.toArray(new String[0]), 0, operant);
		if (value == null) {
			return false;
		}
		if ((operant == null || operant.isEmpty())) {
			return true;
		}

		operant = operant != null ? operant.replace("\"", "") : null;
		for (String item : expandedAttrib) {
			if (TIME_PROPS.contains(item)) {
				try {
					operant = SerializationTools.date2Long(operant).toString();
				} catch (Exception e) {
					return false;
				}
			}
		}
		if (operant != null && operant.matches(RANGE)) {
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

		} else if (operant != null && operant.matches(LIST)) {
			String[] listOfOperants = operant.split(",");
			if (!(value instanceof List)) {
				return false;
			}
			@SuppressWarnings("unchecked") // check above
			List<Object> myList = (List<Object>) value;
			return switch (operator) {
			case "!=" -> {
				for (String listOperant : listOfOperants) {
					if (myList.contains(listOperant)) {
						yield false;
					}
				}
				yield true;
			}
			case "==" -> {
				for (String listOperant : listOfOperants) {
					if (myList.contains(listOperant)) {
						yield true;
					}
				}
				yield false;
			}
			default -> false;
			};
		} else {
			switch (operator) {
			case "==":
				if (value instanceof List<?> l) {
					return listContains(l, operant);
				}
				if (operant.equals(value.toString())) {
					return true;
				}
				break;
			case "!=":
				finalReturnValue = true;
				if (value instanceof List<?> l) {
					return !listContains(l, operant);
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
//        }
		return finalReturnValue;
	}

	@SuppressWarnings("rawtypes") // rawtypes are fine here and intentionally used
	private boolean calculate(List<BaseProperty> properties, String attribute, String operator, String operant) {

		if (!attribute.matches(URI) && attribute.contains(".")) {
			String[] splittedAttrib = attribute.split("[\\[\\].]");
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
		if (value instanceof List<?> l1) {
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
		ArrayList<BaseProperty> result = new ArrayList<>();
		for (BaseEntry next : potentialMatch.getEntries().values()) {
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

	private Object getValue(BaseEntry myEntry) {
		Object value = null;
		if (myEntry instanceof PropertyEntry) {
			value = ((PropertyEntry) myEntry).getValue();
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

	private int getAttribQuery(StringBuilder result, int dollarCount, Tuple tuple, boolean isDist, boolean localOnly) {
		result.append("ENTITY ? $");
		result.append(dollarCount);

		String[] splitted = getAttribute().split("\\[");
		if (splitted.length > 1) {
			splitted[1] = splitted[1].substring(0, splitted[1].length() - 1);
		}
		String[] subAttribPath = splitted.length == 1 ? null : splitted[1].split("\\.");
		String[] attribPath = splitted[0].split("\\.");
		String attribName = linkHeaders.expandIri(attribPath[0], false, true, null, null);

		if (isLinkedQ) {
			result.append(dollarCount);
			tuple.addString(linkedAttrName);
			dollarCount++;
			if (!isDist || localOnly) {
				result.append(" AND EXISTS (SELECT TRUE FROM JSONB_ARRAY_ELEMENTS(ENTITY -> $");
				result.append(dollarCount);
				tuple.addString(linkedAttrName);
				result.append(
						" ) as rel, JSONB_ARRAY_ELEMENTS(rel -> 'https://uri.etsi.org/ngsi-ld/hasObject') as obj left join entity toplevel on obj->>'@id'=toplevel.id WHERE rel.value #>> '{@type,0}' = 'https://uri.etsi.org/ngsi-ld/Relationship'");
				dollarCount++;
				if (!localOnly) {
					result.append(" AND rel.value ? 'https://uri.etsi.org/ngsi-ld/hasObjectType'");
					if (!linkedEntityTypes.isEmpty()) {
						result.append(
								" AND EXISTS (SELECT TRUE FROM JSONB_ARRAY_ELEMENTS(rel.value -> 'https://uri.etsi.org/ngsi-ld/hasObjectType') as objType WHERE (objType ->> '@id') IN (");
						for (String linkedEntityType : linkedEntityTypes) {
							result.append('$');
							result.append(dollarCount);
							dollarCount++;
							tuple.addString(linkedEntityType);
							result.append(',');
						}
						result.setCharAt(result.length() - 1, ')');
						result.append(" AND toplevel.e_types && ARRAY[");
						for (String linkedEntityType : linkedEntityTypes) {
							result.append('$');
							result.append(dollarCount);
							dollarCount++;
							tuple.addString(linkedEntityType);
							result.append(',');
						}
						result.setCharAt(result.length() - 1, ']');
						result.append(')');
					}

				} else if (!linkedEntityTypes.isEmpty()) {
					result.append(" AND toplevel.e_types && ARRAY[");
					for (String linkedEntityType : linkedEntityTypes) {
						result.append('$');
						result.append(dollarCount);
						dollarCount++;
						tuple.addString(linkedEntityType);
						result.append(',');
					}
					result.setCharAt(result.length() - 1, ']');
				}

				result.append(')');

			}
			if ((operator != null && !operator.isEmpty()) || attribPath.length > 1
					|| (subAttribPath != null && subAttribPath.length > 0)) {
				dollarCount = commonWherePart(attribPath, subAttribPath, "toplevel", dollarCount, tuple, result, this);
			} else {
				result.append(')');
			}
		} else {
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
					dollarCount = commonWherePart(attribPath, subAttribPath, "toplevel", dollarCount, tuple, result,
							this);
				} else {
					result.append(')');
				}
			}
		}
		return dollarCount;
	}

	private int getAttribQuery(StringBuilder result, StringBuilder followUp, int dollarCount, Tuple tuple,
			boolean isDist, boolean localOnly) {

		String[] splitted = getAttribute().split("\\[");
		if (splitted.length > 1) {
			splitted[1] = splitted[1].substring(0, splitted[1].length() - 1);
		}
		String[] subAttribPath = splitted.length == 1 ? null : splitted[1].split("\\.");
		String[] attribPath = splitted[0].split("\\.");
		String attribName = linkHeaders.expandIri(attribPath[0], false, true, null, null);
		boolean wildcardUse;

		if (isLinkedQ) {
			String linkedAttrExpanded = linkHeaders.expandIri(linkedAttrName, false, true, null, null);
			wildcardUse = NGSIConstants.NGSI_LD_STAR.equals(attribName);
			if (!wildcardUse) {
				result.append("ENTITY ? $");
				result.append(dollarCount);

				followUp.append("ENTITY ? ''' || $");
				followUp.append(dollarCount);
				followUp.append("|| '''");
				tuple.addString(linkedAttrExpanded);
				dollarCount++;
			}
			if (!isDist || localOnly) {
				if (wildcardUse) {
					result.append(
							"AND EXISTS (SELECT TRUE FROM JSONB_OBJECT_KEYS(ENTITY) AS ATTRKEY, JSONB_ARRAY_ELEMENTS(ENTITY -> ATTRKEY) AS rel");
					

					followUp.append(
							"AND EXISTS (SELECT TRUE FROM JSONB_OBJECT_KEYS(ENTITY) AS ATTRKEY, JSONB_ARRAY_ELEMENTS(ENTITY -> ATTRKEY) AS rel");
					

				}else {
					result.append(" AND EXISTS (SELECT TRUE FROM JSONB_ARRAY_ELEMENTS(ENTITY -> $");
					result.append(dollarCount);
					result.append(") as rel");
					followUp.append(" AND EXISTS (SELECT TRUE FROM JSONB_ARRAY_ELEMENTS(ENTITY -> ''' || $");
					followUp.append(dollarCount);
					followUp.append("|| ''') as rel");
					tuple.addString(linkedAttrExpanded);
					dollarCount++;
				}
				result.append(
						", JSONB_ARRAY_ELEMENTS(rel -> 'https://uri.etsi.org/ngsi-ld/hasObject') as obj left join entity on obj->>'@id'=entity.id WHERE rel.value #>> '{@type,0}' = 'https://uri.etsi.org/ngsi-ld/Relationship'");
				followUp.append(
						", JSONB_ARRAY_ELEMENTS(rel -> ''https://uri.etsi.org/ngsi-ld/hasObject'') as obj left join entity on obj->>''@id''=entity.id WHERE rel.value #>> ''{@type,0}'' = ''https://uri.etsi.org/ngsi-ld/Relationship''");
				
				
				if (!localOnly) {
					result.append(" AND rel.value ? 'https://uri.etsi.org/ngsi-ld/hasObjectType'");
					if (!linkedEntityTypes.isEmpty()) {
						result.append(
								" AND EXISTS (SELECT TRUE FROM JSONB_ARRAY_ELEMENTS(rel.value -> 'https://uri.etsi.org/ngsi-ld/hasObjectType') as objType WHERE (objType ->> '@id') IN (");
						followUp.append(
								" AND EXISTS (SELECT TRUE FROM JSONB_ARRAY_ELEMENTS(rel.value -> ''https://uri.etsi.org/ngsi-ld/hasObjectType'') as objType WHERE (objType ->> ''@id'') IN ('' || ");
						for (String linkedEntityType : linkedEntityTypes) {
							result.append('$');
							result.append(dollarCount);
							result.append(',');

							followUp.append('$');
							followUp.append(dollarCount);
							followUp.append(" || ''',''' || ");
							dollarCount++;
							tuple.addString(linkedEntityType);

						}
						result.setCharAt(result.length() - 1, ')');
						followUp.setLength(followUp.length() - 8);
						followUp.append(')');
						result.append(" AND entity.e_types && ARRAY[");
						followUp.append(" AND entity.e_types && ARRAY[''' || ");
						for (String linkedEntityType : linkedEntityTypes) {
							result.append('$');
							result.append(dollarCount);
							result.append(',');

							followUp.append('$');
							followUp.append(dollarCount);
							followUp.append(" || ''',''' || ");
							dollarCount++;
							tuple.addString(linkedEntityType);

						}
						result.setCharAt(result.length() - 1, ']');
						result.append(')');
						followUp.setLength(followUp.length() - 8);
						followUp.append("])");
					}

				} else if (!linkedEntityTypes.isEmpty()) {
					result.append(" AND entity.e_types && ARRAY[");
					followUp.append(" AND entity.e_types && ARRAY[''' ||");
					for (String linkedEntityType : linkedEntityTypes) {
						result.append('$');
						result.append(dollarCount);
						result.append(',');
						dollarCount++;
						tuple.addString(linkedEntityType);
						followUp.append('$');
						followUp.append(dollarCount);
						followUp.append(" || ''',''' || ");

					}
					result.setCharAt(result.length() - 1, ']');
					followUp.setLength(followUp.length() - 8);
					followUp.append("]");
				}

			}
			if (firstChild != null) {
				result.append(" AND ");
				followUp.append(" AND ");
				try {
					firstChild.setOperant(operant);
				} catch (ResponseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				firstChild.setOperator(operator);
				firstChild.setExpandedOpt(expandedOpt);
				dollarCount = firstChild.toSql(result, followUp, dollarCount, tuple, isDist, localOnly);

			}
			result.append(')');
			followUp.append(")");
//			result.append(')');
//			followUp.append(')');

		} else {
			wildcardUse = NGSIConstants.NGSI_LD_STAR.equals(attribName);
			if (!wildcardUse) {
				result.append("ENTITY ? $");
				result.append(dollarCount);

				followUp.append("ENTITY ? ''' || $");
				followUp.append(dollarCount);
				followUp.append("|| '''");
			}
			if (attribName.equals("@id")) {
				result.append(" AND entity ->> $");
				result.append(dollarCount);

				followUp.append(" AND entity ->> ''' || $");
				followUp.append(dollarCount);
				followUp.append(" || '''");
				dollarCount++;
				tuple.addString(attribName);
				result.append(" ~ $");
				result.append(dollarCount);

				followUp.append(" ~ ''' || $");
				followUp.append(dollarCount);
				followUp.append(" || '''");

				dollarCount++;
				tuple.addString(operant);
			} else {
				if (!wildcardUse) {
					followUp.append(" AND ");
					result.append(" AND ");
				}
				if (operator.equals(NGSIConstants.QUERY_UNEQUAL)) {
					result.append("NOT ");
					followUp.append("NOT ");
				}

				if (wildcardUse) {
					result.append(
							"EXISTS (SELECT TRUE FROM JSONB_OBJECT_KEYS(ENTITY) AS ATTRKEY, JSONB_ARRAY_ELEMENTS(ENTITY -> ATTRKEY) AS toplevel WHERE ATTRKEY NOT IN ('");
					result.append(NGSIConstants.JSON_LD_ID);
					result.append("','");
					result.append(NGSIConstants.JSON_LD_TYPE);
					result.append("','");
					result.append(NGSIConstants.NGSI_LD_MODIFIED_AT);
					result.append("','");
					result.append(NGSIConstants.NGSI_LD_CREATED_AT);
					result.append("') ");

					followUp.append(
							"EXISTS (SELECT TRUE FROM JSONB_OBJECT_KEYS(ENTITY) AS ATTRKEY, JSONB_ARRAY_ELEMENTS(ENTITY -> ATTRKEY) AS toplevel WHERE ATTRKEY NOT IN (''");
					followUp.append(NGSIConstants.JSON_LD_ID);
					followUp.append("'',''");
					followUp.append(NGSIConstants.JSON_LD_TYPE);
					followUp.append("'',''");
					followUp.append(NGSIConstants.NGSI_LD_MODIFIED_AT);
					followUp.append("'',''");
					followUp.append(NGSIConstants.NGSI_LD_CREATED_AT);
					followUp.append("'') ");

				} else {
					result.append("EXISTS (SELECT TRUE FROM JSONB_ARRAY_ELEMENTS(ENTITY -> $");
					followUp.append("EXISTS (SELECT TRUE FROM JSONB_ARRAY_ELEMENTS(ENTITY -> ''' || $");
					result.append(dollarCount);
					followUp.append(dollarCount);
					followUp.append(" || '''");
					dollarCount++;
					tuple.addString(attribName);
					result.append(") AS toplevel ");
					followUp.append(") AS toplevel ");
				}

				if ((operator != null && !operator.isEmpty()) || attribPath.length > 1
						|| (subAttribPath != null && subAttribPath.length > 0)) {
					if (wildcardUse) {
						result.append("AND ");
						followUp.append("AND ");
					} else {
						result.append("WHERE ");
						followUp.append("WHERE ");
					}
					dollarCount = commonWherePart(attribPath, subAttribPath, "toplevel", dollarCount, tuple, result,
							followUp, this);
				} else {
					result.append(')');
					followUp.append(')');
				}
			}
		}
		return dollarCount;
	}

	public int toSql(StringBuilder result, int dollarCount, Tuple tuple, boolean isDist, boolean localOnly) {
		if (firstChild != null) {
			result.append("(");
			dollarCount = firstChild.toSql(result, dollarCount, tuple, isDist, localOnly);
			result.append(")");
		} else {
			dollarCount = getAttribQuery(result, dollarCount, tuple, isDist, localOnly);
		}
		if (hasNext()) {
			if (nextAnd) {
				result.append(" and ");
			} else {
				result.append(" or ");
			}
			dollarCount = next.toSql(result, dollarCount, tuple, isDist, localOnly);
		}
		return dollarCount;
	}

	public int toSql(StringBuilder result, StringBuilder followUp, int dollarCount, Tuple tuple, boolean isDist,
			boolean localOnly) {
		if (firstChild != null && !isLinkedQ) {
			result.append("(");
			dollarCount = firstChild.toSql(result, followUp, dollarCount, tuple, isDist, localOnly);
			result.append(")");
		} else {
			dollarCount = getAttribQuery(result, followUp, dollarCount, tuple, isDist, localOnly);
		}
		if (hasNext()) {
			if (nextAnd) {
				result.append(" and ");
			} else {
				result.append(" or ");
			}
			dollarCount = next.toSql(result, followUp, dollarCount, tuple, isDist, localOnly);
		}
		return dollarCount;
	}

//	private ArrayList<String> getAttribPathArray(String attribute) {
//		ArrayList<String> attribPath = new ArrayList<String>();
//		if (attribute.contains("[") && attribute.contains(".")) {
//			if (attribute.contains(".")) {
//				for (String subPart : attribute.split("\\.")) {
//					if (subPart.contains("[")) {
//						for (String subParts : subPart.split("\\[")) {
//							// subParts = subParts.replaceAll("\\]", "");
//							attribPath.add(expandAttributeName(subParts));
//						}
//					} else {
//						attribPath.add(expandAttributeName(subPart));
//					}
//				}
//			}
//		} else if (attribute.contains("[")) {
//			for (String subPart : attribute.split("\\[")) {
//				subPart = subPart.replaceAll("\\]", "");
//				attribPath.addAll(getAttribPathArray(subPart));
//			}
//		} else if (attribute.matches(URI)) {
//			attribPath.add(expandAttributeName(attribute));
//		} else if (attribute.contains(".")) {
//			for (String subPart : attribute.split("\\.")) {
//				attribPath.addAll(getAttribPathArray(subPart));
//			}
//		} else {
//			attribPath.add(expandAttributeName(attribute));
//		}
//		return attribPath;
//	}

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

	private int addItemToTupel(Tuple tuple, String listItem, StringBuilder sql, StringBuilder followUp,
			int dollarCount) {
		try {
			double tmp = Double.parseDouble(listItem);
			sql.append("TO_JSONB($");
			sql.append(dollarCount);
			sql.append(")");

			followUp.append("TO_JSONB( ' || $");
			followUp.append(dollarCount);
			followUp.append(" || ')");

			tuple.addDouble(tmp);
			dollarCount++;
		} catch (NumberFormatException e) {
			if (listItem.equalsIgnoreCase("true") || listItem.equalsIgnoreCase("false")) {

				sql.append("$");
				sql.append(dollarCount);
				sql.append("::jsonb");
				followUp.append("' || $");
				followUp.append(dollarCount);
				followUp.append(" || '::jsonb");
				dollarCount++;
				tuple.addBoolean(Boolean.parseBoolean(listItem));
			} else {
				sql.append("$");
				sql.append(dollarCount);

				followUp.append("''' || $");
				followUp.append(dollarCount);
				followUp.append(" || '''");
				dollarCount++;

				// if (!listItem.matches(DATETIME)) {
				if (listItem.charAt(0) != '"' || listItem.charAt(listItem.length() - 1) != '"') {
					listItem = '"' + listItem + '"';
				}

				sql.append("::text::jsonb");
				followUp.append("::text::jsonb");

				// }

				tuple.addString(listItem);
			}
		}
		return dollarCount;

	}

	private int applyOperator(StringBuilder attributeFilterProperty, StringBuilder followUp, int dollarCount,
			Tuple tuple, Boolean needExpanded) {
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
					// attributeFilterProperty.append(" not");
					// followUp.append(" not");
				}
				attributeFilterProperty.append(" in (");
				for (String listItem : finalOperant.split(",")) {
					dollarCount++;
					dollarCount = addItemToTupel(tuple, listItem, attributeFilterProperty, followUp, dollarCount);
					attributeFilterProperty.append(',');

					followUp.append(',');

				}
				attributeFilterProperty.setCharAt(attributeFilterProperty.length() - 1, ')');
				followUp.setCharAt(followUp.length() - 1, ')');
			} else if (finalOperant.matches(RANGE)) {
				String[] myRange = finalOperant.split("\\.\\.");
				if (operator.equals(NGSIConstants.QUERY_UNEQUAL)) {
					// attributeFilterProperty.append(" not");
					// followUp.append(" not");
				}
				attributeFilterProperty.append(" between ");

				followUp.append(" between ");

				dollarCount = addItemToTupel(tuple, myRange[0], attributeFilterProperty, followUp, dollarCount);
				attributeFilterProperty.append(" and ");
				followUp.append(" and ");

				dollarCount = addItemToTupel(tuple, myRange[1], attributeFilterProperty, followUp, dollarCount);
				attributeFilterProperty.append("::" + typecast);
				followUp.append("::" + typecast);
			} else {
				if (operator.equals(NGSIConstants.QUERY_UNEQUAL)) {
					attributeFilterProperty.append(" = ");
					followUp.append(" = ");
				} else {
					attributeFilterProperty.append(" = ");
					followUp.append(" = ");
				}

				dollarCount = addItemToTupelForEqualAndUnequal(tuple, finalOperant, attributeFilterProperty, followUp,
						dollarCount);

			}

			break;
		case NGSIConstants.QUERY_GREATEREQ:
			attributeFilterProperty.append(" >= ");

			followUp.append(" >= ");

			dollarCount = addItemToTupel(tuple, finalOperant, attributeFilterProperty, followUp, dollarCount);

			break;
		case NGSIConstants.QUERY_LESSEQ:
			attributeFilterProperty.append(" <= ");
			followUp.append(" <= ");
			dollarCount = addItemToTupel(tuple, finalOperant, attributeFilterProperty, followUp, dollarCount);

			break;
		case NGSIConstants.QUERY_GREATER:
			attributeFilterProperty.append(" > ");
			followUp.append(" > ");
			dollarCount = addItemToTupel(tuple, finalOperant, attributeFilterProperty, followUp, dollarCount);
			break;
		case NGSIConstants.QUERY_LESS:
			attributeFilterProperty.append(" < ");
			followUp.append(" < ");
			dollarCount = addItemToTupel(tuple, finalOperant, attributeFilterProperty, followUp, dollarCount);
			break;
		case NGSIConstants.QUERY_PATTERNOP:
			attributeFilterProperty.append("::text ~ $");
			attributeFilterProperty.append(dollarCount);
			followUp.append("::text ~ ''' || $");
			followUp.append(dollarCount);
			followUp.append(" || '''");
			// attributeFilterProperty.append("'");
			dollarCount++;
			tuple.addString(finalOperant);
			// addItemToTupel(tuple, operant);
			break;
		case NGSIConstants.QUERY_NOTPATTERNOP:
			attributeFilterProperty.append("::text !~ $");
			attributeFilterProperty.append(dollarCount);
			followUp.append("::text !~ ''' || $");
			followUp.append(dollarCount);
			followUp.append(" || '''");
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
			sql.insert(sql.lastIndexOf("$"), "TO_JSONB(");
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

	private int addItemToTupelForEqualAndUnequal(Tuple tuple, String listItem, StringBuilder sql,
			StringBuilder followUp, int dollarCount) {
		String strTBU;
		if (listItem.charAt(0) != '"' || listItem.charAt(listItem.length() - 1) != '"') {
			strTBU = '"' + listItem + '"';
		} else {
			strTBU = listItem;
		}
		sql.append("ANY(ARRAY[");
		followUp.append("ANY(ARRAY[");
		try {
			double tmp = Double.parseDouble(listItem);
			sql.append("TO_JSONB($");
			sql.append(dollarCount);
			sql.append(")");

			followUp.append("TO_JSONB(' || $");
			followUp.append(dollarCount);
			followUp.append(" || ')");

			tuple.addDouble(tmp);
			dollarCount++;

			sql.append(",$");
			sql.append(dollarCount);
			sql.append("::text::jsonb");

			followUp.append(",''' || $");
			followUp.append(dollarCount);
			followUp.append(" || '''::text::jsonb");

			dollarCount++;
			tuple.addString(strTBU);
		} catch (NumberFormatException e) {
			if (listItem.equalsIgnoreCase("true") || listItem.equalsIgnoreCase("false")) {

				sql.append("$");
				sql.append(dollarCount);

				followUp.append("' || $");
				followUp.append(dollarCount);
				followUp.append(" || '");
				dollarCount++;
				tuple.addBoolean(Boolean.parseBoolean(listItem));

				sql.append(",$");
				sql.append(dollarCount);
				sql.append("::text::jsonb");

				followUp.append(",' || $");
				followUp.append(dollarCount);
				followUp.append(" || '::text::jsonb");

				dollarCount++;
				tuple.addString(strTBU);
			} else {
				sql.append("$");
				sql.append(dollarCount);

				followUp.append("''' || $");
				followUp.append(dollarCount);
				followUp.append(" || '''");
				dollarCount++;

				// if (!listItem.matches(DATETIME)) {

				sql.append("::text::jsonb");
				followUp.append("::text::jsonb");

				// }

				tuple.addString(strTBU);
			}
		}
		sql.append("])");
		followUp.append("])");
		return dollarCount;

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
				if (current.getOperator().equals(NGSIConstants.QUERY_PATTERNOP)) {
					sql.append("->>'");
				} else {
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
				if (current.getOperator().equals(NGSIConstants.QUERY_PATTERNOP)) {
					sql.append("LANGPROP ->> '");
				} else {
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

	private int commonWherePart(String[] attribPath, String[] subAttribPath, String currentSqlAttrib, int dollarCount,
			Tuple tuple, StringBuilder sql, StringBuilder followUp, QQueryTerm current) {
		char currentChar = 'a';
		String prefix = "dataarray";

		for (int i = 1; i < attribPath.length; i++) {
			sql.append("EXISTS(SELECT TRUE FROM JSONB_ARRAY_ELEMENTS(");
			sql.append(currentSqlAttrib);
			sql.append(" -> $");
			sql.append(dollarCount);

			followUp.append("EXISTS(SELECT TRUE FROM JSONB_ARRAY_ELEMENTS(");
			followUp.append(currentSqlAttrib);
			followUp.append(" -> ''' || $");
			followUp.append(dollarCount);
			followUp.append(" || '''");

			tuple.addString(linkHeaders.expandIri(attribPath[i], false, true, null, null));
			dollarCount++;
			currentSqlAttrib = prefix + currentChar;
			currentChar++;
			sql.append(") AS ");
			sql.append(currentSqlAttrib);
			sql.append(" WHERE ");

			followUp.append(") AS ");
			followUp.append(currentSqlAttrib);
			followUp.append(" WHERE ");
		}

		if (subAttribPath == null) {

			if (!current.getOperator().isEmpty()) {
				sql.append(" CASE WHEN (");
				sql.append(currentSqlAttrib);
				sql.append(" #>'{");
				sql.append(NGSIConstants.JSON_LD_ID);
				sql.append("}') ");

				followUp.append(" CASE WHEN (");
				followUp.append(currentSqlAttrib);
				followUp.append(" #>''{");
				followUp.append(NGSIConstants.JSON_LD_ID);
				followUp.append("}'') ");

				dollarCount = applyOperator(sql, followUp, dollarCount, tuple, false);
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

				followUp.append(" THEN true");

				followUp.append(" WHEN ");
				followUp.append(currentSqlAttrib);
				followUp.append(" #>>''{");
				followUp.append(NGSIConstants.JSON_LD_TYPE);
				followUp.append(",0}'' = ''");
				followUp.append(NGSIConstants.NGSI_LD_PROPERTY);
				followUp.append("'' THEN EXISTS (SELECT TRUE FROM JSONB_ARRAY_ELEMENTS(");
				followUp.append(currentSqlAttrib);
				followUp.append(" ->''");
				followUp.append(NGSIConstants.NGSI_LD_HAS_VALUE);
				followUp.append("'') AS mostInnerValue WHERE (mostInnerValue->''");
				followUp.append(NGSIConstants.JSON_LD_VALUE);
				followUp.append("'')");

				dollarCount = applyOperator(sql, followUp, dollarCount, tuple, false);
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

				followUp.append(") WHEN ");

				followUp.append(currentSqlAttrib);
				followUp.append(" #>>''{");
				followUp.append(NGSIConstants.JSON_LD_TYPE);
				followUp.append(",0}'' = ''");
				followUp.append(NGSIConstants.NGSI_LD_RELATIONSHIP);
				followUp.append("'' THEN EXISTS (SELECT TRUE FROM JSONB_ARRAY_ELEMENTS(");
				followUp.append(currentSqlAttrib);
				followUp.append(" ->''");
				followUp.append(NGSIConstants.NGSI_LD_HAS_OBJECT);
				followUp.append("'') AS mostInnerValue WHERE (mostInnerValue->''");
				followUp.append(NGSIConstants.JSON_LD_ID);
				followUp.append("'')");

				dollarCount = applyOperator(sql, followUp, dollarCount, tuple, false);
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

				followUp.append(") WHEN ");

				followUp.append(currentSqlAttrib);
				followUp.append(" #>>''{");
				followUp.append(NGSIConstants.JSON_LD_TYPE);
				followUp.append(",0}'' = ''");
				followUp.append(NGSIConstants.NGSI_LD_VocabProperty);
				followUp.append("'' THEN EXISTS (SELECT TRUE FROM JSONB_ARRAY_ELEMENTS(");
				followUp.append(currentSqlAttrib);
				followUp.append(" ->''");
				followUp.append(NGSIConstants.NGSI_LD_HAS_VOCAB);
				followUp.append("'') AS mostInnerValue WHERE (mostInnerValue->''");
				followUp.append(NGSIConstants.JSON_LD_ID);
				followUp.append("'')");
				dollarCount = applyOperator(sql, followUp, dollarCount, tuple, true);
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

				followUp.append(") WHEN ");

				followUp.append(currentSqlAttrib);
				followUp.append(" #>>''{");
				followUp.append(NGSIConstants.JSON_LD_TYPE);
				followUp.append(",0}'' = ''");
				followUp.append(NGSIConstants.NGSI_LD_ListProperty);
				followUp.append("'' THEN EXISTS (SELECT TRUE FROM JSONB_ARRAY_ELEMENTS(");
				followUp.append(currentSqlAttrib);
				followUp.append(" ->''");
				followUp.append(NGSIConstants.NGSI_LD_HAS_LIST);
				followUp.append("'') AS mostInnerValue WHERE (mostInnerValue->''");
				followUp.append(NGSIConstants.JSON_LD_ID);
				followUp.append("'')");
				dollarCount = applyOperator(sql, followUp, dollarCount, tuple, true);
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

				followUp.append(") WHEN ");

				followUp.append(currentSqlAttrib);
				followUp.append(" #>>''{");
				followUp.append(NGSIConstants.JSON_LD_TYPE);
				followUp.append(",0}'' = ''");
				followUp.append(NGSIConstants.NGSI_LD_LISTRELATIONSHIP);
				followUp.append("'' THEN EXISTS (SELECT TRUE FROM JSONB_ARRAY_ELEMENTS(");
				followUp.append(currentSqlAttrib);
				followUp.append(" ->''");
				followUp.append(NGSIConstants.NGSI_LD_HAS_OBJECT_LIST);
				followUp.append("'') AS mostInnerValue WHERE (mostInnerValue->''");
				followUp.append(NGSIConstants.JSON_LD_ID);
				followUp.append("'')");
				dollarCount = applyOperator(sql, followUp, dollarCount, tuple, false);
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

				followUp.append(") WHEN ");

				followUp.append(currentSqlAttrib);
				followUp.append(" #>>''{");
				followUp.append(NGSIConstants.JSON_LD_TYPE);
				followUp.append("}'' = ''");
				followUp.append(NGSIConstants.NGSI_LD_DATE_TIME);
				followUp.append("'' THEN (");
				followUp.append(currentSqlAttrib);
				followUp.append(" ->''");
				followUp.append(NGSIConstants.JSON_LD_VALUE);
				followUp.append("'')");

				dollarCount = applyOperator(sql, followUp, dollarCount, tuple, false);
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

				followUp.append(" WHEN ");
				followUp.append(currentSqlAttrib);
				followUp.append(" #>>''{");
				followUp.append(NGSIConstants.JSON_LD_TYPE);
				followUp.append(",0}'' = ''");
				followUp.append(NGSIConstants.NGSI_LD_DATE_TIME);
				followUp.append("'' THEN (");
				followUp.append(currentSqlAttrib);
				followUp.append(" ->''");
				followUp.append(NGSIConstants.JSON_LD_VALUE);
				followUp.append("'')");

				dollarCount = applyOperator(sql, followUp, dollarCount, tuple, false);

				sql.append(" ELSE FALSE END ");
				followUp.append(" ELSE FALSE END ");

			} else {
				sql.setLength(sql.length() - " WHERE ".length());
				followUp.setLength(followUp.length() - " WHERE ".length());
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

			followUp.append(" CASE WHEN ");
			followUp.append(currentSqlAttrib);
			followUp.append(" #>>''{");
			followUp.append(NGSIConstants.JSON_LD_TYPE);
			followUp.append(",0}'' = ''");
			followUp.append(NGSIConstants.NGSI_LD_PROPERTY);
			followUp.append("'' THEN EXISTS (SELECT TRUE FROM JSONB_ARRAY_ELEMENTS(");
			followUp.append(currentSqlAttrib);
			followUp.append(" ->''");
			followUp.append(NGSIConstants.NGSI_LD_HAS_VALUE);
			followUp.append("'') AS mostInnerValue");

			String currentSqlAttrib2 = "mostInnerValue";
			prefix = "mostInnerValue";
			currentChar = 'a';
			for (int i = 0; i < subAttribPath.length; i++) {
				sql.append(" WHERE EXISTS(SELECT TRUE FROM JSONB_ARRAY_ELEMENTS(");
				sql.append(currentSqlAttrib2);
				sql.append(" -> $");
				sql.append(dollarCount);

				followUp.append(" WHERE EXISTS(SELECT TRUE FROM JSONB_ARRAY_ELEMENTS(");
				followUp.append(currentSqlAttrib2);
				followUp.append(" -> ''' || $");
				followUp.append(dollarCount);
				followUp.append(" || '''");
				tuple.addString(linkHeaders.expandIri(subAttribPath[i], false, true, null, null));
				dollarCount++;
				currentSqlAttrib2 = prefix + currentChar;
				currentChar++;
				sql.append(") AS " + currentSqlAttrib2);
				followUp.append(") AS " + currentSqlAttrib2);
			}

			if (!current.getOperator().isEmpty()) {
				sql.append(" WHERE ");
				sql.append(currentSqlAttrib2);

				followUp.append(" WHERE ");
				followUp.append(currentSqlAttrib2);
				if (current.getOperator().equals(NGSIConstants.QUERY_PATTERNOP)) {
					sql.append("->>'");
					followUp.append("->>''");
				} else {
					sql.append("->'");
					followUp.append("->''");
				}
				sql.append(NGSIConstants.JSON_LD_VALUE);
				sql.append("'");
				followUp.append(NGSIConstants.JSON_LD_VALUE);
				followUp.append("''");
				dollarCount = applyOperator(sql, followUp, dollarCount, tuple, false);
			}

			for (int i = 0; i < subAttribPath.length; i++) {
				sql.append(") ");
				followUp.append(") ");
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

			followUp.append(") WHEN ");
			followUp.append(currentSqlAttrib);
			followUp.append(" #>>''{");
			followUp.append(NGSIConstants.JSON_LD_TYPE);
			followUp.append(",0}'' = ''");
			followUp.append(NGSIConstants.NGSI_LD_LANGPROPERTY);
			followUp.append("'' THEN EXISTS (SELECT TRUE FROM JSONB_ARRAY_ELEMENTS(");
			followUp.append(currentSqlAttrib);
			followUp.append(" ->''");
			followUp.append(NGSIConstants.NGSI_LD_HAS_LANGUAGE_MAP);
			followUp.append("'') AS LANGPROP");
			if (!current.getOperator().isEmpty()) {
				sql.append(" WHERE ");
				followUp.append(" WHERE ");
				if (!subAttribPath[0].equals("*")) {
					sql.append("LANGPROP ->> '@language'=$");
					sql.append(dollarCount);

					followUp.append("LANGPROP ->> ''@language''= ''' || $");
					followUp.append(dollarCount);
					followUp.append("|| '''");

					dollarCount++;
					tuple.addString(subAttribPath[0]);
					sql.append(" AND ");
					followUp.append(" AND ");
				}
				if (current.getOperator().equals(NGSIConstants.QUERY_PATTERNOP)) {
					sql.append("LANGPROP ->> '");
					followUp.append("LANGPROP ->> ''");
				} else {
					sql.append("LANGPROP -> '");
					followUp.append("LANGPROP -> ''");
				}
				sql.append(NGSIConstants.JSON_LD_VALUE);
				sql.append("'");
				followUp.append(NGSIConstants.JSON_LD_VALUE);
				followUp.append("''");
				dollarCount = current.applyOperator(sql, followUp, dollarCount, tuple, false);
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

			followUp.append(") WHEN ");
			followUp.append(currentSqlAttrib);
			followUp.append(" #>>''{");
			followUp.append(NGSIConstants.JSON_LD_TYPE);
			followUp.append(",0}'' = ''");
			followUp.append(NGSIConstants.NGSI_LD_JSON_PROPERTY);
			followUp.append("'' THEN EXISTS (SELECT TRUE FROM JSONB_ARRAY_ELEMENTS(");
			followUp.append(currentSqlAttrib);
			followUp.append(" ->''");
			followUp.append(NGSIConstants.NGSI_LD_HAS_JSON);
			followUp.append("'') AS ");
			followUp.append(currentSqlAttrib2);
			followUp.append(" WHERE ");
			followUp.append(currentSqlAttrib2);
			followUp.append(" -> ''");
			followUp.append(NGSIConstants.JSON_LD_VALUE);
			followUp.append("'' ->> ''' || $");
			followUp.append(dollarCount);
			followUp.append("|| '''");

			dollarCount++;
			tuple.addString(subAttribPath[0]);
			sql.append("=");
			sql.append(" $");
			sql.append(dollarCount);

			followUp.append("=");
			followUp.append(" ''$");
			followUp.append(dollarCount);
			followUp.append("''");
			dollarCount++;
			tuple.addString(current.operant);
			sql.append(")  ELSE FALSE END ");
			followUp.append(")  ELSE FALSE END ");
		}
		sql.append(") ");
		followUp.append(") ");
		for (int i = 1; i < attribPath.length; i++) {
			sql.append(") ");
			followUp.append(") ");
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

	public boolean hasLinkedQ() {
		return hasLinkedQ;
	}

	public void setHasLinkedQ(boolean hasLinkedQ) {
		this.hasLinkedQ = hasLinkedQ;
	}

	public Map<String, Map<String, Object>> calculateQuery(List<Map<String, Object>> resultData,
			EntityCache updatedEntityCache, Set<String> jsonKeys, boolean localOnly) {
		Iterator<Map<String, Object>> it = resultData.iterator();
		Map<String, Map<String, Object>> deleted = Maps.newHashMap();
		while (it.hasNext()) {
			Map<String, Object> entity = it.next();
			if (!calculateEntity(entity, updatedEntityCache, jsonKeys, localOnly)) {
				deleted.put((String) entity.get(NGSIConstants.JSON_LD_ID), entity);
				it.remove();
			}
		}
		return deleted;
	}

	public boolean calculateEntity(Map<String, Object> entity, EntityCache updatedEntityCache, Set<String> jsonKeys,
			boolean localOnly) {
		if (this.isLinkedQ) {
			return calculateLinkedEntity(entity, updatedEntityCache, jsonKeys, localOnly);
		} else {
			boolean result = false;
			if (firstChild == null) {
				result = calculate(entity, attribute, operator, operant, jsonKeys);
			} else {
				result = firstChild.calculateEntity(entity, updatedEntityCache, jsonKeys, localOnly);
			}
			if (hasNext()) {
				if (nextAnd) {
					result = result && next.calculateEntity(entity, updatedEntityCache, jsonKeys, localOnly);
				} else {
					result = result || next.calculateEntity(entity, updatedEntityCache, jsonKeys, localOnly);
				}
			}

			return result;
		}

	}

	@SuppressWarnings("unchecked")
	private boolean calculateLinkedEntity(Map<String, Object> entity, EntityCache updatedEntityCache,
			Set<String> jsonKeys, boolean localOnly) {
		Object attrObj = entity.get(this.linkedAttrName);
		if (attrObj != null && attrObj instanceof List<?> attrList) {
			for (Object listAttrObj : attrList) {
				if (listAttrObj instanceof Map<?, ?> listAttrMap) {
					Object typeObj = listAttrMap.get(NGSIConstants.JSON_LD_TYPE);
					if (typeObj != null && typeObj instanceof List<?> typeList) {
						if (typeList.contains(NGSIConstants.NGSI_LD_RELATIONSHIP)) {
							List<Map<String, String>> hasObject = (List<Map<String, String>>) listAttrMap
									.get(NGSIConstants.NGSI_LD_HAS_OBJECT);
							Object objectTypeObj = listAttrMap.get(NGSIConstants.NGSI_LD_OBJECT_TYPE);
							for (Map<String, String> objectEntry : hasObject) {
								String entityId = objectEntry.get(NGSIConstants.JSON_LD_ID);
								QQueryTerm linkedQ = this.getFirstChild();
								if (localOnly) {

									Tuple2<Map<String, Object>, Set<String>> t = updatedEntityCache.get(entityId);
									if (t != null) {
										Map<String, Object> linkedEntity = t.getItem1();
										if (linkedEntity != null && linkedQ.calculateEntity(linkedEntity,
												updatedEntityCache, jsonKeys, localOnly)) {
											return true;
										}
									}

								} else {
									if (objectTypeObj != null && objectTypeObj instanceof List<?> objectTypes) {
										for (Object objectTypeEntry : objectTypes) {
											if (objectTypeEntry instanceof Map<?, ?> objectType) {
												String type = (String) objectType.get(NGSIConstants.JSON_LD_ID);
												if (linkedEntityTypes.isEmpty() || linkedEntityTypes.contains(type)) {
													Tuple2<Map<String, Object>, Set<String>> entity2CsourceIds = updatedEntityCache
															.get(entityId);
													if (entity2CsourceIds != null) {
														Map<String, Object> linkedEntity = entity2CsourceIds.getItem1();
														if (linkedEntity != null) {
//															List<String> linkedEntityType = (List<String>) linkedEntity
//																	.get(NGSIConstants.JSON_LD_TYPE);

															if (linkedQ.calculateEntity(linkedEntity,
																	updatedEntityCache, jsonKeys, localOnly)) {
																return true;
															}
														}
													}
												}
											}
										}
									}
								}
							}

						} else if (typeList.contains(NGSIConstants.NGSI_LD_LISTRELATIONSHIP)) {
							List<Map<String, List<Map<String, List<Map<String, String>>>>>> hasObjectList = (List<Map<String, List<Map<String, List<Map<String, String>>>>>>) listAttrMap
									.get(NGSIConstants.NGSI_LD_HAS_OBJECT_LIST);
							Object objectTypeObj = listAttrMap.get(NGSIConstants.NGSI_LD_OBJECT_TYPE);
							for (Map<String, List<Map<String, List<Map<String, String>>>>> atListEntry : hasObjectList) {
								List<Map<String, List<Map<String, String>>>> atList = atListEntry
										.get(NGSIConstants.JSON_LD_LIST);
								for (Map<String, List<Map<String, String>>> hasObjectEntry : atList) {
									List<Map<String, String>> objectList = hasObjectEntry
											.get(NGSIConstants.NGSI_LD_HAS_OBJECT);
									for (Map<String, String> objectEntry : objectList) {
										String entityId = objectEntry.get(NGSIConstants.JSON_LD_ID);
										QQueryTerm linkedQ = this.getFirstChild();
										if (localOnly) {
											Tuple2<Map<String, Object>, Set<String>> t = updatedEntityCache
													.get(entityId);
											if (t != null) {
												Map<String, Object> linkedEntity = t.getItem1();
												if (linkedEntity != null && linkedQ.calculateEntity(linkedEntity,
														updatedEntityCache, jsonKeys, localOnly)) {
													return true;
												}
											}

										} else {
											if (objectTypeObj != null && objectTypeObj instanceof List<?> objectTypes) {
												for (Object objectTypeEntry : objectTypes) {
													if (objectTypeEntry instanceof Map<?, ?> objectType) {
														String type = (String) objectType.get(NGSIConstants.JSON_LD_ID);
														if (linkedEntityTypes.isEmpty()
																|| linkedEntityTypes.contains(type)) {
															Tuple2<Map<String, Object>, Set<String>> entity2CsourceIds = updatedEntityCache
																	.get(entityId);
															if (entity2CsourceIds != null) {
																Map<String, Object> linkedEntity = entity2CsourceIds
																		.getItem1();
																if (linkedEntity != null) {
//																	List<String> linkedEntityType = (List<String>) linkedEntity
//																			.get(NGSIConstants.JSON_LD_TYPE);

																	if (linkedQ.calculateEntity(linkedEntity,
																			updatedEntityCache, jsonKeys, localOnly)) {
																		return true;
																	}
																}
															}
														}
													}
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}
		}
		return false;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String toQueryParam(Context context) {
		String result;
		if (firstChild != null) {
			result = "(" + firstChild.toQueryParam(context) + ")";
		} else {
			result = URLEncoder.encode(context.compactIri(attribute), StandardCharsets.UTF_8);
			if (isLinkedQ) {

			} else {

			}
			if (operant != null && operator != null) {
				result += operator + operant;
			}
			if (hasNext()) {
				if (isNextAnd()) {
					result += ';';
				} else {
					result += '|';
				}
				result += next.toQueryParam(context);
			}
		}
		return result;
	}

}
