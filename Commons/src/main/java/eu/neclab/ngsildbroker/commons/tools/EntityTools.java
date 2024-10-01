package eu.neclab.ngsildbroker.commons.tools;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLDService;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.common.net.HttpHeaders;

import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.BaseProperty;
import eu.neclab.ngsildbroker.commons.datatypes.EntityCache;
import eu.neclab.ngsildbroker.commons.datatypes.GeoProperty;
import eu.neclab.ngsildbroker.commons.datatypes.LDGeoQuery;
import eu.neclab.ngsildbroker.commons.datatypes.Notification;
import eu.neclab.ngsildbroker.commons.datatypes.Property;
import eu.neclab.ngsildbroker.commons.datatypes.PropertyEntry;
import eu.neclab.ngsildbroker.commons.datatypes.QueryInfos;
import eu.neclab.ngsildbroker.commons.datatypes.QueryRemoteHost;
import eu.neclab.ngsildbroker.commons.datatypes.RegistrationEntry;
import eu.neclab.ngsildbroker.commons.datatypes.RemoteHost;
import eu.neclab.ngsildbroker.commons.datatypes.ViaHeaders;
import eu.neclab.ngsildbroker.commons.datatypes.results.QueryResult;
import eu.neclab.ngsildbroker.commons.datatypes.terms.AttrsQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.DataSetIdTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.GeoQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.LanguageQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.OmitTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.PickTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.QQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.ScopeQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.TypeQueryTerm;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple3;
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.ext.web.client.HttpRequest;
import io.vertx.mutiny.ext.web.client.HttpResponse;
import io.vertx.mutiny.ext.web.client.WebClient;

@SuppressWarnings("unchecked")
public final class EntityTools {

	private EntityTools() {

	}

	private static final String BROKER_PREFIX = "ngsildbroker:";

	public static final Set<String> DO_NOT_MERGE_KEYS = Sets.newHashSet(NGSIConstants.JSON_LD_ID,
			NGSIConstants.JSON_LD_TYPE, NGSIConstants.NGSI_LD_CREATED_AT, NGSIConstants.NGSI_LD_OBSERVED_AT,
			NGSIConstants.NGSI_LD_MODIFIED_AT);
	private static final Logger logger = LoggerFactory.getLogger(EntityTools.class);

	public static String getRandomID(String prefix) {
		if (prefix == null) {
			prefix = ":";
		}
		if (!prefix.endsWith(":")) {
			prefix += ":";
		}
		return BROKER_PREFIX + prefix + UUID.randomUUID().getLeastSignificantBits();

	}

	public static Uni<Map<String, Object>> prepareSplitUpEntityForSending(Map<String, Object> expanded, Context context,
			JsonLDService ldService) {
		if (expanded.containsKey(NGSIConstants.JSON_LD_TYPE)) {
			expanded.put(NGSIConstants.JSON_LD_TYPE,
					Lists.newArrayList((Set<String>) expanded.get(NGSIConstants.JSON_LD_TYPE)));
		}
		if (expanded.containsKey(NGSIConstants.NGSI_LD_SCOPE)) {
			Set<String> collectedScopes = (Set<String>) expanded.get(NGSIConstants.NGSI_LD_SCOPE);
			List<Map<String, String>> finalScopes = Lists.newArrayList();
			for (String scope : collectedScopes) {
				finalScopes.add(Map.of(NGSIConstants.JSON_LD_VALUE, scope));
			}
			expanded.put(NGSIConstants.NGSI_LD_SCOPE, finalScopes);
		}
		return ldService.compact(expanded, null, context, HttpUtils.opts, -1);

	}

	public static Notification squashNotifications(List<Notification> data) {
		List<Map<String, Object>> newData = new ArrayList<Map<String, Object>>();
		Set<Object> context = new HashSet<Object>();
		for (Notification notification : data) {
			newData.addAll(notification.getData());
			context.addAll(notification.getContext());
		}
		return new Notification(data.get(0).getId(), data.get(0).getType(), System.currentTimeMillis(),
				data.get(0).getSubscriptionId(), newData, data.get(0).getTriggerReason(),
				new ArrayList<Object>(context), data.get(0).getHeaders());
	}


	public static GeoProperty getLocation(Map<String, Object> fullEntry, LDGeoQuery ldGeoQuery) {
		String locationName = NGSIConstants.NGSI_LD_LOCATION;
		if (ldGeoQuery != null) {
			locationName = ldGeoQuery.getGeoProperty();
		}
		Object obj = fullEntry.get(locationName);
		if (obj == null) {
			return null;
		}
		return SerializationTools.parseGeoProperty((List<Map<String, Object>>) obj, locationName);
	}

	

	public static Set<String> getRegisteredTypes(Map<String, Object> cSourceRegistration) {
		List<Map<String, Object>> entities = (List<Map<String, Object>>) ((List<Map<String, Object>>) cSourceRegistration
				.get(NGSIConstants.NGSI_LD_INFORMATION)).get(0).get(NGSIConstants.NGSI_LD_ENTITIES);
		HashSet<String> result = new HashSet<String>();
		if (entities != null) {
			for (Map<String, Object> entry : entities) {
				result.addAll((List<String>) entry.get(NGSIConstants.JSON_LD_TYPE));
			}
		}
		return result;
	}

	public static String generateUniqueRegId(Map<String, Object> resolved) {
		String key = "urn:ngsi-ld:csourceregistration:" + resolved.hashCode();
		return key;
	}

	public static List<BaseProperty> getBaseProperties(Map<String, Object> fullEntry) {
		ArrayList<BaseProperty> result = new ArrayList<BaseProperty>();
		for (Entry<String, Object> entry : fullEntry.entrySet()) {
			String key = entry.getKey();
			if (NGSIConstants.JSON_LD_TYPE.equals(key) || NGSIConstants.JSON_LD_ID.equals(key)) {
				continue;
			}
			List<Map<String, Object>> value = (List<Map<String, Object>>) entry.getValue();
			Map<String, Object> tmp = value.get(0);
			Object type = tmp.get(NGSIConstants.JSON_LD_TYPE);
			BaseProperty prop;
			if (type == null) {
				prop = generateFakeProperty(key, tmp);
			} else {
				String typeString;
				if (type instanceof List) {
					typeString = ((List<String>) type).get(0);
				} else {
					typeString = (String) type;
				}
				switch (typeString) {
				case NGSIConstants.NGSI_LD_GEOPROPERTY:
					prop = SerializationTools.parseGeoProperty((List<Map<String, Object>>) value, key);
					continue;
				case NGSIConstants.NGSI_LD_RELATIONSHIP:
					prop = SerializationTools.parseRelationship((List<Map<String, Object>>) value, key);
					break;
				case NGSIConstants.NGSI_LD_LISTRELATIONSHIP:
					prop = SerializationTools.parseRelationship((List<Map<String, Object>>) value, key);
					break;
				case NGSIConstants.NGSI_LD_DATE_TIME:
					prop = generateFakeProperty(typeString, ((List<Map<String, Object>>) value).get(0));
					break;
				case NGSIConstants.NGSI_LD_LANGPROPERTY:
					prop = generateFakeProperty(key, tmp);
					break;
				case NGSIConstants.NGSI_LD_VocabProperty:
					prop = generateFakeProperty(key, tmp);
					break;
				case NGSIConstants.NGSI_LD_ListProperty:
					prop = generateFakeProperty(key, tmp);
					break;
				case NGSIConstants.NGSI_LD_LOCALONLY:
					prop = generateFakeProperty(key, tmp);
					break;
				case NGSIConstants.NGSI_LD_PROPERTY:
				default:
					prop = SerializationTools.parseProperty((List<Map<String, Object>>) value, key);
					break;
				}

			}

			result.add(prop);
		}
		return result;
	}

	private static BaseProperty generateFakeProperty(String key, Map<String, Object> tmp) {
		Property result = new Property();
		result.setId(key);
		Object value = tmp.get(NGSIConstants.JSON_LD_VALUE);
		result.setSingleEntry(new PropertyEntry(null, value));
		return result;
	}

//	@SuppressWarnings("unchecked")
//	public static Set<String> getTypesFromEntity(BaseRequest createRequest) {
//		List<String> temp = (List<String>) createRequest.getPayload().get(NGSIConstants.JSON_LD_TYPE);
//		return new HashSet<String>(temp);
//	}

	public static String getInstanceId(Map<String, Object> jsonElement) {
		Object instanceId = jsonElement.get(NGSIConstants.NGSI_LD_INSTANCE_ID);
		if (instanceId == null) {
			return null;
		}
		return ((List<Map<String, String>>) instanceId).get(0).get(NGSIConstants.JSON_LD_ID);
	}

	public static List<String[]> getScopes(Map<String, Object> fullEntry) {
		ArrayList<String[]> result = new ArrayList<String[]>();
		Object scopes = fullEntry.get(NGSIConstants.NGSI_LD_SCOPE);
		if (scopes != null && scopes instanceof List) {
			List<Map<String, String>> list = (List<Map<String, String>>) scopes;
			for (Map<String, String> entry : list) {
				result.add(entry.get(NGSIConstants.JSON_LD_VALUE).substring(1).split("\\/"));
			}
		}
		return result;
	}

	public static void removeRegKey(Map<String, Object> remoteEntity) {
		for (Entry<String, Object> attrib : remoteEntity.entrySet()) {
			String key = attrib.getKey();
			if (DO_NOT_MERGE_KEYS.contains(key)) {
				continue;
			}
			List<Map<String, Object>> list = (List<Map<String, Object>>) attrib.getValue();
			for (Map<String, Object> entry : list) {
				entry.remove(AppConstants.REG_MODE_KEY);
			}
		}

	}

	public static void mergeValues(List<Map<String, Object>> currentValue, List<Map<String, Object>> newValue) {
		long newObservedAt = -1, newModifiedAt = -1, newCreatedAt = -1, currentObservedAt = -1, currentModifiedAt = -1,
				currentCreatedAt = -1;
		int currentRegMode = -1;
		String newDatasetId;
		int removeIndex = -1;
		int regMode = -1;
		boolean found = false;
		for (Map<String, Object> entry : newValue) {
			if (entry.containsKey(NGSIConstants.NGSI_LD_DATA_SET_ID)) {
				newDatasetId = ((List<Map<String, String>>) entry.get(NGSIConstants.NGSI_LD_DATA_SET_ID)).get(0)
						.get(NGSIConstants.JSON_LD_ID);
			} else {
				newDatasetId = null;
			}
			newObservedAt = -1;
			newModifiedAt = -1;
			newCreatedAt = -1;
			try {
				newCreatedAt = SerializationTools.date2Long((String) entry.get(NGSIConstants.NGSI_LD_CREATED_AT));
				newModifiedAt = SerializationTools.date2Long((String) entry.get(NGSIConstants.NGSI_LD_MODIFIED_AT));
				if (entry.containsKey(NGSIConstants.NGSI_LD_OBSERVED_AT)) {
					newObservedAt = SerializationTools.date2Long((String) entry.get(NGSIConstants.NGSI_LD_OBSERVED_AT));
				}
			} catch (Exception e) {
				// do nothing intentionally
			}
			regMode = -1;
			if (entry.containsKey(AppConstants.REG_MODE_KEY)) {
				regMode = (int) entry.get(AppConstants.REG_MODE_KEY);
			}
			removeIndex = -1;
			found = false;
			for (int i = 0; i < currentValue.size(); i++) {
				Map<String, Object> currentEntry = currentValue.get(i);
				currentRegMode = -1;
				if (currentEntry.containsKey(AppConstants.REG_MODE_KEY)) {
					currentRegMode = (int) currentEntry.get(AppConstants.REG_MODE_KEY);
				}
				String currentDatasetId;
				if (currentEntry.containsKey(NGSIConstants.NGSI_LD_DATA_SET_ID)) {
					currentDatasetId = ((List<Map<String, String>>) currentEntry.get(NGSIConstants.NGSI_LD_DATA_SET_ID))
							.get(0).get(NGSIConstants.JSON_LD_ID);
				} else {
					currentDatasetId = null;
				}
				if ((currentDatasetId == null && newDatasetId == null) || (currentDatasetId != null
						&& newDatasetId != null && currentDatasetId.equals(newDatasetId))) {
					// 0 auxilliary
					// 1 inclusive
					// 2 proxy
					// 3 exclusive
					found = true;
					if (currentRegMode == 3 || regMode == 0) {
						break;
					}
					if (regMode == 3 || currentRegMode == 0) {
						removeIndex = i;
						break;
					}
					currentObservedAt = -1;
					currentModifiedAt = -1;
					currentCreatedAt = -1;
					try {
						currentCreatedAt = SerializationTools
								.date2Long((String) currentEntry.get(NGSIConstants.NGSI_LD_CREATED_AT));
						currentModifiedAt = SerializationTools
								.date2Long((String) currentEntry.get(NGSIConstants.NGSI_LD_MODIFIED_AT));
						if (currentEntry.containsKey(NGSIConstants.NGSI_LD_OBSERVED_AT)) {
							currentObservedAt = SerializationTools
									.date2Long((String) currentEntry.get(NGSIConstants.NGSI_LD_OBSERVED_AT));
						}
					} catch (Exception e) {
						// do nothing intentionally
					}
					// if observedAt is set it will take preference over modifiedAt
					if (currentObservedAt != -1 || newObservedAt != -1) {
						if (currentObservedAt >= newObservedAt) {
							break;
						} else {
							removeIndex = i;
							break;
						}
					}
					if (currentModifiedAt >= newModifiedAt) {
						break;
					} else {
						removeIndex = i;
						break;
					}

				}
			}
			if (found) {
				if (removeIndex != -1) {
					currentValue.remove(removeIndex);
					currentValue.add(entry);
				}
			} else {
				currentValue.add(entry);
			}
		}
	}

	public static void addRegModeToValue(List<Map<String, Object>> newValue, int regMode) {
		for (Map<String, Object> entry : newValue) {
			entry.put(AppConstants.REG_MODE_KEY, regMode);
		}
	}

	public static void removeAttrs(Map<String, Object> localEntity, Set<String> attrs) {
		Set<String> entityAttrs = localEntity.keySet();
		boolean attrsFound = false;
		for (String attr : attrs) {
			if (entityAttrs.contains(attr)) {
				attrsFound = true;
				break;
			}
		}
		entityAttrs.removeAll(attrs);
		entityAttrs.removeAll(EntityTools.DO_NOT_MERGE_KEYS);
		if (entityAttrs.isEmpty() && !attrsFound) {
			localEntity.clear();
		} else {
			for (String attr : entityAttrs) {
				localEntity.remove(attr);
			}
		}

	}

	public static Map<String, Object> addSysAttrs(Map<String, Object> resolved, long timeStamp) {
		String now = SerializationTools.formatter.format(Instant.ofEpochMilli(timeStamp));
		setTemporalProperties(resolved, now, now, false);
		return resolved;
	}

	@SuppressWarnings({ "rawtypes"})
	protected static void setTemporalProperties(Object jsonNode, String createdAt, String modifiedAt,
			boolean rootOnly) {
		if (!(jsonNode instanceof Map)) {
			return;
		}
		Map<String, Object> objectNode = (Map<String, Object>) jsonNode;
		if (!createdAt.isEmpty()) {
			objectNode.remove(NGSIConstants.NGSI_LD_CREATED_AT);
			ArrayList<Object> tmp = getDateTime(createdAt);
			objectNode.put(NGSIConstants.NGSI_LD_CREATED_AT, tmp);
		}
		if (!modifiedAt.isEmpty()) {
			objectNode.remove(NGSIConstants.NGSI_LD_MODIFIED_AT);
			ArrayList<Object> tmp = getDateTime(modifiedAt);
			objectNode.put(NGSIConstants.NGSI_LD_MODIFIED_AT, tmp);
		}
		if (rootOnly) {
			return;
		}
		for (Entry<String, Object> entry : objectNode.entrySet()) {
			if (entry.getValue() instanceof List && !((List) entry.getValue()).isEmpty()) {
				List list = (List) entry.getValue();
				for (Object entry2 : list) {
					if (entry2 instanceof Map) {
						Map<String, Object> map = (Map<String, Object>) entry2;
						if (map.containsKey(NGSIConstants.JSON_LD_TYPE)
								&& map.get(NGSIConstants.JSON_LD_TYPE) instanceof List
								&& !((List) map.get(NGSIConstants.JSON_LD_TYPE)).isEmpty()
								&& NGSIConstants.NGSI_LD_ATTR_TYPES
										.contains(((List) map.get(NGSIConstants.JSON_LD_TYPE)).get(0).toString())) {
							setTemporalProperties(map, createdAt, modifiedAt, rootOnly);
						}
					}
				}

			}
		}
	}

	public static String getCoordinates(List<Map<String, Object>> jsonCoordinates) {
		String result = "";
		boolean lon = true;
		for (Map<String, Object> entry : jsonCoordinates) {
			for (Entry<String, Object> entry1 : entry.entrySet()) {
				String key = entry1.getKey();
				Object value = entry1.getValue();
				if (key.equals(NGSIConstants.JSON_LD_VALUE)) {
					double myValue = 0;
					if (value instanceof Double) {
						myValue = (Double) value;
					} else if (value instanceof Integer) {
						myValue = ((Integer) value).doubleValue();
					} else if (value instanceof Long) {
						myValue = ((Long) value).doubleValue();
					}
					if (lon) {
						myValue = SerializationTools.getProperLon(myValue);
						result += "[" + myValue + ", ";
					} else {
						myValue = SerializationTools.getProperLat(myValue);
						result += myValue + "]";
					}

					lon = !lon;
				} else if (key.equals(NGSIConstants.JSON_LD_LIST)) {
					result += "[" + getCoordinates((List<Map<String, Object>>) value) + "]";
				}
			}
			result += ",";
		}
		return result.substring(0, result.length() - 1);
	}

	private static ArrayList<Object> getDateTime(String createdAt) {
		ArrayList<Object> tmp = new ArrayList<Object>();
		HashMap<String, Object> tmp2 = new HashMap<String, Object>();
		tmp2.put(NGSIConstants.JSON_LD_TYPE, NGSIConstants.NGSI_LD_DATE_TIME);
		tmp2.put(NGSIConstants.JSON_LD_VALUE, createdAt);
		tmp.add(tmp2);
		return tmp;
	}

	public static void noConcise(Object object) throws ResponseException {
		noConcise(object, null, null, 0);
	}

	
	private static void noConcise(Object object, Map<String, Object> parentMap, String keyOfObject, int level) throws ResponseException {
		// Object is Map
		if (object instanceof Map<?, ?> map) {
			// Map have object but not type
			if (map.containsKey(NGSIConstants.OBJECT)) {
				((Map<String, Object>) map).put(NGSIConstants.TYPE, NGSIConstants.RELATIONSHIP);
			} else if (map.containsKey(NGSIConstants.OBJECT_LIST)) {
				((Map<String, Object>) map).put(NGSIConstants.TYPE, NGSIConstants.LISTRELATIONSHIP);
				Object objList = ((Map<String, Object>) map).get(NGSIConstants.OBJECT_LIST);
				if(objList instanceof List<?> l) {
					List<Map<String, String>> tmpList = new ArrayList<>(l.size());
					for(Object entry: l) {
						if(entry instanceof Map<?,?> m) {
							if(m.size() != 1) {
								throw new ResponseException(ErrorType.BadRequestData, "Unkown format for object list entry.");
							}
							Object relId = m.get(NGSIConstants.OBJECT);
							if(relId == null || !(relId instanceof String)) {
								throw new ResponseException(ErrorType.BadRequestData, "Unkown format for object list entry.");
							}
							HttpUtils.validateUri((String) relId);
							tmpList.add((Map<String, String>) m);
						}else if(entry instanceof String s) {
							HttpUtils.validateUri(s);
							Map<String, String> tmpMap = new HashMap<>(1);
							tmpMap.put(NGSIConstants.OBJECT, s);
							tmpList.add(tmpMap);
						}else {
							throw new ResponseException(ErrorType.BadRequestData, "Unkown format for object list entry.");		
						}
					}
					((Map<String, Object>) map).put(NGSIConstants.OBJECT_LIST, tmpList);
				}else {
					throw new ResponseException(ErrorType.BadRequestData, "Unkown format for object list entry.");
				}
			} else if (map.containsKey(NGSIConstants.VALUE_LIST)) {
				((Map<String, Object>) map).put(NGSIConstants.TYPE, NGSIConstants.LISTPROPERTY);
			}
			// Map have vocab but not type
			else if (map.containsKey(NGSIConstants.VOCAB)) {
				((Map<String, Object>) map).put(NGSIConstants.TYPE, NGSIConstants.VOCABPROPERTY);
			}
			// Map have value but not type
			else if (map.containsKey(NGSIConstants.VALUE) && !map.containsKey(NGSIConstants.TYPE)) {
				// for GeoProperty
				if (map.get(NGSIConstants.VALUE) instanceof Map<?, ?> nestedMap
						&& (NGSIConstants.GEO_KEYWORDS.contains(nestedMap.get(NGSIConstants.TYPE)))) {
					((Map<String, Object>) map).put(NGSIConstants.TYPE, NGSIConstants.NGSI_LD_GEOPROPERTY_SHORT);
				} else
					((Map<String, Object>) map).put(NGSIConstants.TYPE, NGSIConstants.PROPERTY);
			}
			// for GeoProperty
			else if (map.containsKey(NGSIConstants.TYPE)
					&& (NGSIConstants.GEO_KEYWORDS.contains(map.get(NGSIConstants.TYPE)))
					&& !keyOfObject.equals(NGSIConstants.VALUE)) {
				Map<String, Object> newMap = new HashMap<>();
				newMap.put(NGSIConstants.TYPE, NGSIConstants.NGSI_LD_GEOPROPERTY_SHORT);
				newMap.put(NGSIConstants.VALUE, map);
				parentMap.put(keyOfObject, newMap);
			} else if (map.containsKey(NGSIConstants.LANGUAGE_MAP)) {
				((Map<String, Object>) map).put(NGSIConstants.TYPE, NGSIConstants.LANGUAGE_PROPERTY);
			} else if (map.containsKey(NGSIConstants.JSON)) {
				((Map<String, Object>) map).put(NGSIConstants.TYPE, NGSIConstants.JSONPROPERTY);
			} else if (parentMap != null && !map.containsKey(NGSIConstants.TYPE) && level == 1) {
				Map<String, Object> newMap = new HashMap<>();
				newMap.put(NGSIConstants.VALUE, map);
				newMap.put(NGSIConstants.TYPE, NGSIConstants.PROPERTY);
				parentMap.put(keyOfObject, newMap);
			} else {
				// Iterate through every element of Map
				Object[] mapKeys = map.keySet().toArray();
				for (Object key : mapKeys) {
					if (!key.equals(NGSIConstants.ID) && !key.equals(NGSIConstants.TYPE)
							&& !key.equals(NGSIConstants.JSON_LD_CONTEXT)
							&& !key.equals(NGSIConstants.QUERY_PARAMETER_COORDINATES)
							&& !key.equals(NGSIConstants.QUERY_PARAMETER_OBSERVED_AT)
							&& !key.equals(NGSIConstants.INSTANCE_ID)
							&& !key.equals(NGSIConstants.QUERY_PARAMETER_DATA_SET_ID)
							&& !key.equals(NGSIConstants.OBJECT) && !key.equals(NGSIConstants.OBJECT_LIST)
							&& !key.equals(NGSIConstants.VALUE) && !key.equals(NGSIConstants.SCOPE)
							&& !key.equals(NGSIConstants.QUERY_PARAMETER_UNIT_CODE)
							&& !key.equals(NGSIConstants.LANGUAGE_MAP) && !key.equals(NGSIConstants.JSON)
							&& !key.equals(NGSIConstants.VOCAB) && !key.equals(NGSIConstants.LIST)
							&& !key.equals(NGSIConstants.LOCALONLY) && !key.equals(NGSIConstants.OBJECT_TYPE)) {
						noConcise(map.get(key), (Map<String, Object>) map, key.toString(), level + 1);
					}
				}
			}
			if (map.containsKey(NGSIConstants.PROVIDED_BY)) {
				noConcise(map.get(NGSIConstants.PROVIDED_BY), (Map<String, Object>) map, NGSIConstants.PROVIDED_BY,
						level + 1);
			}
		}
		// Object is List
		else if (object instanceof List<?> list) {
			boolean putType = true;
			for (Object obj : list) {
				if (obj instanceof Map<?, ?> map && map.containsKey(NGSIConstants.TYPE)) {
					putType = false;
					break;
				}
			}
			if (putType && parentMap != null && level == 1) {
				Map<String, Object> newMap = new HashMap<>();
				newMap.put(NGSIConstants.VALUE, list);
				newMap.put(NGSIConstants.TYPE, NGSIConstants.PROPERTY);
				parentMap.put(keyOfObject, newMap);
			} else {
				for (Object o : list) {
					noConcise(o, null, null, level);
				}
			}
		}
		// Object is String or Number value
		else if ((object instanceof String || object instanceof Number) && parentMap != null) {
			// if keyofobject is value then just need to convert double to int if possible
			if (keyOfObject != null && keyOfObject.equals(NGSIConstants.VALUE)) {
				parentMap.put(keyOfObject, HttpUtils.doubleToInt(object));
			} else {
				Map<String, Object> newMap = new HashMap<>();
				newMap.put(NGSIConstants.VALUE, HttpUtils.doubleToInt(object));
				newMap.put(NGSIConstants.TYPE, NGSIConstants.PROPERTY);
				parentMap.put(keyOfObject, newMap);
			}

		}
	}

	private static Uni<List<Map<String, Object>>> handle414(WebClient webClient, QueryRemoteHost remoteHost,
			int timeout, JsonLDService ldService, String id, String type, String idPattern) {
		logger.debug("Attempting 414 recovery");
		if (id == null) {
			logger.debug("Can't recover 414 because no id has been used");
			return Uni.createFrom().item(Lists.newArrayList());
		}
		int idMiddle = (int) (id.length() / 2);
		String newHalfIdOne;
		String newHalfIdTwo;
		if (id.charAt(idMiddle) == ',') {
			newHalfIdOne = id.substring(0, idMiddle);
			newHalfIdTwo = id.substring(idMiddle + 1, id.length());
		} else {
			int idx = id.substring(0, idMiddle).lastIndexOf(',');
			if (idx == -1) {
				logger.debug("Can't recover 414 because no multiple ids has been used");
				return Uni.createFrom().item(Lists.newArrayList());
			}
			newHalfIdOne = id.substring(0, idx);
			newHalfIdTwo = id.substring(idx + 1, id.length());
		}

		logger.debug(newHalfIdOne);
		logger.debug(newHalfIdTwo);
		QueryRemoteHost hostOne = remoteHost.copyFor414Handle(newHalfIdOne, type, idPattern);
		QueryRemoteHost hostTwo = remoteHost.copyFor414Handle(newHalfIdTwo, type, idPattern);

		return Uni.combine().all().unis(getRemoteEntities(hostOne, webClient, timeout, ldService),
				getRemoteEntities(hostTwo, webClient, timeout, ldService)).asTuple().onItem().transform(tpl -> {
					List<Map<String, Object>> result = tpl.getItem1();
					result.addAll(tpl.getItem2());
					return result;
				});
	}

	public static Uni<List<Map<String, Object>>> getRemoteEntities(QueryRemoteHost remoteHost, WebClient webClient,
			int timeout, JsonLDService ldService) {

		List<Tuple3<String, String, String>> idsAndTypesAndIdPattern = remoteHost.getIdsAndTypesAndIdPattern();
		Context context = remoteHost.context();
		List<Uni<List<Object>>> unis = new ArrayList<>();
		if (remoteHost.isCanDoBatchQuery()) {
			Map<String, Object> batchBody = Maps.newHashMap();
			batchBody.put(NGSIConstants.TYPE, NGSIConstants.QUERY_TYPE);
			List<Tuple3<String, String, String>> idsTypeAndPattern = remoteHost.getIdsAndTypesAndIdPattern();
			if (idsTypeAndPattern != null) {
				List<Map<String, String>> entities = Lists.newArrayList();
				for (Tuple3<String, String, String> entry : idsTypeAndPattern) {
					String id = entry.getItem1();
					String type = entry.getItem2();
					String idPattern = entry.getItem3();
					Map<String, String> tmp = Maps.newHashMap();
					if (id != null) {
						tmp.put(NGSIConstants.ID, id);
					}
					if (type != null) {
						tmp.put(NGSIConstants.TYPE, type);
					}
					if (id != null) {
						tmp.put(NGSIConstants.QUERY_PARAMETER_IDPATTERN, idPattern);
					}
					entities.add(tmp);
				}
				batchBody.put(NGSIConstants.NGSI_LD_ENTITIES_SHORT, entities);
			}
			Map<String, String> queryParams = remoteHost.getQueryParam();
			queryParams.remove(NGSIConstants.ID);
			queryParams.remove(NGSIConstants.TYPE);
			if (queryParams != null) {
				batchBody.putAll(queryParams);
			}
			HttpRequest<Buffer> req = webClient.postAbs(remoteHost.host() + NGSIConstants.ENDPOINT_BATCH_QUERY).timeout(timeout);
			req = req.setQueryParam("limit", "1000");
			req = req.setQueryParam("options", "sysAttrs");
			req = req.putHeader(HttpHeaders.VIA, remoteHost.getViaHeaders().getViaHeaders());
			String batchString;
			req.putHeader(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSONLD);
			batchBody.put(NGSIConstants.JSON_LD_CONTEXT, context.getOriginalAtContext());
			try {
				batchString = JsonUtils.toPrettyString(batchBody);
			} catch (Exception e) {
				logger.warn("failed to serialize batch request");
				return Uni.createFrom().item(Lists.newArrayList());
			}
			if(!remoteHost.headers().contains(HttpHeaders.ACCEPT)) {
				req = req.putHeader(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSON);
			}
			
			unis.add(req.putHeaders(remoteHost.headers()).sendBuffer(Buffer.buffer(batchString))
					.onItem().transformToUni(response -> {
						if (response != null) {
							switch (response.statusCode()) {
							case 200: {
								return handle200(webClient, remoteHost, response, ldService, timeout);
							}
							default: {
								return Uni.createFrom().item(Lists.newArrayList());
							}

							}
						} else {
							return Uni.createFrom().item(Lists.newArrayList());
						}

					}).onFailure().recoverWithUni(e -> {
						logger.warn("Failed to query remote host" + remoteHost.toString(), e);
						return Uni.createFrom().item(Lists.newArrayList());
					}));

		} else if (remoteHost.isCanDoQuery()) {
			if (idsAndTypesAndIdPattern == null) {
				Tuple3<String, String, String> tmpTpl = Tuple3.of(null, null, null);
				idsAndTypesAndIdPattern = Lists.newArrayList();
				idsAndTypesAndIdPattern.add(tmpTpl);
			}
			for (Tuple3<String, String, String> tpl : idsAndTypesAndIdPattern) {
				String id = tpl.getItem1();
				String type = tpl.getItem2();
				String idPattern = tpl.getItem3();
				HttpRequest<Buffer> req = webClient.getAbs(remoteHost.host() + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT);
				if (id != null) {
					req = req.setQueryParam(NGSIConstants.ID, id);
				}
				if (type != null) {
					req = req.setQueryParam(NGSIConstants.TYPE, type);
				}
				if (idPattern != null) {
					req = req.setQueryParam(NGSIConstants.QUERY_PARAMETER_IDPATTERN, idPattern);
				}

				for (Entry<String, String> param : remoteHost.getQueryParam().entrySet()) {
					req = req.setQueryParam(param.getKey(), (String) param.getValue());
				}
				req = req.setQueryParam("limit", "1000");
				req = req.setQueryParam("options", "sysAttrs");
				req = req.putHeader(HttpHeaders.VIA, remoteHost.getViaHeaders().getViaHeaders());
				// <https://raw.githubusercontent.com/ScorpioBroker/ScorpioBroker/new_ci/testcontext.json>;
				// rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"
				if (context != null && context.getOriginalAtContext() != null
						&& !context.getOriginalAtContext().isEmpty()) {
					Object ctx = context.getOriginalAtContext().get(0);
					if (ctx instanceof String ctxStr) {
						req = req.putHeader(HttpHeaders.LINK,
								"<" + ctxStr + ">; rel=\"" + NGSIConstants.HEADER_REL_LDCONTEXT + "\"; type=\""
										+ AppConstants.NGB_APPLICATION_JSONLD + "\"");
					}
				}
				if(!remoteHost.headers().contains(HttpHeaders.ACCEPT)) {
					req = req.putHeader(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSON);
				}
				unis.add(req.putHeaders(remoteHost.headers()).timeout(timeout).send().onItem()
						.transformToUni(response -> {

							if (response != null) {
								switch (response.statusCode()) {
								case 200: {
									return handle200(webClient, remoteHost, response, ldService, timeout);
								}
								case 414: {
									return handle414(webClient, remoteHost, timeout, ldService, id, type, idPattern)
											.onItem().transformToUni(entities -> {
												logger.debug("414 recovered");
												return ldService.expand(context, entities, AppConstants.opts, -1,
														false);
											});
								}
								default: {

									return Uni.createFrom().item(Lists.newArrayList());
								}

								}
							} else {
								return Uni.createFrom().item(Lists.newArrayList());
							}

						}).onFailure().recoverWithUni(e -> {
							logger.warn("Failed to query remote host" + remoteHost.toString(), e);

							return Uni.createFrom().item(Lists.newArrayList());
						}));
			}
		} else if (remoteHost.isCanDoRetrieve()) {
			List<Tuple3<String, String, String>> idsTypeAndPattern = remoteHost.getIdsAndTypesAndIdPattern();
			Map<String, String> queryParams = remoteHost.getQueryParam();

			if (idsTypeAndPattern != null) {
				for (Tuple3<String, String, String> tpl : idsTypeAndPattern) {
					String id = tpl.getItem1();
					if (id != null) {
						String[] ids = id.split(",");
						for (String idEntry : ids) {
							HttpRequest<Buffer> req = webClient.getAbs(
									remoteHost.host() + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT + "/" + idEntry);
							req = req.putHeader(HttpHeaders.VIA, remoteHost.getViaHeaders().getViaHeaders());
							if (queryParams != null) {
								for (Entry<String, String> param : queryParams.entrySet()) {
									req = req.addQueryParam(param.getKey(), param.getValue());
								}
							}
							if (context != null && context.getOriginalAtContext() != null
									&& !context.getOriginalAtContext().isEmpty()) {
								Object ctx = context.getOriginalAtContext().get(0);
								if (ctx instanceof String ctxStr) {
									req = req.putHeader(HttpHeaders.LINK,
											"<" + ctxStr + ">; rel=\"" + NGSIConstants.HEADER_REL_LDCONTEXT
													+ "\"; type=\"" + AppConstants.NGB_APPLICATION_JSONLD + "\"");
								}
							}
							req = req.setQueryParam("options", "sysAttrs");
							if(!remoteHost.headers().contains(HttpHeaders.ACCEPT)) {
								req = req.putHeader(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSON);
							}
							unis.add(req.putHeaders(remoteHost.headers()).timeout(timeout).send().onItem()
									.transformToUni(response -> {

										if (response != null) {
											switch (response.statusCode()) {
											case 200: {
												return handle200(webClient, remoteHost, response, ldService, timeout);
											}
											default: {

												return Uni.createFrom().item(Lists.newArrayList());
											}

											}
										} else {
											return Uni.createFrom().item(Lists.newArrayList());
										}

									}).onFailure().recoverWithUni(e -> {
										logger.warn("Failed to query remote host" + remoteHost.toString(), e);

										return Uni.createFrom().item(Lists.newArrayList());
									}));

						}
					}
				}
			}
		}
		if (unis.isEmpty()) {
			return Uni.createFrom().item(Lists.newArrayList());
		}
		return Uni.combine().all().unis(unis).with(l -> {
			List<Map<String, Object>> result = Lists.newArrayList();
			for (Object obj : l) {
				List<Object> entities = (List<Object>) obj;
				for (Object entityObj : entities) {
					Map<String, Object> entityEntry = (Map<String, Object>) entityObj;
					entityEntry.put(AppConstants.REG_MODE_KEY, remoteHost.regMode());
					result.add(entityEntry);
				}
			}
			return result;
		});
	}

	private static Uni<List<Object>> handle200(WebClient webClient, QueryRemoteHost remoteHost,
			HttpResponse<Buffer> response, JsonLDService ldService, int timeout) {
		List<Map<String, Object>> tmpList = response.bodyAsJsonArray().getList();
		return ldService.expand(remoteHost.context(), tmpList, AppConstants.opts, -1, false).onItem()
				.transformToUni(expanded -> {
					if (response.headers().contains("Next")) {
						remoteHost.setParamsFromNext(response.headers().get("Next"));
						return getRemoteEntities(remoteHost, webClient, timeout, ldService).onItem()
								.transform(nextResult -> {

									if (nextResult != null) {
										expanded.addAll(nextResult);
									}

									return expanded;
								});

					}
					return Uni.createFrom().item(expanded);
				});
	}

	public static Collection<QueryRemoteHost> getRemoteQueries(String tenant,
			List<Tuple3<String[], TypeQueryTerm, String>> idsAndTypeQueryAndIdPattern, AttrsQueryTerm attrsQuery,
			QQueryTerm qQuery, GeoQueryTerm geoQuery, ScopeQueryTerm scopeQuery, LanguageQueryTerm langQuery,
			Table<String, String, List<RegistrationEntry>> tenant2CId2RegEntries, Context context,
			EntityCache fullEntityCache, boolean splitEntities, ViaHeaders viaHeaders) {
		return getRemoteQueries(idsAndTypeQueryAndIdPattern, attrsQuery, qQuery, geoQuery, scopeQuery, langQuery,
				tenant2CId2RegEntries.row(tenant).values(), context, fullEntityCache, viaHeaders, splitEntities);
	}

	public static Collection<QueryRemoteHost> getRemoteQueries(
			List<Tuple3<String[], TypeQueryTerm, String>> idsAndTypeQueryAndIdPattern, AttrsQueryTerm attrsQuery,
			QQueryTerm qQuery, GeoQueryTerm geoQuery, ScopeQueryTerm scopeQuery, LanguageQueryTerm langQuery,
			Collection<List<RegistrationEntry>> regEntries, Context context, EntityCache fullEntityCache,
			ViaHeaders viaHeaders, boolean isDist) {

		// ids, types, attrs, geo, scope
		List<Map<QueryRemoteHost, QueryInfos>> remoteHost2QueryInfos = Lists.newArrayList();
		if (idsAndTypeQueryAndIdPattern == null) {
			idsAndTypeQueryAndIdPattern = Lists.newArrayList();
			idsAndTypeQueryAndIdPattern.add(Tuple3.of(null, null, null));
		}
		for (Tuple3<String[], TypeQueryTerm, String> t : idsAndTypeQueryAndIdPattern) {
			Map<QueryRemoteHost, QueryInfos> remoteHost2QueryInfo = Maps.newHashMap();
			remoteHost2QueryInfos.add(remoteHost2QueryInfo);
			Iterator<List<RegistrationEntry>> it = regEntries.iterator();
			String[] id = t.getItem1();
			TypeQueryTerm typeQuery = t.getItem2();
			String idPattern = t.getItem3();
			while (it.hasNext()) {
				Iterator<RegistrationEntry> tenantRegs = it.next().iterator();
				while (tenantRegs.hasNext()) {

					RegistrationEntry regEntry = tenantRegs.next();
					if (regEntry.expiresAt() > 0 && regEntry.expiresAt() <= System.currentTimeMillis()) {
						it.remove();
						continue;
					}
					if (!regEntry.queryBatch() && !regEntry.queryEntity()) {
						continue;
					}

					if (regEntry.matches(id, idPattern, typeQuery, attrsQuery, qQuery, geoQuery, scopeQuery) == null) {
						continue;
					}

					RemoteHost regHost = regEntry.host();
					if (viaHeaders.getHostUrls().contains(regHost.host())) {
						continue;
					}

					QueryRemoteHost hostToQuery = QueryRemoteHost.fromRegEntry(regEntry);
					QueryInfos queryInfos = remoteHost2QueryInfo.get(hostToQuery);
					if (queryInfos == null) {
						queryInfos = new QueryInfos();
						queryInfos.setGeoQuery(geoQuery);
						queryInfos.setLangQuery(langQuery);
						queryInfos.setTypeQuery(typeQuery);
						remoteHost2QueryInfo.put(hostToQuery, queryInfos);
					}

					if (!queryInfos.isFullIdFound()) {
						if (regEntry.eId() != null) {
							queryInfos.getIds().add(regEntry.eId());
						} else {
							if (id != null) {
								queryInfos.setIds(Sets.newHashSet(id));
								queryInfos.setFullIdFound(true);
							} else if (idPattern != null) {
								queryInfos.setIdPattern(idPattern);
							}
						}
					}
					if (!queryInfos.isFullTypesFound()) {
						if (regEntry.type() != null) {
							queryInfos.getTypes().add(regEntry.type());
						} else {
							if (typeQuery != null) {
								queryInfos.setTypes(typeQuery.getAllTypes());
								queryInfos.setFullTypesFound(true);
							}
						}
					}
					if (!queryInfos.isFullAttrsFound()) {
						if (regEntry.eProp() != null) {
							queryInfos.getAttrs().add(regEntry.eProp());
						} else if (regEntry.eRel() != null) {
							queryInfos.getAttrs().add(regEntry.eRel());
						} else {
							queryInfos.setFullAttrsFound(true);
							if (attrsQuery != null && attrsQuery.getAttrs() != null
									&& !attrsQuery.getAttrs().isEmpty()) {
								queryInfos.setAttrs(attrsQuery.getAttrs());
							}
						}
					}
				}

			}
		}
		Map<String, QueryRemoteHost> cSourceId2QueryRemoteHost = Maps.newHashMap();

		for (Map<QueryRemoteHost, QueryInfos> remoteHost2QueryInfo : remoteHost2QueryInfos) {
			for (Entry<QueryRemoteHost, QueryInfos> entry : remoteHost2QueryInfo.entrySet()) {
				QueryRemoteHost tmpHost = entry.getKey();
				QueryRemoteHost finalHost = cSourceId2QueryRemoteHost.get(tmpHost.cSourceId());
				if (finalHost == null) {
					finalHost = tmpHost;
					viaHeaders.addViaHeader(tmpHost.host());
					finalHost.setViaHeaders(viaHeaders);
					cSourceId2QueryRemoteHost.put(finalHost.cSourceId(), finalHost);
				}

				Context contextToUse = finalHost.context();
				if (contextToUse == null) {
					finalHost.setContext(context);
					contextToUse = context;
				}
				Map<String, String> queryParams = entry.getValue().toQueryParams(context, false, fullEntityCache,
						finalHost);
				finalHost.addIdsAndTypesAndIdPattern(
						Tuple3.of(queryParams.remove(NGSIConstants.ID), queryParams.remove(NGSIConstants.TYPE),
								queryParams.remove(NGSIConstants.QUERY_PARAMETER_IDPATTERN)));
				finalHost.setQueryParam(queryParams);

			}
		}

		return cSourceId2QueryRemoteHost.values();
	}

	public static Map<String, Map<String, Object>> evaluateFilterQueries(QueryResult queryResult, QQueryTerm qQuery,
			ScopeQueryTerm scopeQuery, GeoQueryTerm geoQuery, AttrsQueryTerm attrsTerm, PickTerm pickTerm,
			OmitTerm omitTerm, DataSetIdTerm dataSetIdTerm, EntityCache entityCache, Set<String> jsonKeys,
			boolean calculateLinked) {
		Map<String, Map<String, Object>> deleted = Maps.newHashMap();
		List<Map<String, Object>> resultData = queryResult.getData();
		Iterator<Map<String, Object>> it = resultData.iterator();
		Map<String, Map<String, Object>> flatEntities = queryResult.getFlatJoin();
		Set<String> pickForFlat = Sets.newHashSet();
		boolean flatJoin = queryResult.isFlatJoin();
		while (it.hasNext()) {
			Map<String, Object> entity = it.next();
			// order is important here qquery scope and geo remove full entities and the
			// rest modifies the entities and might result in empty entities
			boolean qResult = (qQuery != null && !qQuery.calculateEntity(entity, entityCache, jsonKeys, false));
			boolean scopeResult = (scopeQuery != null && !scopeQuery.calculateEntity(entity));
			boolean geoQResult = (geoQuery != null && !geoQuery.calculateEntity(entity));
			boolean attrsResult = (attrsTerm != null && !attrsTerm.calculateEntity(entity));
			boolean pickResult = (pickTerm != null
					&& !pickTerm.calculateEntity(entity, flatJoin, flatEntities, pickForFlat, calculateLinked));
			boolean omitResult = (omitTerm != null
					&& !omitTerm.calculateEntity(entity, flatJoin, flatEntities, pickForFlat, calculateLinked));
			boolean datasetIdResult = (dataSetIdTerm != null && !dataSetIdTerm.calculateEntity(entity));
			if (qResult || scopeResult || geoQResult || attrsResult || pickResult || omitResult || datasetIdResult) {
				it.remove();
				deleted.put((String) entity.get(NGSIConstants.JSON_LD_ID), entity);
			}

		}
		if (flatEntities != null && flatJoin) {
			if (pickTerm == null && omitTerm == null) {
				resultData.addAll(flatEntities.values());
			} else {
				for (String id : pickForFlat) {
					resultData.add(flatEntities.get(id));
				}
			}
		}
		return deleted;
	}

}
