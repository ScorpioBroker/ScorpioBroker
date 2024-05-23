package eu.neclab.ngsildbroker.commons.tools;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLDService;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;

import java.util.Set;
import java.util.UUID;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.BaseProperty;
import eu.neclab.ngsildbroker.commons.datatypes.GeoProperty;
import eu.neclab.ngsildbroker.commons.datatypes.LDGeoQuery;
import eu.neclab.ngsildbroker.commons.datatypes.Notification;
import eu.neclab.ngsildbroker.commons.datatypes.Property;
import eu.neclab.ngsildbroker.commons.datatypes.PropertyEntry;
import eu.neclab.ngsildbroker.commons.datatypes.QueryInfos;
import eu.neclab.ngsildbroker.commons.datatypes.QueryRemoteHost;
import eu.neclab.ngsildbroker.commons.datatypes.RegistrationEntry;
import eu.neclab.ngsildbroker.commons.datatypes.RemoteHost;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.datatypes.terms.AttrsQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.GeoQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.LanguageQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.QQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.ScopeQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.TypeQueryTerm;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.vertx.mutiny.ext.web.client.WebClient;

public abstract class EntityTools {

	private static final String BROKER_PREFIX = "ngsildbroker:";
	public static final String REG_MODE_KEY = "!@#$%";
	public static final Set<String> DO_NOT_MERGE_KEYS = Sets.newHashSet(NGSIConstants.JSON_LD_ID,
			NGSIConstants.JSON_LD_TYPE, NGSIConstants.NGSI_LD_CREATED_AT, NGSIConstants.NGSI_LD_OBSERVED_AT,
			NGSIConstants.NGSI_LD_MODIFIED_AT);

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

	@SuppressWarnings("unchecked")
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

	public static Map<String, Object> clearBaseProps(Map<String, Object> fullEntry, SubscriptionRequest subscription) {
		Set<String> notificationAttrs = subscription.getSubscription().getNotification().getAttributeNames();
		if (notificationAttrs == null || notificationAttrs.isEmpty()) {
			return fullEntry;
		}
		Set<String> allNames = new HashSet<String>(fullEntry.keySet());
		allNames.remove(NGSIConstants.JSON_LD_ID);
		allNames.remove(NGSIConstants.JSON_LD_TYPE);
		allNames.removeAll(notificationAttrs);
		for (String name : allNames) {
			fullEntry.remove(name);
		}
		return fullEntry;
	}

	@SuppressWarnings("unchecked")
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

	@SuppressWarnings("unchecked")
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

	@SuppressWarnings("unchecked")
	public static Set<String> getTypesFromEntity(BaseRequest createRequest) {
		List<String> temp = (List<String>) createRequest.getPayload().get(NGSIConstants.JSON_LD_TYPE);
		return new HashSet<String>(temp);
	}

	@SuppressWarnings("unchecked")
	public static String getInstanceId(Map<String, Object> jsonElement) {
		Object instanceId = jsonElement.get(NGSIConstants.NGSI_LD_INSTANCE_ID);
		if (instanceId == null) {
			return null;
		}
		return ((List<Map<String, String>>) instanceId).get(0).get(NGSIConstants.JSON_LD_ID);
	}

	@SuppressWarnings("unchecked")
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
				entry.remove(REG_MODE_KEY);
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
			if (entry.containsKey(REG_MODE_KEY)) {
				regMode = (int) entry.get(REG_MODE_KEY);
			}
			removeIndex = -1;
			found = false;
			for (int i = 0; i < currentValue.size(); i++) {
				Map<String, Object> currentEntry = currentValue.get(i);
				currentRegMode = -1;
				if (currentEntry.containsKey(REG_MODE_KEY)) {
					currentRegMode = (int) currentEntry.get(REG_MODE_KEY);
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
			entry.put(REG_MODE_KEY, regMode);
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

	@SuppressWarnings({ "rawtypes", "unchecked" })
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

	public static void noConcise(Object object) {
		noConcise(object, null, null, 0);
	}

	private static void noConcise(Object object, Map<String, Object> parentMap, String keyOfObject, int level) {
		// Object is Map
		if (object instanceof Map<?, ?> map) {
			// Map have object but not type
			if (map.containsKey(NGSIConstants.OBJECT)) {
				((Map<String, Object>) map).put(NGSIConstants.TYPE, NGSIConstants.RELATIONSHIP);
			} else if (map.containsKey(NGSIConstants.OBJECT_LIST)) {
				((Map<String, Object>) map).put(NGSIConstants.TYPE, NGSIConstants.LISTRELATIONSHIP);
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

	public static Uni<List<Map<String, Object>>> getRemoteEntities(QueryRemoteHost remoteHost, WebClient webClient) {
		return webClient.getAbs(remoteHost.host() + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT + remoteHost.queryString())
				.send().onItem().transformToUni(response -> {
					List<Map<String, Object>> result = Lists.newArrayList();
					result.addAll(response.bodyAsJsonArray().getList());
					if (response.headers().contains("Next")) {
						QueryRemoteHost updatedHost = remoteHost.updatedDuplicate(response.headers().get("Next"));
						return getRemoteEntities(updatedHost, webClient).onItem().transform(nextResult -> {
							result.addAll(nextResult);
							return result;
						});
					}
					return Uni.createFrom().item(result);
				}).onFailure().recoverWithItem(e -> Lists.newArrayList());

	}

	public static Map<QueryRemoteHost, Set<String>> getRemoteQueries(String tenant, String[] id,
			TypeQueryTerm typeQuery, String idPattern, AttrsQueryTerm attrsQuery, QQueryTerm qQuery,
			GeoQueryTerm geoQuery, ScopeQueryTerm scopeQuery, LanguageQueryTerm langQuery,
			Table<String, String, List<RegistrationEntry>> tenant2CId2RegEntries, Context context,
			Map<String, Map<String, Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>>>> fullEntityCache, boolean onlyFullEntitiesDistributed) {
		return getRemoteQueries(id, typeQuery, idPattern, attrsQuery, qQuery, geoQuery, scopeQuery, langQuery,
				tenant2CId2RegEntries.row(tenant).values().iterator(), context, fullEntityCache);
	}

	public static Map<QueryRemoteHost, Set<String>> getRemoteQueries(String[] id, TypeQueryTerm typeQuery,
			String idPattern, AttrsQueryTerm attrsQuery, QQueryTerm qQuery, GeoQueryTerm geoQuery,
			ScopeQueryTerm scopeQuery, LanguageQueryTerm langQuery, Iterator<List<RegistrationEntry>> it,
			Context context,
			Map<String, Map<String, Tuple2<Map<String, Object>, Map<String, QueryRemoteHost>>>> fullEntityCache) {

		// ids, types, attrs, geo, scope
		Map<QueryRemoteHost, QueryInfos> remoteHost2QueryInfo = Maps.newHashMap();
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
				QueryRemoteHost hostToQuery = QueryRemoteHost.fromRemoteHost(regHost, null, regEntry.canDoIdQuery(),
						regEntry.canDoZip(), null);
				QueryInfos queryInfos = remoteHost2QueryInfo.get(hostToQuery);
				if (queryInfos == null) {
					queryInfos = new QueryInfos();
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
						if (attrsQuery != null && attrsQuery.getAttrs() != null && !attrsQuery.getAttrs().isEmpty()) {
							queryInfos.setAttrs(attrsQuery.getAttrs());
						}
					}
				}
			}

		}
		Map<QueryRemoteHost, Set<String>> result = Maps.newHashMap();
		for (Entry<QueryRemoteHost, QueryInfos> entry : remoteHost2QueryInfo.entrySet()) {
			QueryRemoteHost tmpHost = entry.getKey();
			String queryString = entry.getValue().toQueryString(context, typeQuery, geoQuery, langQuery, false,
					fullEntityCache, tmpHost);
			if (queryString != null) {
				result.put(
						new QueryRemoteHost(tmpHost.host(), tmpHost.tenant(), tmpHost.headers(), tmpHost.cSourceId(),
								tmpHost.canDoSingleOp(), tmpHost.canDoBatchOp(), tmpHost.regMode(), queryString,
								tmpHost.canDoEntityMap(), tmpHost.canDoZip(), tmpHost.remoteToken()),
						entry.getValue().getIds());
			}
		}
		return result;
	}

}
