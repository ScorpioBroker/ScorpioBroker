package eu.neclab.ngsildbroker.commons.tools;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.BaseProperty;
import eu.neclab.ngsildbroker.commons.datatypes.GeoProperty;
import eu.neclab.ngsildbroker.commons.datatypes.LDGeoQuery;
import eu.neclab.ngsildbroker.commons.datatypes.Notification;
import eu.neclab.ngsildbroker.commons.datatypes.Property;
import eu.neclab.ngsildbroker.commons.datatypes.PropertyEntry;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;

public abstract class EntityTools {

	private static final String BROKER_PREFIX = "ngsildbroker:";

	public static String getRandomID(String prefix) {
		if (prefix == null) {
			prefix = ":";
		}
		if (!prefix.endsWith(":")) {
			prefix += ":";
		}
		return BROKER_PREFIX + prefix + UUID.randomUUID().getLeastSignificantBits();

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
				new ArrayList<Object>(context), data.get(0).getHeaders(), data.get(0).getContentType());
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
		List<String> notificationAttrs = subscription.getSubscription().getNotification().getAttributeNames();
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
					case NGSIConstants.NGSI_LD_DATE_TIME:
						prop = generateFakeProperty(typeString, ((List<Map<String, Object>>) value).get(0));
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
		List<String> temp = (List<String>) createRequest.getFinalPayload().get(NGSIConstants.JSON_LD_TYPE);
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

}
