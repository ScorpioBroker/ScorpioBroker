package eu.neclab.ngsildbroker.commons.tools;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import java.util.UUID;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.BaseProperty;
import eu.neclab.ngsildbroker.commons.datatypes.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.CSourceNotification;
import eu.neclab.ngsildbroker.commons.datatypes.CSourceRegistration;
import eu.neclab.ngsildbroker.commons.datatypes.GeoProperty;
import eu.neclab.ngsildbroker.commons.datatypes.Notification;
import eu.neclab.ngsildbroker.commons.datatypes.Property;
import eu.neclab.ngsildbroker.commons.datatypes.PropertyEntry;
import eu.neclab.ngsildbroker.commons.datatypes.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.enums.TriggerReason;

public abstract class EntityTools {

	private static final String BROKER_PREFIX = "ngsildbroker:";

	public static String getRandomID(String prefix) throws URISyntaxException {
		if (prefix == null) {
			prefix = ":";
		}
		if (!prefix.endsWith(":")) {
			prefix += ":";
		}
		return BROKER_PREFIX + prefix + UUID.randomUUID().getLeastSignificantBits();

	}

	public static List<CSourceNotification> squashCSourceNotifications(List<CSourceNotification> data) {
		List<CSourceRegistration> newData = new ArrayList<CSourceRegistration>();
		List<CSourceRegistration> updatedData = new ArrayList<CSourceRegistration>();
		List<CSourceRegistration> deletedData = new ArrayList<CSourceRegistration>();
		List<CSourceNotification> result = new ArrayList<CSourceNotification>();
		for (CSourceNotification notification : data) {
			switch (notification.getTriggerReason()) {
			case newlyMatching:
				newData.addAll(notification.getData());
				break;
			case updated:
				updatedData.addAll(notification.getData());
				break;
			case noLongerMatching:
				deletedData.addAll(notification.getData());
				break;
			default:
				break;

			}

		}
		long now = System.currentTimeMillis();
		try {
			if (!newData.isEmpty()) {
				result.add(new CSourceNotification(getRandomID("csource"), data.get(0).getSubscriptionId(),
						new Date(now), TriggerReason.newlyMatching, newData, data.get(0).getErrorMsg(),
						data.get(0).getErrorType(), data.get(0).getShortErrorMsg(), data.get(0).isSuccess()));
			}
			if (!updatedData.isEmpty()) {
				result.add(new CSourceNotification(getRandomID("csource"), data.get(0).getSubscriptionId(),
						new Date(now), TriggerReason.updated, updatedData, data.get(0).getErrorMsg(),
						data.get(0).getErrorType(), data.get(0).getShortErrorMsg(), data.get(0).isSuccess()));
			}
			if (!deletedData.isEmpty()) {
				result.add(new CSourceNotification(getRandomID("csource"), data.get(0).getSubscriptionId(),
						new Date(now), TriggerReason.noLongerMatching, deletedData, data.get(0).getErrorMsg(),
						data.get(0).getErrorType(), data.get(0).getShortErrorMsg(), data.get(0).isSuccess()));
			}
		} catch (URISyntaxException e) {
			// left empty intentionally should never happen
			throw new AssertionError();
		}

		return result;
	}

	public static Notification squashNotifications(List<Notification> data) {
		List<Map<String, Object>> newData = new ArrayList<Map<String, Object>>();
		for (Notification notification : data) {
			newData.addAll(notification.getData());
		}
		return new Notification(data.get(0).getId(), System.currentTimeMillis(), data.get(0).getSubscriptionId(),
				newData, data.get(0).getErrorMsg(), data.get(0).getErrorType(), data.get(0).getShortErrorMsg(),
				data.get(0).isSuccess());
	}

	public static GeoProperty getLocation(Map<String, Object> fullEntry, String geoPropertyName) {
		Object obj = fullEntry.get(geoPropertyName);
		if (obj == null) {
			return null;
		}
		return SerializationTools.parseGeoProperty((List<Map<String, Object>>) obj, geoPropertyName);
	}

	public static Map<String, Object> clearBaseProps(Map<String, Object> fullEntry, SubscriptionRequest subscription) {
		List<String> attrNames = subscription.getSubscription().getAttributeNames();
		if (attrNames == null || attrNames.isEmpty()) {
			return fullEntry;
		}
		Set<String> allNames = fullEntry.keySet();
		allNames.remove(NGSIConstants.JSON_LD_ID);
		allNames.remove(NGSIConstants.JSON_LD_TYPE);
		allNames.removeAll(attrNames);
		for (String name : allNames) {
			fullEntry.remove(name);
		}
		return fullEntry;
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
		String key = "urn:ngsi-ld:csourceregistration:" + UUID.fromString("" + resolved.hashCode()).toString();
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
			boolean multiValue = true;
			if (value.size() == 1) {
				multiValue = false;
			}
			Map<String, Object> tmp = value.get(0);
			Object type = tmp.get(NGSIConstants.JSON_LD_TYPE);
			BaseProperty prop;
			if (type == null) {
				prop = generateFakeProperty(key, tmp);
			} else {
				String typeString = ((List<String>) type).get(0);
				switch (typeString) {
				case NGSIConstants.NGSI_LD_GEOPROPERTY:
					prop = SerializationTools.parseGeoProperty((List<Map<String, Object>>) value, key);
					continue;
				case NGSIConstants.NGSI_LD_RELATIONSHIP:
					prop = SerializationTools.parseRelationship((List<Map<String, Object>>) value, key);
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

	public static Set<String> getTypesFromEntity(BaseRequest createRequest) {
		List<String> temp = (List<String>) createRequest.getFinalPayload().get(NGSIConstants.JSON_LD_TYPE);
		return new HashSet<String>(temp);
	}

}
