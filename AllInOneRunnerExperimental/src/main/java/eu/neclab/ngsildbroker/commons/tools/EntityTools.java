package eu.neclab.ngsildbroker.commons.tools;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import eu.neclab.ngsildbroker.commons.datatypes.CSourceNotification;
import eu.neclab.ngsildbroker.commons.datatypes.CSourceRegistration;
import eu.neclab.ngsildbroker.commons.datatypes.Entity;
import eu.neclab.ngsildbroker.commons.datatypes.Notification;
import eu.neclab.ngsildbroker.commons.enums.TriggerReason;

public abstract class EntityTools {

	private static final String BROKER_PREFIX = "ngsildbroker:";

	public static URI getRandomID(String prefix) throws URISyntaxException {
		if (prefix == null) {
			prefix = ":";
		}
		if (!prefix.endsWith(":")) {
			prefix += ":";
		}
		URI result;

		result = new URI(BROKER_PREFIX + prefix + UUID.randomUUID().getLeastSignificantBits());
		return result;

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
		if(!newData.isEmpty()) {
				result.add(new CSourceNotification(getRandomID("csource"),data.get(0).getSubscriptionId(),new Date(now),TriggerReason.newlyMatching,newData, data.get(0).getErrorMsg(),data.get(0).getErrorType(),data.get(0).getShortErrorMsg(),data.get(0).isSuccess()));
		}
		if(!updatedData.isEmpty()) {
			result.add(new CSourceNotification(getRandomID("csource"),data.get(0).getSubscriptionId(),new Date(now),TriggerReason.updated,updatedData, data.get(0).getErrorMsg(),data.get(0).getErrorType(),data.get(0).getShortErrorMsg(),data.get(0).isSuccess()));
		}
		if(!deletedData.isEmpty()) {
			result.add(new CSourceNotification(getRandomID("csource"),data.get(0).getSubscriptionId(),new Date(now),TriggerReason.noLongerMatching,deletedData, data.get(0).getErrorMsg(),data.get(0).getErrorType(),data.get(0).getShortErrorMsg(),data.get(0).isSuccess()));
		}
		} catch (URISyntaxException e) {
			//left empty intentionally should never happen
			throw new AssertionError();
		}
			
		return result;
	}

	public static Notification squashNotifications(List<Notification> data) {
		List<Entity> newData = new ArrayList<Entity>();
		for (Notification notification : data) {
			newData.addAll(notification.getData());
		}
		return new Notification(data.get(0).getId(), System.currentTimeMillis(),
				data.get(0).getSubscriptionId(), newData, data.get(0).getErrorMsg(), data.get(0).getErrorType(),
				data.get(0).getShortErrorMsg(), data.get(0).isSuccess());
	}

}
