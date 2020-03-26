package eu.neclab.ngsildbroker.commons.interfaces;

import java.net.URI;
import java.util.List;
import java.util.Map;

import eu.neclab.ngsildbroker.commons.datatypes.Notification;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:40:09
 */
public interface NotificationHandler {

	/**
	 * 
	 * @param notification
	 * @param ldContext 
	 */
	public void notify(Notification notification, URI callback, String acceptHeader, String subId, List<Object> context, int throttling, Map<String, String> clientSettings);

}