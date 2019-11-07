package eu.neclab.ngsildbroker.subscriptionmanager.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.Notification;
import eu.neclab.ngsildbroker.commons.interfaces.SubscriptionManager;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;

@RestController
@RequestMapping("/remotenotify")
public class NotificationController {
	
	@Autowired
	SubscriptionManager subscriptionManager;
	
	
	
	@RequestMapping(method=RequestMethod.POST, value = "/{id}")
	public void notify(@RequestBody String payload, @PathVariable(name = NGSIConstants.QUERY_PARAMETER_ID, required = false) String id) {
		Notification notification = DataSerializer.getNotification(payload);
		subscriptionManager.remoteNotify(id, notification);
	}

}
