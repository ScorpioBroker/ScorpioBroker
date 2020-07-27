package eu.neclab.ngsildbroker.subscriptionmanager.controller;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.Notification;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.SubscriptionManager;
import eu.neclab.ngsildbroker.commons.ldcontext.ContextResolverBasic;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;

@RestController
@RequestMapping("/remotenotify")
public class NotificationController {
	
	@Autowired
	SubscriptionManager subscriptionManager;
	
	@Autowired
	ContextResolverBasic resolver;
	
	@RequestMapping(method=RequestMethod.POST, value = "/{id}")
	public void notify(HttpServletRequest req, @RequestBody String payload, @PathVariable(name = NGSIConstants.QUERY_PARAMETER_ID, required = false) String id) {
		try {
			subscriptionManager.remoteNotify(id, DataSerializer.getNotification(resolver.expand(payload, HttpUtils.getAtContext(req),true, AppConstants.SUBSCRIPTIONS_URL_ID)));
		} catch (ResponseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
