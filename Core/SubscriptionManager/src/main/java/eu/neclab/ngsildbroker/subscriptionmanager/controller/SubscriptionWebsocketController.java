package eu.neclab.ngsildbroker.subscriptionmanager.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Controller;

@Controller
public class SubscriptionWebsocketController {

	
	@Autowired
	public SubscriptionWebsocketController() {
	}

	@SuppressWarnings("rawtypes")//in dev
	@MessageMapping("/incoming")
	@SendTo("/topic/outgoing")
	public String incoming(Message message) {
		
		return "blaaaaa";
	}

	

}
