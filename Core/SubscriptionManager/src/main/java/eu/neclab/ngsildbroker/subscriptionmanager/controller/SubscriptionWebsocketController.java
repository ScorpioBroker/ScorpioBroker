package eu.neclab.ngsildbroker.subscriptionmanager.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Controller;

@Controller
public class SubscriptionWebsocketController {

	
	@Autowired
	public SubscriptionWebsocketController() {
	}

	@MessageMapping("/incoming")
	@SendTo("/topic/outgoing")
	public String incoming(Message message) {
		System.out.println(message.getPayload());
		return "blaaaaa";
	}

	

}
