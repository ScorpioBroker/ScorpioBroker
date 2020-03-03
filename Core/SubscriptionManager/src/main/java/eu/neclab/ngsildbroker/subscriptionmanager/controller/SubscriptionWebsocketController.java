package eu.neclab.ngsildbroker.subscriptionmanager.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Controller;

//@Controller
public class SubscriptionWebsocketController {

	private final SimpMessagingTemplate simpMessagingTemplate;

	//@Autowired
	public SubscriptionWebsocketController(SimpMessagingTemplate simpMessagingTemplate) {
		this.simpMessagingTemplate = simpMessagingTemplate;
	}

	//@MessageMapping("/incoming")
	@SendTo("/topic/outgoing")
	public String incoming(Message message) {
				
		return "blaaaaa";
	}

	//@Scheduled(fixedRate = 15000L)
	public void timed() {
		simpMessagingTemplate.convertAndSend("/topic/outgoing", String.format("Application on port pushed a message!"));
	}

}
