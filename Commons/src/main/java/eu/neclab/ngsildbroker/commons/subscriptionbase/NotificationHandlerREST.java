package eu.neclab.ngsildbroker.commons.subscriptionbase;

import java.net.URI;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.reactive.function.client.WebClient;

import com.fasterxml.jackson.databind.ObjectMapper;

import reactor.core.publisher.Mono;

class NotificationHandlerREST extends BaseNotificationHandler {

	private static final Logger logger = LoggerFactory.getLogger(NotificationHandlerREST.class);
	
	private WebClient webClient;

	NotificationHandlerREST(BaseSubscriptionService subscriptionManagerService,
			ObjectMapper objectMapper, WebClient webClient) {
		super(subscriptionManagerService, objectMapper);
		this.webClient = webClient;
		
	}

	@Override
	protected void sendReply(ResponseEntity<String> reply, URI callback, Map<String, String> clientSettings) throws Exception {
		webClient.post().uri(callback).headers( httpHeadersOnWebClientBeingBuilt -> { 
	         httpHeadersOnWebClientBeingBuilt.addAll( reply.getHeaders() );
	    }).bodyValue(reply.getBody())
	    .exchangeToMono(response -> {
	         if (response.statusCode().equals(HttpStatus.OK)) {
	             return Mono.just(Void.class);
	         }
	         else {
	        	 logger.error("Failed to send notification");
	             return response.createException().flatMap(Mono::error);
	         }
	     }).retry(5).block();
	}

}
