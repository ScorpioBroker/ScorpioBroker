package eu.neclab.ngsildbroker.registryhandler.service;

import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClient;

import eu.neclab.ngsildbroker.commons.datatypes.CSourceNotification;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.interfaces.CSourceNotificationHandler;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;
import reactor.core.publisher.Mono;


public class CSourceNotificationHandlerREST implements CSourceNotificationHandler {

	private final static Logger logger = LogManager.getLogger(CSourceNotificationHandlerREST.class);
	private WebClient webClient;

	public CSourceNotificationHandlerREST(WebClient webClient) {
		this.webClient = webClient;
	}

	@Override
	public void notify(CSourceNotification notification, Subscription sub) {
		String regString = DataSerializer.toJson(notification);
		// TODO rework when storage of sub context is done
		// regString = contextResolver.simplify(regString,
		// contextResolver.getContextAsSet(sub.getId().toString()),
		// true).getSimplifiedCompletePayload();
		HashMap<String, String> addHeaders = new HashMap<String, String>();
		if (sub.getNotification().getEndPoint().getAccept() != null) {
			addHeaders.put("accept", sub.getNotification().getEndPoint().getAccept());
		}
		webClient.post().uri(sub.getNotification().getEndPoint().getUri())
				.accept(MediaType.valueOf(sub.getNotification().getEndPoint().getAccept())).bodyValue(regString)
				.exchangeToMono(response -> {
					if (response.statusCode().equals(HttpStatus.OK)) {
						return Mono.just(Void.class);
					} else {
						logger.error("Failed to send notification");
						return response.createException().flatMap(Mono::error);
					}
				}).retry(5).block();

	}

}
