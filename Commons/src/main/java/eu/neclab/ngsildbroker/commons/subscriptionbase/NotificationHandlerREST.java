package eu.neclab.ngsildbroker.commons.subscriptionbase;

import java.util.Date;
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.client.WebClient;

import eu.neclab.ngsildbroker.commons.datatypes.Notification;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import reactor.core.publisher.Mono;

class NotificationHandlerREST extends BaseNotificationHandler {

	private WebClient webClient;

	NotificationHandlerREST(WebClient webClient) {
		this.webClient = webClient;

	}

	@Override
	protected void sendReply(Notification notification, SubscriptionRequest request) throws Exception {
		webClient.post().uri(request.getSubscription().getNotification().getEndPoint().getUri())
				.headers(httpHeadersOnWebClientBeingBuilt -> {
					request.getHeaders().entries().forEach(entry -> {
						httpHeadersOnWebClientBeingBuilt.add(entry.getKey(), entry.getValue());
					});
				}).bodyValue(notification.toJson()).exchangeToMono(response -> {
					if (response.statusCode().equals(HttpStatus.OK)) {
						return Mono.just(Void.class);
					} else {

						logger.error("Failed to send notification");
						request.getSubscription().getNotification()
								.setLastFailedNotification(new Date(System.currentTimeMillis()));
						return Mono.just(Void.class);
					}
				}).doOnError(onError -> {
					logger.error("Failed to send notification retrying");
				}).doOnSuccess(response -> {
					System.err.println("SUCCESSFULL SEND");
				}).retry(5).doOnError(onError -> {
					logger.error("Finally failed to send notification");
					logger.debug(onError.getMessage());
					request.getSubscription().getNotification()
							.setLastFailedNotification(new Date(System.currentTimeMillis()));
					return;
				}).subscribe();

	}

}
