package eu.neclab.ngsildbroker.commons.subscriptionbase;

import java.time.Duration;
import java.util.Date;
import org.springframework.web.reactive.function.client.WebClient;

import eu.neclab.ngsildbroker.commons.datatypes.Notification;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

class NotificationHandlerREST extends BaseNotificationHandler {

	private WebClient webClient;

	NotificationHandlerREST(WebClient webClient) {
		this.webClient = webClient;

	}

	@Override
	protected void sendReply(Notification notification, SubscriptionRequest request, int internalState)
			throws Exception {
		webClient.post().uri(request.getSubscription().getNotification().getEndPoint().getUri())
				.headers(httpHeadersOnWebClientBeingBuilt -> {
					request.getHeaders().entries().forEach(entry -> {
						httpHeadersOnWebClientBeingBuilt.add(entry.getKey(), entry.getValue());
					});
				}).bodyValue(notification.toCompactedJsonString()).exchangeToMono(response -> {
					if (response.statusCode().is4xxClientError() || response.statusCode().is5xxServerError()) {
						logger.error("Failed to send notification with return code " + response.statusCode()
								+ " subscription id: " + request.getSubscription().getId() + " notification id: "
								+ notification.getId());
						request.getSubscription().getNotification()
								.setLastFailedNotification(new Date(System.currentTimeMillis()));
						return Mono.just(Void.class);
					} else {
						return Mono.just(Void.class);

					}
				}).doOnError(onError -> {
					logger.error("Failed to send notification retrying subscription id: "
							+ request.getSubscription().getId() + " notification id: " + notification.getId());
				}).doOnSuccess(response -> {
					logger.info("success subscription id: " + request.getSubscription().getId() + " notification id: "
							+ notification.getId());
				}).retryWhen(Retry.backoff(5, Duration.ofSeconds(2))).doOnError(onError -> {
					logger.error("Finally failed to send notification subscription id: "
							+ request.getSubscription().getId() + " notification id: " + notification.getId());
					logger.debug(onError.getMessage());
					request.getSubscription().getNotification()
							.setLastFailedNotification(new Date(System.currentTimeMillis()));
					return;
				}).onErrorResume(fallback -> {
					return Mono.empty();
				}).subscribe();

	}

}
