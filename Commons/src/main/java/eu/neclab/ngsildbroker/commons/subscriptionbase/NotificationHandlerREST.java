package eu.neclab.ngsildbroker.commons.subscriptionbase;

import java.util.Date;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ThreadLocalRandom;

import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;
import eu.neclab.ngsildbroker.commons.datatypes.Notification;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;

class NotificationHandlerREST extends BaseNotificationHandler {
	// Comment about webclient ...
	// it just doesn't work for us at the moment maybe we are doing something wrong
	// but for now its the restTemplate
//	private WebClient webClient;
//	webClient.post().uri(request.getSubscription().getNotification().getEndPoint().getUri())
//	.headers(httpHeadersOnWebClientBeingBuilt -> {
//		httpHeadersOnWebClientBeingBuilt.addAll(compacted.getHeaders());
//	}).bodyValue(compacted.getBody()).exchangeToMono(response -> {
//		if (response.statusCode().is4xxClientError() || response.statusCode().is5xxServerError()) {
//			logger.error("Failed to send notification with return code " + response.statusCode()
//					+ " subscription id: " + request.getSubscription().getId() + " notification id: "
//					+ notification.getId());
//			request.getSubscription().getNotification()
//					.setLastFailedNotification(new Date(System.currentTimeMillis()));
//			return Mono.just(Void.class);
//		} else {
//			return Mono.just(Void.class);
//
//		}
//	}).doOnError(onError -> {
//		logger.error("Failed to send notification retrying subscription id: "
//				+ request.getSubscription().getId() + " notification id: " + notification.getId());
//	}).doOnSuccess(response -> {
//		logger.info("success subscription id: " + request.getSubscription().getId() + " notification id: "
//				+ notification.getId());
//	}).retryWhen(Retry.backoff(5, Duration.ofSeconds(2))).doOnError(onError -> {
//		logger.error("Finally failed to send notification subscription id: "
//				+ request.getSubscription().getId() + " notification id: " + notification.getId());
//		logger.debug(onError.getMessage());
//		request.getSubscription().getNotification()
//				.setLastFailedNotification(new Date(System.currentTimeMillis()));
//		return;
//	}).onErrorResume(fallback -> {
//		return Mono.empty();
//	}).subscribe();

	private RestTemplate restTemplate;

	NotificationHandlerREST(RestTemplate restTemplate) {
		this.restTemplate = restTemplate;
	}

	@Override
	protected void sendReply(Notification notification, SubscriptionRequest request) throws Exception {
		ResponseEntity<String> compacted;
		compacted = notification.toCompactedJson();
		
		HttpEntity<String> entity = new HttpEntity<String>(compacted.getBody(), compacted.getHeaders());

		int retryCount = 5;
		boolean success = false;
		ThreadLocalRandom random = ThreadLocalRandom.current();
		while (true) {
			try {
				ResponseEntity<String> response = restTemplate.exchange(
						request.getSubscription().getNotification().getEndPoint().getUri(), HttpMethod.POST, entity,
						String.class);
				HttpStatus returnStatus = response.getStatusCode();
				if (returnStatus.is2xxSuccessful()) {
					logger.info("success subscription id: " + request.getSubscription().getId() + " notification id: "
							+ notification.getId());
					success = true;
					break;
				} else if (returnStatus.is3xxRedirection()) {
					logger.info("redirect");
					success = true;
					break;
				}
			} catch (HttpServerErrorException e) {
				if (retryCount == 0) {
					logger.error("finally failed to send notification subscription id: "
							+ request.getSubscription().getId() + " notification id: " + notification.getId());
					break;
				}
				int waitTime = random.nextInt(500, 5000);
				logger.error("failed to send notification subscription id: " + request.getSubscription().getId()
						+ " notification id: " + notification.getId(), e);
				logger.error("retrying " + retryCount + " times waiting " + waitTime + " ms");
				Thread.sleep(waitTime);
				retryCount--;
			}

		}
		if (!success) {
			request.getSubscription().getNotification().setLastFailedNotification(new Date(System.currentTimeMillis()));
		}
	}

}
