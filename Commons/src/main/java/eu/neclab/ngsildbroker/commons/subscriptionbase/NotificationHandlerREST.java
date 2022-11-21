package eu.neclab.ngsildbroker.commons.subscriptionbase;

import java.net.ConnectException;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.http.HttpResponse;
import org.apache.http.client.fluent.Request;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
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

	@Value("${scorpio.subscription.maxretries:5}")
	int maxRetries;

	NotificationHandlerREST(SubscriptionInfoDAOInterface baseSubscriptionInfoDAO, RestTemplate restTemplate) {
		super(baseSubscriptionInfoDAO);
		this.restTemplate = restTemplate;
	}

	@Override
	protected void sendReply(Notification notification, SubscriptionRequest request) throws Exception {
		ResponseEntity<String> compacted;
		compacted = notification.toCompactedJson();
		Request req = Request.Post(request.getSubscription().getNotification().getEndPoint().getUri());

		for (Entry<String, List<String>> entry : compacted.getHeaders().entrySet()) {
			for (String value : entry.getValue()) {
				req = req.addHeader(entry.getKey(), value);
			}
		}
		if (request.getSubscription().getNotification().getEndPoint().getReceiverInfo() != null) {
			for (Entry<String, String> entry : request.getSubscription().getNotification().getEndPoint()
					.getReceiverInfo().entries()) {
				req = req.addHeader(entry.getKey(), entry.getValue().toString());
			}
		}
		req = req.bodyByteArray(compacted.getBody().getBytes());
		int retryCount = maxRetries;
		boolean success = false;
		ThreadLocalRandom random = ThreadLocalRandom.current();
		while (true) {
			try {
				HttpResponse response = req.execute().returnResponse();
				HttpStatus returnStatus = HttpStatus.valueOf(response.getStatusLine().getStatusCode());
				if (returnStatus.is2xxSuccessful()) {
					logger.info("success subscription id: " + request.getSubscription().getId() + " notification id: "
							+ notification.getId());
					success = true;
					break;
				} else if (returnStatus.is3xxRedirection()) {
					logger.info("redirect");
					success = true;
					break;
				} else {

					if (retryCount == 0) {
						logger.error("finally failed to send notification subscription id: "
								+ request.getSubscription().getId() + " notification id: " + notification.getId());
						break;
					}
					int waitTime = random.nextInt(500, 5000);
					logger.error("failed to send notification subscription id: " + request.getSubscription().getId()
							+ " notification id: " + notification.getId() + ". "
							+ response.getStatusLine().getStatusCode() + " "
							+ response.getStatusLine().getReasonPhrase() + " " + response.getEntity().toString());
					logger.error("retrying " + retryCount + " times waiting " + waitTime + " ms");
					Thread.sleep(waitTime);
					retryCount--;
				}
			} catch (ConnectException exception) {
				// TODO: handle exception
			}

		}

		if (!success) {
			request.getSubscription().getNotification().setLastFailedNotification(new Date(System.currentTimeMillis()));
		}
	}

}
