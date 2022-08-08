package eu.neclab.ngsildbroker.commons.subscriptionbase;

import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;

import org.jboss.resteasy.reactive.RestResponse;

import eu.neclab.ngsildbroker.commons.datatypes.Notification;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import io.vertx.core.http.impl.headers.HeadersMultiMap;
import io.vertx.mutiny.core.MultiMap;
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.ext.web.client.HttpRequest;
import io.vertx.mutiny.ext.web.client.WebClient;

class NotificationHandlerREST extends BaseNotificationHandler {
	private WebClient webClient;

	NotificationHandlerREST(WebClient webClient) {
		this.webClient = webClient;
	}

	@Override
	protected void sendReply(Notification notification, SubscriptionRequest request) throws Exception {
		RestResponse<String> compacted = notification.toCompactedJson();
		HttpRequest<Buffer> httpReq = webClient
				.postAbs(request.getSubscription().getNotification().getEndPoint().getUri().toString())
				.followRedirects(true);
		for (Entry<String, List<Object>> header : compacted.getHeaders().entrySet()) {
			for (Object objEntry : header.getValue()) {
				httpReq = httpReq.putHeader(header.getKey(), objEntry.toString());
			}
		}
		if (request.getSubscription().getNotification().getEndPoint().getReceiverInfo() != null) {
			MultiMap headers = httpReq.headers();

			HeadersMultiMap receiverInfo = MicroServiceUtils
					.getHeaders(request.getSubscription().getNotification().getEndPoint().getReceiverInfo());
			for (String headerName : receiverInfo.names()) {
				List<String> value = receiverInfo.getAll(headerName);
				if (value.size() == 1) {
					headers.set(headerName, value.get(0));
				} else {
					headers.set(headerName, value);
				}
			}

		}
		logger.debug("should send notification now");
		logger.debug(httpReq.toString());
		logger.debug("Content length: " + compacted.getEntity().length());
		int retryCount = 5;
		httpReq.sendBuffer(Buffer.buffer(compacted.getEntity())).onFailure().retry()
				.withBackOff(Duration.ofSeconds(5), Duration.ofSeconds(60)).atMost(retryCount).onItem()
				.transform(result -> {
					logger.debug("did try to send notification");
					int statusCode = result.statusCode();
					logger.debug("status code: " + statusCode);
					if (statusCode >= 200 && statusCode < 300) {
						logger.info("success subscription id: " + request.getSubscription().getId()
								+ " notification id: " + notification.getId());
						request.getSubscription().getNotification()
								.setLastSuccessfulNotification(new Date(System.currentTimeMillis()));
					} else {
						logger.error("Failed to send notification subscription id: " + request.getSubscription().getId()
								+ " notification id: " + notification.getId());
						logger.error("Status code: " + statusCode);
						logger.error("Status message: " + result.statusMessage());
						logger.error("Response: " + result.bodyAsString());
						request.getSubscription().getNotification()
								.setLastFailedNotification(new Date(System.currentTimeMillis()));
					}
					return null;
				}).onFailure().recoverWithItem(t -> {
					logger.error("finally failed to send notification subscription id: "
							+ request.getSubscription().getId() + " notification id: " + notification.getId());
					return null;
				}).await().indefinitely();
	}

}
