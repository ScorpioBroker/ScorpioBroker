package eu.neclab.ngsildbroker.commons.subscriptionbase;

import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.resteasy.reactive.RestResponse;

import com.google.common.net.HttpHeaders;

import eu.neclab.ngsildbroker.commons.datatypes.Notification;
import eu.neclab.ngsildbroker.commons.datatypes.NotificationParam;
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
	protected void sendReply(Notification notification, SubscriptionRequest request, int maxRetries) throws Exception {
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
		sendHttpReq(httpReq, compacted.getEntity(), request.getSubscription().getId(), notification.getId(),
				request.getSubscription().getNotification(), 0, maxRetries);

	}

	private void sendHttpReq(HttpRequest<Buffer> httpReq, String entity, String subId, String notificationId,
			NotificationParam notification, int retry, int maxRetries) {
		if (retry == maxRetries) {
			return;
		}
		int retryCount = maxRetries;
		httpReq.sendBuffer(Buffer.buffer(entity)).onFailure().retry()
				.withBackOff(Duration.ofSeconds(5), Duration.ofSeconds(60)).atMost(retryCount).onItem()
				.transform(result -> {
					logger.debug("did try to send notification");
					int statusCode = result.statusCode();
					logger.debug("status code: " + statusCode);
					if (statusCode >= 200 && statusCode < 300) {
						logger.info("success subscription id: " + subId + " notification id: " + notificationId);
						notification.setLastSuccessfulNotification(new Date(System.currentTimeMillis()));
					} else if (statusCode == 302) {
						HttpRequest<Buffer> followHttpReq = webClient.postAbs(result.getHeader(HttpHeaders.LOCATION))
								.followRedirects(true);
						followHttpReq.headers().setAll(httpReq.headers());
						sendHttpReq(followHttpReq, entity, subId, notificationId, notification, retryCount + 1,
								maxRetries);
					} else {
						logger.error("Failed to send notification subscription id: " + subId + " notification id: "
								+ notificationId);
						logger.error("Status code: " + statusCode);
						logger.error("Status message: " + result.statusMessage());
						logger.error("Response: " + result.bodyAsString());
						notification.setLastFailedNotification(new Date(System.currentTimeMillis()));
					}
					return null;
				}).onFailure().recoverWithItem(t -> {
					logger.error("finally failed to send notification subscription id: " + subId + " notification id: "
							+ notificationId);
					return null;
				}).await().indefinitely();
	}

}
