package eu.neclab.ngsildbroker.commons.subscriptionbase;

import java.util.Date;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ThreadLocalRandom;

import org.jboss.resteasy.reactive.RestResponse;

import eu.neclab.ngsildbroker.commons.datatypes.Notification;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;

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

		int retryCount = 5;
		boolean success = false;
		ThreadLocalRandom random = ThreadLocalRandom.current();
		while (true) {
			HttpResponse<Buffer> result = httpReq.sendBuffer(Buffer.buffer(compacted.getEntity())).result();
			int statusCode = result.statusCode();
			if (statusCode >= 200 && statusCode < 300) {
				logger.info("success subscription id: " + request.getSubscription().getId() + " notification id: "
						+ notification.getId());
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
						+ " notification id: " + notification.getId(), result.bodyAsString());
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
