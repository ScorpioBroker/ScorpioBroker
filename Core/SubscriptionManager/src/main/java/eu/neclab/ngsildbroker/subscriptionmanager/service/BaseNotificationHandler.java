package eu.neclab.ngsildbroker.subscriptionmanager.service;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpCookie;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.ResponseEntity.BodyBuilder;
import org.springframework.http.server.RequestPath;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.util.MultiValueMap;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.datatypes.Notification;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.NotificationHandler;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;
import eu.neclab.ngsildbroker.commons.tools.EntityTools;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import reactor.core.publisher.Flux;

public abstract class BaseNotificationHandler implements NotificationHandler {

	protected abstract void sendReply(ResponseEntity<byte[]> reply, URI callback, Map<String, String> clientSettings)
			throws Exception;

	private final Logger logger = LogManager.getLogger(this.getClass());
	private SubscriptionService subscriptionManagerService;

	private ObjectMapper objectMapper;

	public BaseNotificationHandler(SubscriptionService subscriptionManagerService, ObjectMapper objectMapper) {
		this.subscriptionManagerService = subscriptionManagerService;
		this.objectMapper = objectMapper;

	}

	HashMap<String, Long> subId2LastReport = new HashMap<String, Long>();
	ArrayListMultimap<String, Notification> subId2Notifications = ArrayListMultimap.create();
	Timer executor = new Timer(true);

	@Override
	public void notify(Notification notification, URI callback, String acceptHeader, String subId, List<Object> context,
			int throttling, Map<String, String> clientSettings, String tenantId) {

		ArrayList<String> subIds = new ArrayList<String>();
		subIds.add(subId);

		if (throttling > 0) {
			synchronized (subId2Notifications) {
				subId2Notifications.put(subId, notification);
				Long lastReport = subId2LastReport.get(subId);
				Long now = System.currentTimeMillis() / 1000;
				if (lastReport == null) {
					lastReport = 0l;
				}
				Long delay = 0l;
				Long delta = now - lastReport;
				if (delta < throttling) {
					delay = delta;
				}
				executor.schedule(new TimerTask() {

					@Override
					public void run() {
						synchronized (subId2Notifications) {
							Notification sendOutNotification = EntityTools
									.squashNotifications(subId2Notifications.removeAll(subId));
							String jsonStr = DataSerializer.toJson(sendOutNotification);
							Long now = System.currentTimeMillis();
							subId2LastReport.put(subId, now / 1000);
							subscriptionManagerService.reportNotification(tenantId, subId, now);
							try {
								logger.trace("Sending notification");
								logger.debug("Json to be sent: " + jsonStr);
								ResponseEntity<byte[]> reply = generateNotificationResponse(acceptHeader, jsonStr,
										context);
								logger.debug("body to be sent: " + reply.getBody().toString());
								sendReply(reply, callback, clientSettings);
								subscriptionManagerService.reportSuccessfulNotification(tenantId, subId, now);
							} catch (Exception e) {
								logger.error("Exception ::", e);
								subscriptionManagerService.reportFailedNotification(tenantId, subId, now);
								e.printStackTrace();
							}
						}

					}
				}, delay);

			}

		} else {
			String jsonStr = DataSerializer.toJson(notification);
			logger.debug("Sending notification");
			ResponseEntity<byte[]> reply;
			long now = System.currentTimeMillis();
			try {
				reply = generateNotificationResponse(acceptHeader, jsonStr, context);
				logger.debug(new String(reply.getBody()));
				sendReply(reply, callback, clientSettings);
				subscriptionManagerService.reportNotification(tenantId, subId, now);
			} catch (Exception e) {
				logger.error("Excep	tion ::", e);
				subscriptionManagerService.reportFailedNotification(tenantId, subId, now);
				e.printStackTrace();
			}

		}

	}

	private ResponseEntity<byte[]> generateNotificationResponse(String acceptHeader, String body, List<Object> context)
			throws ResponseException {
		ServerHttpRequest request = new ServerHttpRequest() {
			HttpHeaders headers = new HttpHeaders();
			{
				headers.add(HttpHeaders.ACCEPT, acceptHeader);
			}

			@Override
			public Flux<DataBuffer> getBody() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public HttpHeaders getHeaders() {
				// TODO Auto-generated method stub
				return headers;
			}

			@Override
			public URI getURI() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public String getMethodValue() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public MultiValueMap<String, String> getQueryParams() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public RequestPath getPath() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public String getId() {
				// TODO Auto-generated method stub
				return null;
			}

			@Override
			public MultiValueMap<String, HttpCookie> getCookies() {
				// TODO Auto-generated method stub
				return null;
			}
		};

		ResponseEntity<byte[]> temp = HttpUtils.generateReply(request, body, null, context);
		JsonNode jsonTree;
		try {
			jsonTree = objectMapper.readTree(temp.getBody());
			if (jsonTree.get("data").isArray()) {
				return temp;
			}
			ArrayNode dataArray = objectMapper.createArrayNode();

			dataArray.add(jsonTree.get("data"));
			((ObjectNode) jsonTree).set("data", dataArray);

			BodyBuilder builder = ResponseEntity.status(HttpStatus.ACCEPTED);

			return builder.headers(temp.getHeaders()).body(objectMapper.writeValueAsBytes(jsonTree));

		} catch (IOException e) {
			// Left empty intentionally
		}
		return temp;

	}

}
