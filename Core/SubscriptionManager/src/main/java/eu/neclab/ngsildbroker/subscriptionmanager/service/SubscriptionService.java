package eu.neclab.ngsildbroker.subscriptionmanager.service;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.annotation.PreDestroy;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import com.github.jsonldjava.utils.JsonUtils;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.BatchInfo;
import eu.neclab.ngsildbroker.commons.datatypes.InternalNotification;
import eu.neclab.ngsildbroker.commons.datatypes.Notification;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.subscriptionbase.BaseSubscriptionService;
import eu.neclab.ngsildbroker.commons.subscriptionbase.SubscriptionInfoDAOInterface;
import eu.neclab.ngsildbroker.commons.tools.EntityTools;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;

@Service
public class SubscriptionService extends BaseSubscriptionService {
	@Autowired
	@Qualifier("subdao")
	SubscriptionInfoDAOInterface subService;

	private JsonLdOptions opts = new JsonLdOptions(JsonLdOptions.JSON_LD_1_1);
	private HashMap<String, SubscriptionRequest> remoteNotifyCallbackId2InternalSub = new HashMap<String, SubscriptionRequest>();
	private HashMap<String, String> internalSubId2RemoteNotifyCallbackId2 = new HashMap<String, String>();
	private HashMap<String, String> internalSubId2ExternalEndpoint = new HashMap<String, String>();

	@Value("${scorpio.topics.internalregsub}")
	private String INTERNAL_SUBSCRIPTION_TOPIC;

	@Value("${scorpio.topics.subsync}")
	private String SUB_SYNC_TOPIC;

	@Autowired
	private MicroServiceUtils microServiceUtils;

	@Override
	protected SubscriptionInfoDAOInterface getSubscriptionInfoDao() {
		return subService;
	}

	@Override
	protected Set<String> getTypesFromEntry(BaseRequest createRequest) {
		return EntityTools.getTypesFromEntity(createRequest);
	}

	@Override
	protected Notification getNotification(SubscriptionRequest request, List<Map<String, Object>> dataList,
			int triggerReason) {
		return new Notification(EntityTools.getRandomID("notification:"), NGSIConstants.NOTIFICATION,
				System.currentTimeMillis(), request.getSubscription().getId(), dataList, -1, request.getContext(),
				request.getHeaders(), request.getSubscription().getNotification().getEndPoint().getAccept());
	}

	@Override
	protected boolean sendInitialNotification() {
		return false;
	}

	@PreDestroy
	private void unsubscribeToAllRemote() {
		for (String entry : internalSubId2ExternalEndpoint.values()) {
			restTemplate.delete(entry);
		}
	}

	@Override
	public void unsubscribe(String id, ArrayListMultimap<String, String> headers) throws ResponseException {
		unsubscribeRemote(id);
		SubscriptionRequest request = tenant2subscriptionId2Subscription.get(HttpUtils.getInternalTenant(headers), id);
		if (request != null) {
			// let super unsubscribe take care of further error handling
			request.setRequestType(AppConstants.DELETE_REQUEST);
			sendToKafka(id, request);

		}
		super.unsubscribe(id, headers);

	}

	private void sendToKafka(String id, SubscriptionRequest request) {
		new Thread() {
			@Override
			public void run() {
				kafkaTemplate.send(INTERNAL_SUBSCRIPTION_TOPIC, id, request);
			}
		}.start();
	}

	@Override
	public String subscribe(SubscriptionRequest subscriptionRequest) throws ResponseException {
		String result = super.subscribe(subscriptionRequest);
		sendToKafka(subscriptionRequest.getSubscription().getId(), subscriptionRequest);
		return result;
	}

	@Override
	public void updateSubscription(SubscriptionRequest subscriptionRequest) throws ResponseException {
		super.updateSubscription(subscriptionRequest);
		sendToKafka(subscriptionRequest.getSubscription().getId(), subscriptionRequest);
	}

	public void subscribeToRemote(SubscriptionRequest subscriptionRequest, InternalNotification notification) {
		if (subscriptionRequest == null) {
			// this can happen when sub is already deleted but a notification for it still
			// arrives.
			return;
		}
		new Thread() {

			@Override
			public void run() {
				Subscription remoteSub = new Subscription(subscriptionRequest.getSubscription());
				remoteSub.getNotification().getEndPoint().setUri(prepareNotificationServlet(subscriptionRequest));
				String body;
				try {
					Map<String, Object> expandedBody = remoteSub.toJson();
					expandedBody.remove(NGSIConstants.NGSI_LD_STATUS);
					expandedBody.remove(NGSIConstants.JSON_LD_ID);
					body = JsonUtils.toPrettyString(
							JsonLdProcessor.compact(expandedBody, subscriptionRequest.getContext(), opts));
				} catch (Exception e) {
					throw new AssertionError();
				}
				for (Map<String, Object> entry : notification.getData()) {
					HttpHeaders additionalHeaders = HttpUtils.getAdditionalHeaders(entry,
							subscriptionRequest.getContext(),
							Arrays.asList(remoteSub.getNotification().getEndPoint().getAccept()));
					additionalHeaders.add(HttpHeaders.CONTENT_TYPE, "application/ld+json");
					String remoteEndpoint = getRemoteEndPoint(entry);
					StringBuilder temp = new StringBuilder(remoteEndpoint);
					if (remoteEndpoint.endsWith("/")) {
						temp.deleteCharAt(remoteEndpoint.length() - 1);
					}
					temp.append(AppConstants.SUBSCRIPTIONS_URL);
					HttpEntity<String> entity = new HttpEntity<String>(body, additionalHeaders);

					ResponseEntity<String> response = restTemplate.exchange(temp.toString(), HttpMethod.POST, entity,
							String.class);
					if (response.getStatusCode().is2xxSuccessful()) {
						String locationHeader = response.getHeaders().getFirst(HttpHeaders.LOCATION);
						//check if it's a relative path
						if(locationHeader.charAt(0) == '/') {
							locationHeader = remoteEndpoint + locationHeader;
						}
						internalSubId2ExternalEndpoint.put(subscriptionRequest.getSubscription().getId(),
								locationHeader);
					}
				}
			}
		}.start();

	}

	private URI prepareNotificationServlet(SubscriptionRequest subToCheck) {

		String uuid = Long.toString(UUID.randomUUID().getLeastSignificantBits());
		remoteNotifyCallbackId2InternalSub.put(uuid, subToCheck);
		internalSubId2RemoteNotifyCallbackId2.put(subToCheck.getId(), uuid);
		StringBuilder url = new StringBuilder(microServiceUtils.getGatewayURL().toString()).append("/remotenotify/")
				.append(uuid);
		try {
			return new URI(url.toString());
		} catch (URISyntaxException e) {
			logger.error("Exception ::", e);
			// should never happen
			return null;
		}

	}

	public void remoteNotify(String id, Map<String, Object> notification) {
		notificationPool.execute(new Runnable() {

			@Override
			public void run() {
				SubscriptionRequest subscription = remoteNotifyCallbackId2InternalSub.get(id);
				if (subscription == null) {
					return;
				}
				sendNotification((List<Map<String, Object>>) notification.get(NGSIConstants.NGSI_LD_DATA), subscription,
						AppConstants.UPDATE_REQUEST, new BatchInfo(-1, -1));
			}
		});
	}

	public void handleRegistryNotification(InternalNotification notification) {
		if (notification.getTriggerReason() == AppConstants.DELETE_REQUEST) {
			unsubscribeRemote(notification.getSubscriptionId());
		} else {
			subscribeToRemote(this.tenant2subscriptionId2Subscription.get(notification.getTenantId(),
					notification.getSubscriptionId()), notification);
		}

	}

	@SuppressWarnings("unchecked")
	private String getRemoteEndPoint(Map<String, Object> registration) {
		return ((List<Map<String, String>>) registration.get(NGSIConstants.NGSI_LD_ENDPOINT)).get(0)
				.get(NGSIConstants.JSON_LD_VALUE);
	}

	private void unsubscribeRemote(String subscriptionId) {
		String endpoint = internalSubId2ExternalEndpoint.remove(subscriptionId);
		if (endpoint != null) {
			remoteNotifyCallbackId2InternalSub.remove(internalSubId2RemoteNotifyCallbackId2.remove(subscriptionId));
			restTemplate.delete(endpoint);
		}
	}

	@Override
	protected String generateUniqueSubId(Subscription subscription) {
		return "urn:ngsi-ld:Subscription:" + subscription.hashCode();
	}

	@Override
	protected boolean sendDeleteNotification() {
		return false;
	}

	@Override
	protected boolean evaluateQ() {
		return true;
	}

	@Override
	protected boolean shouldFire(Map<String, Object> entry, SubscriptionRequest subscription) {

		if (subscription.getSubscription().getAttributeNames() == null
				|| subscription.getSubscription().getAttributeNames().isEmpty()) {
			return true;
		}
		Set<String> keys = entry.keySet();
		for (String attribName : subscription.getSubscription().getAttributeNames()) {
			if (keys.contains(attribName)) {
				return true;
			}
		}
		return false;
	}

	@Override
	protected boolean evaluateCSF() {
		return false;
	}

	@Override
	protected void setSyncTopic() {
		this.subSyncTopic = SUB_SYNC_TOPIC;
	}

	@Override
	protected void setSyncId() {
		this.syncIdentifier = SubscriptionSyncService.SYNC_ID;
	}

}
