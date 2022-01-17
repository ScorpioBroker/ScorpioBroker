package eu.neclab.ngsildbroker.subscriptionmanager.service;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import javax.annotation.PreDestroy;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.EndPoint;
import eu.neclab.ngsildbroker.commons.datatypes.InternalNotification;
import eu.neclab.ngsildbroker.commons.datatypes.Notification;
import eu.neclab.ngsildbroker.commons.datatypes.NotificationParam;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;
import eu.neclab.ngsildbroker.commons.subscriptionbase.BaseSubscriptionService;
import eu.neclab.ngsildbroker.commons.subscriptionbase.SubscriptionInfoDAOInterface;
import eu.neclab.ngsildbroker.commons.tools.EntityTools;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import reactor.core.publisher.Mono;

@Service
public class SubscriptionService extends BaseSubscriptionService {
	@Autowired
	@Qualifier("subdao")
	SubscriptionInfoDAOInterface subService;

	@Autowired
	KafkaTemplate<String, Object> kafkaTemplate;

	private HashMap<String, SubscriptionRequest> remoteNotifyCallbackId2InternalSub = new HashMap<String, SubscriptionRequest>();
	private HashMap<String, String> internalSubId2RemoteNotifyCallbackId2 = new HashMap<String, String>();
	private HashMap<String, String> internalSubId2ExternalEndpoint = new HashMap<String, String>();

	@Value("${scorpio.topics.internalregsub}")
	private String INTERNAL_SUBSCRIPTION_TOPIC;

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
				request.getHeaders());
	}

	@Override
	protected boolean sendInitialNotification() {
		return false;
	}

	@PreDestroy
	private void unsubscribeToAllRemote() {
		for (String entry : internalSubId2ExternalEndpoint.values()) {
			webClient.delete().uri(entry).exchangeToMono(response -> {
				return Mono.just(Void.class);
			}).subscribe();
		}
	}

	@Override
	public void unsubscribe(String id, ArrayListMultimap<String, String> headers) throws ResponseException {
		unsubscribeRemote(id);
		SubscriptionRequest request = tenant2subscriptionId2Subscription.get(HttpUtils.getInternalTenant(headers), id);
		if (request != null) {
			// let super unsubscribe take care of further error handling
			request.setRequestType(AppConstants.DELETE_REQUEST);
			kafkaTemplate.send(INTERNAL_SUBSCRIPTION_TOPIC, id, request);
		}
		super.unsubscribe(id, headers);

	}

	@Override
	public String subscribe(SubscriptionRequest subscriptionRequest) throws ResponseException {
		String result = super.subscribe(subscriptionRequest);
		kafkaTemplate.send(INTERNAL_SUBSCRIPTION_TOPIC, subscriptionRequest.getSubscription().getId(),
				subscriptionRequest);
		return result;
	}

	@Override
	public void updateSubscription(SubscriptionRequest subscriptionRequest) throws ResponseException {
		super.updateSubscription(subscriptionRequest);
		kafkaTemplate.send(INTERNAL_SUBSCRIPTION_TOPIC, subscriptionRequest.getSubscription().getId(),
				subscriptionRequest);
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

				Subscription remoteSub = new Subscription();
				Subscription subscription = subscriptionRequest.getSubscription();
				remoteSub.setDescription(subscription.getDescription());
				remoteSub.setEntities(subscription.getEntities());
				remoteSub.setExpiresAt(subscription.getExpiresAt());
				remoteSub.setLdGeoQuery(subscription.getLdGeoQuery());
				remoteSub.setLdQuery(subscription.getLdQuery());
				remoteSub.setLdTempQuery(subscription.getLdTempQuery());
				remoteSub.setSubscriptionName(subscription.getSubscriptionName());
				remoteSub.setStatus(subscription.getStatus());
				remoteSub.setThrottling(subscription.getThrottling());
				remoteSub.setTimeInterval(subscription.getTimeInterval());
				remoteSub.setType(subscription.getType());
				NotificationParam remoteNotification = new NotificationParam();
				remoteNotification.setAttributeNames(subscription.getNotification().getAttributeNames());
				remoteNotification.setFormat(subscription.getNotification().getFormat());
				EndPoint endPoint = new EndPoint();
				endPoint.setAccept(AppConstants.NGB_APPLICATION_JSONLD);
				endPoint.setUri(prepareNotificationServlet(subscriptionRequest));
				remoteNotification.setEndPoint(endPoint);
				remoteSub.setAttributeNames(subscription.getAttributeNames());
				String body = DataSerializer.toJson(remoteSub);
				for (Map<String, Object> entry : notification.getData()) {
					HttpHeaders additionalHeaders = HttpUtils.getAdditionalHeaders(entry,
							subscriptionRequest.getContext(),
							subscription.getNotification().getEndPoint().getAccept());
					String remoteEndpoint = getRemoteEndPoint(entry);
					StringBuilder temp = new StringBuilder(remoteEndpoint);
					if (remoteEndpoint.endsWith("/")) {
						temp.deleteCharAt(remoteEndpoint.length() - 1);
					}
					temp.append(AppConstants.SUBSCRIPTIONS_URL);
					webClient.post().uri(temp.toString()).headers(httpHeadersOnWebClientBeingBuilt -> {
						httpHeadersOnWebClientBeingBuilt.addAll(additionalHeaders);
					}).bodyValue(body).exchangeToMono(response -> {
						if (response.statusCode().is2xxSuccessful()) {
							internalSubId2ExternalEndpoint.put(subscription.getId(),
									response.headers().header(HttpHeaders.LOCATION).get(0));
							return Mono.just(Void.class);
						} else {
							return response.createException().flatMap(Mono::error);
						}
					}).subscribe();
				}
			}
		}.start();

	}

	private URI prepareNotificationServlet(SubscriptionRequest subToCheck) {

		String uuid = Long.toString(UUID.randomUUID().getLeastSignificantBits());
		remoteNotifyCallbackId2InternalSub.put(uuid, subToCheck);
		internalSubId2RemoteNotifyCallbackId2.put(subToCheck.getId(), uuid);
		StringBuilder url = new StringBuilder(MicroServiceUtils.getGatewayURL().toString()).append("/remotenotify/")
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

		new Thread() {
			@Override
			public void run() {
				SubscriptionRequest subscription = remoteNotifyCallbackId2InternalSub.get(id);
				if (subscription == null) {
					return;
				}
				sendNotification((List<Map<String, Object>>) notification.get(NGSIConstants.NGSI_LD_DATA), subscription,
						AppConstants.UPDATE_REQUEST);
			}
		}.start();

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
			webClient.delete().uri(endpoint).exchangeToMono(response -> {
				return Mono.just(Void.class);
			}).retry(5).subscribe();
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

}
