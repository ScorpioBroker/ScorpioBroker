package eu.neclab.ngsildbroker.subscriptionmanager.service;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.ArrayListMultimap;

import org.eclipse.microprofile.reactive.messaging.Channel;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.BatchInfo;
import eu.neclab.ngsildbroker.commons.datatypes.InternalNotification;
import eu.neclab.ngsildbroker.commons.datatypes.Notification;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.datatypes.SyncMessage;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.subscriptionbase.BaseSubscriptionService;
import eu.neclab.ngsildbroker.commons.subscriptionbase.SubscriptionInfoDAOInterface;
import eu.neclab.ngsildbroker.commons.tools.EntityTools;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import eu.neclab.ngsildbroker.subscriptionmanager.messaging.SubscriptionSyncService;
import eu.neclab.ngsildbroker.subscriptionmanager.repository.SubscriptionInfoDAO;
import io.quarkus.arc.profile.IfBuildProfile;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.annotations.Broadcast;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpHeaders;
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.ext.web.client.HttpRequest;

@Singleton
@IfBuildProfile("in-memory")
public class SubscriptionService extends BaseSubscriptionService {
	@Inject
	SubscriptionInfoDAO subService;

	private JsonLdOptions opts = new JsonLdOptions(JsonLdOptions.JSON_LD_1_1);
	private HashMap<String, SubscriptionRequest> remoteNotifyCallbackId2InternalSub = new HashMap<String, SubscriptionRequest>();
	private HashMap<String, String> internalSubId2RemoteNotifyCallbackId2 = new HashMap<String, String>();
	private HashMap<String, String> internalSubId2ExternalEndpoint = new HashMap<String, String>();

	@Inject
	MicroServiceUtils microServiceUtils;

	@Inject
	@Channel(AppConstants.INTERNAL_SUBS_CHANNEL)
	@Broadcast
	MutinyEmitter<SubscriptionRequest> internalSubEmitter;

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
				request.getTenant());
	}

	@Override
	protected boolean sendInitialNotification() {
		return false;
	}

	@PreDestroy
	public void unsubscribeToAllRemote() {
		destroy();
		for (String entry : internalSubId2ExternalEndpoint.values()) {
			webClient.deleteAbs(entry).sendAndForget();
		}
	}

	@Override
	public Uni<Void> unsubscribe(String id, String tenant) {
		unsubscribeRemote(id);
		SubscriptionRequest request = tenant2subscriptionId2Subscription.get(tenant, id);
		Uni<Void> kafkaSent = Uni.createFrom().nullItem();
		if (request != null) {
			// let super unsubscribe take care of further error handling
			request.setRequestType(AppConstants.DELETE_REQUEST);
			kafkaSent = internalSubEmitter.send(request);
		}

		return Uni.combine().all().unis(super.unsubscribe(id, tenant), kafkaSent).combinedWith((t, u) -> null);

	}

	@Override
	public Uni<String> subscribe(SubscriptionRequest subscriptionRequest) {
		Uni<String> result = super.subscribe(subscriptionRequest);
		Uni<Void> kafkaSent = internalSubEmitter.send(subscriptionRequest);

		return Uni.combine().all().unis(result, kafkaSent).combinedWith((t, u) -> t);
	}

	@Override
	public Uni<Void> updateSubscription(SubscriptionRequest subscriptionRequest) {
		Uni<Void> result = super.updateSubscription(subscriptionRequest);
		Uni<Void> kafkaSent = internalSubEmitter.send(subscriptionRequest);
		return Uni.combine().all().unis(result, kafkaSent).combinedWith((t, u) -> t);
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
					MultiMap additionalHeaders = HttpUtils.getAdditionalHeaders(entry, subscriptionRequest.getContext(),
							Arrays.asList(remoteSub.getNotification().getEndPoint().getAccept()));
					additionalHeaders.add(HttpHeaders.CONTENT_TYPE, "application/ld+json");
					String remoteEndpoint = getRemoteEndPoint(entry);
					StringBuilder temp = new StringBuilder(remoteEndpoint);
					if (remoteEndpoint.endsWith("/")) {
						temp.deleteCharAt(remoteEndpoint.length() - 1);
					}
					temp.append(AppConstants.SUBSCRIPTIONS_URL);

					HttpRequest<Buffer> req = webClient.postAbs(temp.toString());
					for (Entry<String, String> headersEntry : additionalHeaders.entries()) {
						req = req.putHeader(headersEntry.getKey(), headersEntry.getValue());
					}
					req.sendBuffer(Buffer.buffer(body)).onFailure().retry().withBackOff(Duration.ofSeconds(5)).atMost(5)
							.onItem().transformToUni(response -> {
								if (response.statusCode() >= 200 && response.statusCode() < 300) {
									String locationHeader = response.headers().get(HttpHeaders.LOCATION);
									// check if it's a relative path
									if (locationHeader.charAt(0) == '/') {
										locationHeader = remoteEndpoint + locationHeader;
									}
									internalSubId2ExternalEndpoint.put(subscriptionRequest.getSubscription().getId(),
											locationHeader);
								}
								return Uni.createFrom().voidItem();
							}).onFailure().recoverWithUni(t -> {
								logger.error("Failed to subscribe to remote host " + temp.toString(), t);
								return Uni.createFrom().voidItem();
							}).subscribe();

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

		SubscriptionRequest subscription = remoteNotifyCallbackId2InternalSub.get(id);
		if (subscription == null) {
			return;
		}
		sendNotification((List<Map<String, Object>>) notification.get(NGSIConstants.NGSI_LD_DATA), subscription,
				AppConstants.UPDATE_REQUEST, new BatchInfo(-1, -1));
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
			webClient.deleteAbs(endpoint).sendAndForget();
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
	protected void setSyncId() {
		this.syncIdentifier = SubscriptionSyncService.SYNC_ID;
	}

	@Override
	protected MutinyEmitter<SyncMessage> getSyncChannelSender() {
		return null;
	}

}
