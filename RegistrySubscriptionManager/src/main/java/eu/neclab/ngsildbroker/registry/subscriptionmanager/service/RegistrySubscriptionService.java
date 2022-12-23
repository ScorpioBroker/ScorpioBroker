package eu.neclab.ngsildbroker.registry.subscriptionmanager.service;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import org.eclipse.microprofile.reactive.messaging.Channel;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.InternalNotification;
import eu.neclab.ngsildbroker.commons.datatypes.Notification;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.datatypes.SyncMessage;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.interfaces.NotificationHandler;
import eu.neclab.ngsildbroker.commons.subscriptionbase.BaseSubscriptionService;
import eu.neclab.ngsildbroker.commons.subscriptionbase.SubscriptionInfoDAOInterface;
import eu.neclab.ngsildbroker.commons.tools.EntityTools;
import eu.neclab.ngsildbroker.registry.subscriptionmanager.messaging.RegistrySubscriptionSyncService;
import eu.neclab.ngsildbroker.registry.subscriptionmanager.repository.RegistrySubscriptionInfoDAO;
import io.quarkus.arc.profile.IfBuildProfile;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.annotations.Broadcast;

@Singleton
@IfBuildProfile("in-memory")
public class RegistrySubscriptionService extends BaseSubscriptionService {

	@Inject
	RegistrySubscriptionInfoDAO subService;

	@Inject
	@Channel(AppConstants.INTERNAL_NOTIFICATION_CHANNEL)
	@Broadcast
	MutinyEmitter<InternalNotification> internalNotificationSender;

	private NotificationHandler internalHandler;

	private HashMap<String, SubscriptionRequest> id2InternalSubscriptions = new HashMap<String, SubscriptionRequest>();

	@PostConstruct
	void notificationHandlerSetup() {
		this.internalHandler = new InternalNotificationHandler(internalNotificationSender);
	}

	@Override
	protected SubscriptionInfoDAOInterface getSubscriptionInfoDao() {
		return subService;
	}

	@Override
	protected Set<String> getTypesFromEntry(BaseRequest createRequest) {
		return EntityTools.getRegisteredTypes(createRequest.getFinalPayload());
	}

	@Override
	protected Notification getNotification(SubscriptionRequest request, List<Map<String, Object>> dataList,
			int triggerReason) {
		return new Notification(EntityTools.getRandomID("notification:"), NGSIConstants.CSOURCE_NOTIFICATION,
				System.currentTimeMillis(), request.getSubscription().getId(), dataList, triggerReason,
				request.getContext(), request.getTenant());
	}

	@Override
	protected boolean sendInitialNotification() {
		return true;
	}

	@Override
	protected NotificationHandler getNotificationHandler(String endpointProtocol) {
		if (endpointProtocol.equals("internal")) {
			return internalHandler;
		}
		return super.getNotificationHandler(endpointProtocol);
	}

	public Uni<Void> subscribeInternal(SubscriptionRequest request) {
		makeSubscriptionInternal(request);
		return subscribe(request).onItem().transformToUni(t -> Uni.createFrom().voidItem()).onFailure()
				.recoverWithUni(e -> {
					logger.debug("Failed to subscribe internally", e);
					return Uni.createFrom().voidItem();
				});
	}

	public Uni<Void> unsubscribeInternal(String subId) {
		SubscriptionRequest request = id2InternalSubscriptions.remove(subId);
		if (request != null) {
			return unsubscribe(subId, request.getTenant());
		}
		return Uni.createFrom().voidItem();
	}

	@PreDestroy
	public void deconstructor() {
		destroy();
		for (Entry<String, SubscriptionRequest> entry : id2InternalSubscriptions.entrySet()) {
			unsubscribe(entry.getKey(), entry.getValue().getTenant()).await().atMost(Duration.ofMillis(500));
		}

	}

	public Uni<Void> updateInternal(SubscriptionRequest request) {
		makeSubscriptionInternal(request);
		return updateSubscription(request).onItem().transformToUni(t -> Uni.createFrom().voidItem()).onFailure()
				.recoverWithUni(e -> {
					logger.debug("Failed to subscribe internally", e);
					return Uni.createFrom().voidItem();
				});
	}

	private void makeSubscriptionInternal(SubscriptionRequest request) {
		Subscription sub = request.getSubscription();
		try {
			if (sub.getNotification() != null) {
				sub.getNotification().getEndPoint().setUri(new URI("internal://kafka"));
			}
		} catch (URISyntaxException e) {
			logger.debug("Failed to set internal sub endpoint", e);
		}
		sub.setTimeInterval(0);
		id2InternalSubscriptions.put(sub.getId(), request);
	}

	@Override
	protected String generateUniqueSubId(Subscription subscription) {
		return "urn:ngsi-ld:Registry:Subscription:" + subscription.hashCode();
	}

	@Override
	protected boolean sendDeleteNotification() {
		return true;
	}

	@Override
	protected boolean evaluateQ() {
		return false;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected boolean shouldFire(Map<String, Object> entry, SubscriptionRequest subscription) {
		List<String> attribs = subscription.getSubscription().getAttributeNames();
		if (attribs == null || attribs.isEmpty()) {
			return true;
		}
		List<Map<String, Object>> information = (List<Map<String, Object>>) entry
				.get(NGSIConstants.NGSI_LD_INFORMATION);
		for (Map<String, Object> informationEntry : information) {
			Object propertyNames = informationEntry.get(NGSIConstants.NGSI_LD_PROPERTIES);
			Object relationshipNames = informationEntry.get(NGSIConstants.NGSI_LD_RELATIONSHIPS);
			if (relationshipNames == null && relationshipNames == null) {
				return true;
			}
			if (relationshipNames != null) {
				List<Map<String, String>> list = (List<Map<String, String>>) relationshipNames;
				for (Map<String, String> relationshipEntry : list) {
					if (attribs.contains(relationshipEntry.get(NGSIConstants.JSON_LD_ID))) {
						return true;
					}
				}
			}
			if (propertyNames != null) {
				List<Map<String, String>> list = (List<Map<String, String>>) propertyNames;
				for (Map<String, String> propertyEntry : list) {
					if (attribs.contains(propertyEntry.get(NGSIConstants.JSON_LD_ID))) {
						return true;
					}
				}
			}
		}
		return false;
	}

	@Override
	protected boolean evaluateCSF() {
		return true;
	}

	@Override
	protected void setSyncId() {
		this.syncIdentifier = RegistrySubscriptionSyncService.SYNC_ID;
	}

	@Override
	protected MutinyEmitter<SyncMessage> getSyncChannelSender() {
		return null;
	}

}
