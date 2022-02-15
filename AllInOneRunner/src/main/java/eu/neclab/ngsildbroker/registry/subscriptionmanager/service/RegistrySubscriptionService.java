package eu.neclab.ngsildbroker.registry.subscriptionmanager.service;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.InternalNotification;
import eu.neclab.ngsildbroker.commons.datatypes.Notification;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.NotificationHandler;
import eu.neclab.ngsildbroker.commons.subscriptionbase.BaseSubscriptionService;
import eu.neclab.ngsildbroker.commons.subscriptionbase.SubscriptionInfoDAOInterface;
import eu.neclab.ngsildbroker.commons.tools.EntityTools;
import eu.neclab.ngsildbroker.registry.subscriptionmanager.repository.RegistrySubscriptionInfoDAO;

@ApplicationScoped
public class RegistrySubscriptionService extends BaseSubscriptionService {

	@Inject
	RegistrySubscriptionInfoDAO subService;

	@Inject
	@Channel(AppConstants.INTERNAL_NOTIFICATION_CHANNEL)
	Emitter<InternalNotification> kafkaSender;

	private NotificationHandler internalHandler;

	private HashMap<String, SubscriptionRequest> id2InternalSubscriptions = new HashMap<String, SubscriptionRequest>();

	@PostConstruct
	void notificationHandlerSetup() {
		this.internalHandler = new InternalNotificationHandler(kafkaSender);
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
				request.getContext(), request.getHeaders());
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

	void subscribeInternal(SubscriptionRequest request) {
		makeSubscriptionInternal(request);
		try {
			subscribe(request);
		} catch (ResponseException e) {
			logger.debug("Failed to subscribe internally", e);
		}
	}

	void unsubscribeInternal(String subId) {
		SubscriptionRequest request = id2InternalSubscriptions.remove(subId);
		try {
			if (request != null) {
				unsubscribe(subId, request.getHeaders());
			}
		} catch (ResponseException e) {
			logger.debug("Failed to subscribe internally", e);
		}
	}

	@PreDestroy
	@Override
	protected void deconstructor() {
		for (Entry<String, SubscriptionRequest> entry : id2InternalSubscriptions.entrySet()) {
			try {
				unsubscribe(entry.getKey(), entry.getValue().getHeaders());
			} catch (ResponseException e) {
				logger.debug("Failed to subscribe internally", e);
			}
		}
		super.deconstructor();
	}

	public void updateInternal(SubscriptionRequest request) {
		makeSubscriptionInternal(request);
		try {
			updateSubscription(request);
		} catch (ResponseException e) {
			logger.debug("Failed to subscribe internally", e);
		}

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

}
