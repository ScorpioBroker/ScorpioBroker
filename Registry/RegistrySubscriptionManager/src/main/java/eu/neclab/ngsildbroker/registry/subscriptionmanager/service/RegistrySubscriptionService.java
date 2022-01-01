package eu.neclab.ngsildbroker.registry.subscriptionmanager.service;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.Notification;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.subscriptionbase.BaseSubscriptionService;
import eu.neclab.ngsildbroker.commons.subscriptionbase.SubscriptionInfoDAOInterface;
import eu.neclab.ngsildbroker.commons.tools.EntityTools;

@Service
public class RegistrySubscriptionService extends BaseSubscriptionService {

	@Autowired
	@Qualifier("regsubdao")
	SubscriptionInfoDAOInterface subService;

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
				request.getContext());
	}

	@Override
	protected boolean sendInitialNotification() {
		return true;
	}

}
