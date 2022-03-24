package eu.neclab.ngsildbroker.commons.datatypes;

import java.util.Set;

import com.google.common.collect.Sets;

import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.interfaces.AnnouncementMessage;

public class TakingAnnouncement implements AnnouncementMessage {

	Set<String> subscriptionIds;

	public TakingAnnouncement(Set<SubscriptionRequest> subscriptionsTaken) {
		subscriptionIds = Sets.newHashSet();
		subscriptionsTaken.forEach(t -> {
			subscriptionIds.add(t.getId());
		});
	}

	public TakingAnnouncement() {

	}

	public Set<String> getSubscriptionIds() {
		return subscriptionIds;
	}

	public void setSubscriptionIds(Set<String> subscriptionIds) {
		this.subscriptionIds = subscriptionIds;
	}

	@Override
	public String getId() {
		// TODO Auto-generated method stub
		return null;
	}

}
