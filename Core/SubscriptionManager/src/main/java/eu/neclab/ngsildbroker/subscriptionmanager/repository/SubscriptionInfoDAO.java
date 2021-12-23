package eu.neclab.ngsildbroker.subscriptionmanager.repository;

import org.springframework.stereotype.Repository;

import eu.neclab.ngsildbroker.commons.subscriptionbase.BaseSubscriptionInfoDAO;

@Repository("subdao")
public class SubscriptionInfoDAO extends BaseSubscriptionInfoDAO {

	@Override
	protected String getSQLForTypes() {
		return "SELECT id, type FROM entity";
	}
}
