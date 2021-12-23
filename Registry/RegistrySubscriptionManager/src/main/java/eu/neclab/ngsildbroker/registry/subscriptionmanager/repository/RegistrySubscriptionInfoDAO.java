package eu.neclab.ngsildbroker.registry.subscriptionmanager.repository;

import org.springframework.stereotype.Repository;

import eu.neclab.ngsildbroker.commons.subscriptionbase.BaseSubscriptionInfoDAO;

@Repository("regsubdao")
public class RegistrySubscriptionInfoDAO extends BaseSubscriptionInfoDAO {
	@Override
	protected String getSQLForTypes() {
		return "SELECT csource_id AS id, entity_type AS type FROM csourceinformation";
	}
}
