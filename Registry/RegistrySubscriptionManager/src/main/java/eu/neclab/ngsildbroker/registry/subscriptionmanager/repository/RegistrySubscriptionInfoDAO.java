package eu.neclab.ngsildbroker.registry.subscriptionmanager.repository;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.interfaces.StorageFunctionsInterface;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;
import eu.neclab.ngsildbroker.commons.storage.RegistryStorageFunctions;
import eu.neclab.ngsildbroker.commons.subscriptionbase.BaseSubscriptionInfoDAO;

@Repository("regsubdao")
public class RegistrySubscriptionInfoDAO extends BaseSubscriptionInfoDAO {
	@Override
	protected String getSQLForTypes() {
		return "SELECT csource_id AS id, entity_type AS type FROM csourceinformation";
	}

	@Override
	protected StorageFunctionsInterface getStorageFunctions() {
		return new RegistryStorageFunctions();
	}

	@Override
	public void storeSubscription(SubscriptionRequest sub) {
		String tenant = sub.getTenant();
		JdbcTemplate template = getJDBCTemplate(tenant);
		template.update(
				"INSERT INTO registry_subscriptions (subscription_id, subscription_request) VALUES (?, ?) ON CONFLICT(subscription_id) DO UPDATE SET subscription_request = EXCLUDED.subscription_request",
				sub.getId(), DataSerializer.toJson(sub));
	}

	@Override
	public void deleteSubscription(SubscriptionRequest sub) {
		String tenant = sub.getTenant();
		JdbcTemplate template = getJDBCTemplate(tenant);
		template.update("DELETE FROM subscriptions WHERE subscription_id=?", sub.getId());
	}
}
