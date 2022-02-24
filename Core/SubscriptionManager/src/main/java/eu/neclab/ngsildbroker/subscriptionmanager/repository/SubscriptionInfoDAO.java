package eu.neclab.ngsildbroker.subscriptionmanager.repository;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import eu.neclab.ngsildbroker.commons.constants.DBConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.interfaces.StorageFunctionsInterface;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;
import eu.neclab.ngsildbroker.commons.storage.EntityStorageFunctions;
import eu.neclab.ngsildbroker.commons.subscriptionbase.BaseSubscriptionInfoDAO;

@Repository("subdao")
public class SubscriptionInfoDAO extends BaseSubscriptionInfoDAO {

	@Override
	protected String getSQLForTypes() {
		return "SELECT id, type FROM entity";
	}

	@Override
	protected StorageFunctionsInterface getStorageFunctions() {
		return new EntityStorageFunctions();
	}

	@Override
	public void storeSubscription(SubscriptionRequest sub) {
		String tenant = sub.getTenant();
		JdbcTemplate template = getJDBCTemplate(tenant);
		template.update(
				"INSERT INTO subscriptions (subscription_id, subscription_request) VALUES (?, ?) ON CONFLICT(subscription_id) DO UPDATE SET subscription_request = EXCLUDED.subscription_request",
				sub.getId(), DataSerializer.toJson(sub));
	}

	@Override
	public void deleteSubscription(SubscriptionRequest sub) {
		String tenant = sub.getTenant();
		JdbcTemplate template = getJDBCTemplate(tenant);
		template.update("DELETE FROM subscriptions WHERE subscription_id=?", sub.getId());

	}
}
