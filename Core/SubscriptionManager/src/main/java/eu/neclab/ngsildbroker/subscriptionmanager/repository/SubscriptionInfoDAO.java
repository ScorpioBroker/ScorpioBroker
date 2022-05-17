package eu.neclab.ngsildbroker.subscriptionmanager.repository;

import org.springframework.stereotype.Repository;

import eu.neclab.ngsildbroker.commons.interfaces.StorageFunctionsInterface;
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
	protected String getDBName() {
		return "subscriptions";
	}

}
