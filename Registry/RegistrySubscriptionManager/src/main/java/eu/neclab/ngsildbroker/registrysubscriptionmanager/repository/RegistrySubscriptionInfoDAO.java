package eu.neclab.ngsildbroker.registrysubscriptionmanager.repository;

import org.springframework.stereotype.Repository;

import eu.neclab.ngsildbroker.commons.interfaces.StorageFunctionsInterface;
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
	protected String getDBName() {
		return "registry_subscriptions";
	}

}
