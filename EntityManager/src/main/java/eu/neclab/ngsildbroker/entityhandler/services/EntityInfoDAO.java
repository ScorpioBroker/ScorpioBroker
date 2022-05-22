package eu.neclab.ngsildbroker.entityhandler.services;

import eu.neclab.ngsildbroker.commons.interfaces.StorageFunctionsInterface;
import eu.neclab.ngsildbroker.commons.storage.EntityStorageFunctions;
import eu.neclab.ngsildbroker.commons.storage.StorageDAO;

public class EntityInfoDAO extends StorageDAO {

	@Override
	protected StorageFunctionsInterface getStorageFunctions() {
		return new EntityStorageFunctions();
	}
}
