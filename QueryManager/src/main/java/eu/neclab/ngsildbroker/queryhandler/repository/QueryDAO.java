package eu.neclab.ngsildbroker.queryhandler.repository;

import javax.inject.Singleton;

import eu.neclab.ngsildbroker.commons.interfaces.StorageFunctionsInterface;
import eu.neclab.ngsildbroker.commons.storage.EntityStorageFunctions;
import eu.neclab.ngsildbroker.commons.storage.StorageDAO;

@Singleton
public class QueryDAO extends StorageDAO {

	@Override
	protected StorageFunctionsInterface getStorageFunctions() {
		// TODO Auto-generated method stub
		return new EntityStorageFunctions();
	}

}
