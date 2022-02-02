package eu.neclab.ngsildbroker.queryhandler.repository;

import org.springframework.stereotype.Repository;

import eu.neclab.ngsildbroker.commons.interfaces.StorageFunctionsInterface;
import eu.neclab.ngsildbroker.commons.storage.EntityStorageFunctions;
import eu.neclab.ngsildbroker.commons.storage.StorageDAO;


@Repository
public class QueryDAO extends StorageDAO {

	@Override
	protected StorageFunctionsInterface getStorageFunctions() {
		// TODO Auto-generated method stub
		return new EntityStorageFunctions();
	}
	
}
