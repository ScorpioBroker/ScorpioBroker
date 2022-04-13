package eu.neclab.ngsildbroker.queryhandler.repository;

import org.springframework.stereotype.Repository;

import eu.neclab.ngsildbroker.commons.interfaces.StorageFunctionsInterface;
import eu.neclab.ngsildbroker.commons.storage.RegistryStorageFunctions;
import eu.neclab.ngsildbroker.commons.storage.StorageDAO;

@Repository("qmcsourcedao")
public class CSourceDAO extends StorageDAO {

	@Override
	protected StorageFunctionsInterface getStorageFunctions() {
		return new RegistryStorageFunctions();
	}



}
