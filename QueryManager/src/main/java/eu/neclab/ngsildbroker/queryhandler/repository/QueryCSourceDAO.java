package eu.neclab.ngsildbroker.queryhandler.repository;

import javax.enterprise.context.ApplicationScoped;

import eu.neclab.ngsildbroker.commons.interfaces.StorageFunctionsInterface;
import eu.neclab.ngsildbroker.commons.storage.RegistryStorageFunctions;
import eu.neclab.ngsildbroker.commons.storage.StorageDAO;


@ApplicationScoped
public class QueryCSourceDAO extends StorageDAO {

	@Override
	protected StorageFunctionsInterface getStorageFunctions() {
		return new RegistryStorageFunctions();
	}



}
