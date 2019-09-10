package eu.neclab.ngsildbroker.queryhandler.repository;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Repository;
import eu.neclab.ngsildbroker.commons.storage.StorageReaderDAO;

@Repository
@ConditionalOnProperty(value="directDbConnection", havingValue = "true", matchIfMissing = false)
public class QueryDAO extends StorageReaderDAO {
	
}
