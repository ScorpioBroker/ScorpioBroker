package eu.neclab.ngsildbroker.entityhandler.services;

import java.util.HashSet;
import java.util.Set;

import org.springframework.stereotype.Repository;

import eu.neclab.ngsildbroker.commons.storage.StorageReaderDAO;

@Repository
public class EntityInfoDAO extends StorageReaderDAO {
	public Set<String> getAllIds() {
		return new HashSet<String>(readerJdbcTemplate.queryForList("SELECT id FROM entity", String.class));
	}
}
