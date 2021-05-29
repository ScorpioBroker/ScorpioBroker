package eu.neclab.ngsildbroker.registryhandler.repository;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.storage.StorageReaderDAO;
import eu.neclab.ngsildbroker.commons.tenant.TenantAwareDataSource;

@Repository
public class CSourceInfoDAO extends StorageReaderDAO {

	@Autowired
	TenantAwareDataSource tenantAwareDataSource;

	@Autowired
	private DataSource readerDataSource;

	public Set<String> getAllIds() {
		synchronized (readerJdbcTemplate) {
			readerJdbcTemplate = new JdbcTemplate(readerDataSource);
		}
		List<String> tempList = readerJdbcTemplate.queryForList("SELECT id FROM csource", String.class);
		return new HashSet<String>(tempList);
	}

	public Set<String> getAllTenantIds() throws ResponseException {
		DataSource finaldatasource = tenantAwareDataSource.determineTargetDataSource();
		if (finaldatasource == null)
			throw new ResponseException(ErrorType.TenantNotFound);
		synchronized (readerJdbcTemplate) {
			readerJdbcTemplate = new JdbcTemplate(finaldatasource);
		}
		List<String> tempTenantList = readerJdbcTemplate.queryForList("SELECT id FROM csource", String.class);
		return new HashSet<String>(tempTenantList);
	}

	public String getEntity(String entityId) {
		synchronized (readerJdbcTemplate) {
			readerJdbcTemplate = new JdbcTemplate(readerDataSource);
		}
		List<String> tempList = readerJdbcTemplate.queryForList("SELECT data FROM csource WHERE id='" + entityId + "'",
				String.class);
		return tempList.get(0);
	}

	public String getTenantEntity(String entityId) {
		DataSource finaldatasource = tenantAwareDataSource.determineTargetDataSource();
		synchronized (readerJdbcTemplate) {
			readerJdbcTemplate = new JdbcTemplate(finaldatasource);
		}
		List<String> tempTenantList = readerJdbcTemplate
				.queryForList("SELECT data FROM csource WHERE id='" + entityId + "'", String.class);
		return tempTenantList.get(0);
	}

}
