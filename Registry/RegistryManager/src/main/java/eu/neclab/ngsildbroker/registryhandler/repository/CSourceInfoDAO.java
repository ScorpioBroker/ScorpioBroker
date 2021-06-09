package eu.neclab.ngsildbroker.registryhandler.repository;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.storage.StorageReaderDAO;

@Repository
public class CSourceInfoDAO extends StorageReaderDAO {

	public ArrayListMultimap<String, String> getAllIds() throws ResponseException {
		ArrayListMultimap<String, String> result = ArrayListMultimap.create();
		result.putAll(AppConstants.INTERNAL_NULL_KEY,
				getJDBCTemplate(null).queryForList("SELECT DISTINCT id FROM csource", String.class));
		List<String> tenants = getTenants();
		for (String tenant : tenants) {
			result.putAll(tenant,
					getJDBCTemplate(tenant).queryForList("SELECT DISTINCT id FROM csource", String.class));
		}

		return result;
	}
	

	public String getEntity(String tenantId, String entityId) throws ResponseException {

		List<String> tempList = getJDBCTemplate(getTenant(tenantId))
				.queryForList("SELECT data FROM csource WHERE id='" + entityId + "'", String.class);
		return tempList.get(0);
	}

	

	public List<String> getAllRegistrations(String tenant) throws ResponseException {
		tenant = getTenant(tenant);
		return getJDBCTemplate(tenant).queryForList("SELECT data FROM csource", String.class);
	}

	

}
