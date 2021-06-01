package eu.neclab.ngsildbroker.subscriptionmanager.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Repository;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.storage.StorageReaderDAO;

@Repository
public class SubscriptionInfoDAO extends StorageReaderDAO {
	private final static Logger logger = LogManager.getLogger(SubscriptionInfoDAO.class);

	public Set<String> getAllIds(String tenantId) {
		try {
			setTenant(tenantId);
		} catch (ResponseException e) {
			// Left Empty intentionally
		}
		synchronized (readerJdbcTemplate) {
			List<String> tempList = readerJdbcTemplate.queryForList("SELECT id FROM entity", String.class);
			return new HashSet<String>(tempList);
		}

	}

	public Table<String, String, String> getIds2Type() {
		Table<String, String, String> result = HashBasedTable.create();
		List<String> tenants = getTenants();
		for (String tenantId : tenants) {
			try {
				setTenant(tenantId);
			} catch (ResponseException e) {
				// Left Empty intentionally
			}
			synchronized (readerJdbcTemplate) {
				List<Map<String, Object>> temp = readerJdbcTemplate.queryForList("SELECT id, type FROM entity");
				for (Map<String, Object> entry : temp) {
					result.put(tenantId, entry.get("id").toString(), entry.get("type").toString());
				}
			}
		}
		return result;
	}

	private List<String> getTenants() {
		// TODO Auto-generated method stub
		return new ArrayList<String>();
	}

	public String getEntity(String entityId, String tenantId) {
		tenantId = getTenant(tenantId);
		QueryParams qp = new QueryParams();
		qp.setId(entityId);
		qp.setTenant(tenantId);
		try {
			return super.query(qp).get(0);
		} catch (ResponseException e) {
			logger.info("Failed to get info for entity with id " + entityId);
			logger.debug(e);
			return null;
		}
	}

	private String getTenant(String tenantId) {
		if(AppConstants.INTERNAL_NULL_KEY.equals(tenantId)) {
			return null;	
		}
		return tenantId;
	}
}
