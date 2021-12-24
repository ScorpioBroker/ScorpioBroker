package eu.neclab.ngsildbroker.commons.subscriptionbase;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;
import eu.neclab.ngsildbroker.commons.storage.StorageReaderDAO;


public abstract class BaseSubscriptionInfoDAO extends StorageReaderDAO implements SubscriptionInfoDAOInterface {
	private final static Logger logger = LogManager.getLogger(BaseSubscriptionInfoDAO.class);

	

	public Table<String, String, Set<String>> getIds2Type() throws ResponseException {
		Table<String, String, Set<String>> result = HashBasedTable.create();
		String sql = getSQLForTypes();

		for (Map<String, Object> entry : getJDBCTemplate(null).queryForList(sql)) {
			addToResult(result, AppConstants.INTERNAL_NULL_KEY, entry.get("id").toString(),
					entry.get("type").toString());
		}
		List<String> tenants = getTenants();
		for (String tenantId : tenants) {
			tenantId = getTenant(tenantId);
			List<Map<String, Object>> temp = getJDBCTemplate(tenantId).queryForList(sql);
			for (Map<String, Object> entry : temp) {
				addToResult(result, tenantId, entry.get("id").toString(), entry.get("type").toString());
			}

		}
		return result;
	}

	protected abstract String getSQLForTypes();

	private void addToResult(Table<String, String, Set<String>> result, String key, String id, String type) {
		Set<String> value = result.get(key, id);

		if (value == null) {
			value = new HashSet<String>();
			result.put(key, id, value);
		}
		value.add(type);
	}


	public List<String> getStoredSubscriptions() {
		List<String> tenants;
		tenants = getTenants();
		ArrayList<String> result = new ArrayList<String>();
		try {
			result.addAll(
					getJDBCTemplate(null).queryForList("SELECT subscription_request FROM subscriptions", String.class));
			for (String tenantId : tenants) {
				tenantId = getTenant(tenantId);
				result.addAll(getJDBCTemplate(tenantId).queryForList("SELECT subscription_request FROM subscriptions",
						String.class));
			}
		} catch (DataAccessException | ResponseException e) {
			logger.error("Subscriptions could not be loaded", e);
		}
		return result;
	}

	public void storedSubscriptions(Table<String, String, SubscriptionRequest> tenant2subscriptionId2Subscription) {
		Set<String> tenants = tenant2subscriptionId2Subscription.rowKeySet();
		try {
			for (String tenant : tenants) {
				Map<String, SubscriptionRequest> row = tenant2subscriptionId2Subscription.row(tenant);
				tenant = getTenant(tenant);
				JdbcTemplate template = getJDBCTemplate(tenant);
				template.execute("DELETE FROM subscriptions");
				for (Entry<String, SubscriptionRequest> entry : row.entrySet()) {
					template.update("INSERT INTO subscriptions (subscription_id, subscription_request) VALUES (?, ?)",
							entry.getKey(), DataSerializer.toJson(entry.getValue()));
				}
			}
		} catch (DataAccessException | ResponseException e) {
			logger.error("Subscriptions could not be loaded", e);
		}

	}
}
