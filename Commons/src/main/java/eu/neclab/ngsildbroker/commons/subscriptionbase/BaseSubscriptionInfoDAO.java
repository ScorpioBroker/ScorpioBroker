package eu.neclab.ngsildbroker.commons.subscriptionbase;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.datatypes.results.QueryResult;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.ngsiqueries.ParamsResolver;
import eu.neclab.ngsildbroker.commons.storage.StorageDAO;

public abstract class BaseSubscriptionInfoDAO extends StorageDAO implements SubscriptionInfoDAOInterface {
	private final static Logger logger = LogManager.getLogger(BaseSubscriptionInfoDAO.class);

	private String dbname = getDBName();

	public Table<String, String, Set<String>> getIds2Type() throws ResponseException {
		Table<String, String, Set<String>> result = HashBasedTable.create();
		String sql = getSQLForTypes();

		for (Map<String, Object> entry : getJDBCTemplate(null).queryForList(sql)) {
			Object id = entry.get("id");
			Object type = entry.get("type");
			if (id != null && type != null) {
				addToResult(result, AppConstants.INTERNAL_NULL_KEY, id.toString(), type.toString());
			}
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

	protected abstract String getDBName();

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
					getJDBCTemplate(null).queryForList("SELECT subscription_request FROM " + dbname, String.class));
			for (String tenantId : tenants) {
				tenantId = getTenant(tenantId);
				result.addAll(getJDBCTemplate(tenantId).queryForList("SELECT subscription_request FROM " + dbname,
						String.class));
			}
		} catch (DataAccessException e) {
			logger.error("Subscriptions could not be loaded", e);
		}
		return result;
	}

	@Override
	public List<String> getEntriesFromSub(SubscriptionRequest subscriptionRequest) throws ResponseException {
		String tenant = subscriptionRequest.getTenant();
		if (AppConstants.INTERNAL_NULL_KEY.equals(tenant)) {
			tenant = null;
		}
		Subscription subscription = subscriptionRequest.getSubscription();
		List<QueryParams> qps = ParamsResolver.getQueryParamsFromSubscription(subscription);
		return query(qps, tenant);
	}

	private List<String> query(List<QueryParams> qps, String tenant) throws ResponseException {
		ArrayList<String> result = new ArrayList<String>();
		for (QueryParams qp : qps) {
			qp.setTenant(tenant);
			QueryResult qr = query(qp);
			List<String> resultString = qr.getActualDataString();
			if (resultString != null) {
				result.addAll(resultString);
			}
		}
		return result;
	}

	@Override
	public void storeSubscription(SubscriptionRequest sub) {
		String tenant = sub.getTenant();
		JdbcTemplate template;
		if (AppConstants.INTERNAL_NULL_KEY.equals(tenant)) {
			tenant = null;
			template = getJDBCTemplate(tenant);
		} else {
			template = getJDBCTemplates(sub).getWriterJdbcTemplate();
		}

		template.update("INSERT INTO " + dbname
				+ " (subscription_id, subscription_request) VALUES (?, ?) ON CONFLICT(subscription_id) DO UPDATE SET subscription_request = EXCLUDED.subscription_request",
				sub.getId(), sub.toJsonString());
	}

	@Override
	public void deleteSubscription(SubscriptionRequest sub) {
		String tenant = sub.getTenant();
		if (AppConstants.INTERNAL_NULL_KEY.equals(tenant)) {
			tenant = null;
		}
		JdbcTemplate template = getJDBCTemplate(tenant);
		template.update("DELETE FROM " + dbname + " WHERE subscription_id=?", sub.getId());

	}
}
