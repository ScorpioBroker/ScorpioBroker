package eu.neclab.ngsildbroker.commons.storage;

import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.inject.Singleton;

import org.flywaydb.core.Flyway;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.DBConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import io.vertx.mutiny.pgclient.PgPool;
import io.vertx.mutiny.sqlclient.Tuple;

@Singleton
public class ClientManager {

	@Inject
	PgPool defaultClient;


	protected HashMap<String, PgPool> tenant2Client = new HashMap<String, PgPool>();

	@PostConstruct
	void loadTenantClients() {
		tenant2Client.put(AppConstants.INTERNAL_NULL_KEY, defaultClient);

	}

	public PgPool getClient(String tenant, boolean create) {
		PgPool result = tenant2Client.get(tenant);
		if (create) {
			if (result == null) {
				result = generateTenant(tenant);
				tenant2Client.put(tenant, result);
			}
		}
		return result;

	}

	public HashMap<String,PgPool> getAllClients() {
		return tenant2Client;
	}

	private PgPool generateTenant(String tenant) {
		// TODO Auto-generated method stub
		return null;
	}

	public void storeTenantdata(String tableName, String columnName, String tenantidvalue, String databasename)
			throws SQLException {
		String sql;
		if (!tenantidvalue.equals(null)) {
			sql = "INSERT INTO " + tableName
					+ " (tenant_id, database_name) VALUES ($1, $2) ON CONFLICT(tenant_id) DO UPDATE SET tenant_id = EXCLUDED.tenant_id";
			defaultClient.preparedQuery(sql).executeAndForget(Tuple.of(tenantidvalue, databasename));
		} else {
			sql = "DELETE FROM " + tableName + " WHERE id = $1";
			defaultClient.preparedQuery(sql).executeAndForget(Tuple.of(tenantidvalue));
		}
	}

	private String getTenant(BaseRequest request) {
		String tenant;
		if (request.getHeaders().containsKey(NGSIConstants.TENANT_HEADER_FOR_INTERNAL_CHECK)) {
			tenant = request.getHeaders().get(NGSIConstants.TENANT_HEADER_FOR_INTERNAL_CHECK).get(0);
			String databasename = "ngb" + tenant;
			try {
				storeTenantdata(DBConstants.DBTABLE_CSOURCE_TENANT, DBConstants.DBCOLUMN_DATA_TENANT, tenant,
						databasename);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			tenant = null;
		}
		return tenant;

	}
}
