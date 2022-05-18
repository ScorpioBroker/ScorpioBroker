package eu.neclab.ngsildbroker.commons.storage;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.sql.DataSource;

import org.postgresql.ds.PGPoolingDataSource;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.DBConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.DBWriteTemplates;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.Context;
import io.vertx.mutiny.pgclient.PgPool;
import io.vertx.mutiny.sqlclient.SqlConnection;
import io.vertx.mutiny.sqlclient.Tuple;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.sqlclient.PoolOptions;

@Singleton
public class ClientManager {

	@Inject
	private PgPool pgClient;

	@Inject
	protected StorageDAO storageDAO;

	protected HashMap<String, PgPool> tenant2Client = new HashMap<String, PgPool>();

	@PostConstruct
	void loadTenantClients() {
		tenant2Client.put(AppConstants.INTERNAL_NULL_KEY, pgClient);
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

	private PgPool generateTenant(String tenant) {
		// TODO Auto-generated method stub
		return null;
	}

	public HashMap<String, PgPool> getAllClients() {
		return tenant2Client;
	}

	protected PgPool getJDBCTemplates(String tenant) throws SQLException {
		PgPool result;
		if (tenant == null) {
			result = pgClient;
		} else {
			if (tenant2Client.containsKey(tenant)) {
				result = tenant2Client.get(tenant);
			} else {
				DataSource finalDataSource = storageDAO.determineTargetDataSource(tenant);
				result = pgClient
						.connectionProvider((Function<Context, Uni<SqlConnection>>) finalDataSource.getConnection());
				tenant2Client.put(tenant, result);
			}

		}
		return result;
	}

	public String findDataBaseNameByTenantId(String tenantidvalue) {
		if (tenantidvalue == null)
			return null;

		String databasename = "ngb" + tenantidvalue;
		ArrayList<String> al1 = new ArrayList<String>();
		pgClient.query("SELECT datname FROM pg_database").executeAndAwait().forEach(t -> {
			al1.add(t.getString(0));
		});

		if (al1.contains(databasename)) {
			System.out.println("already exist");
			return databasename;

		} else {
			String modifydatabasename = " \"" + databasename + "\"";
			String sql = "create database " + modifydatabasename + "";
			pgClient.query(sql).execute().await().indefinitely();
			System.out.println("sucessfully created");
			return databasename;
		}

	}

	public void storeTenantdata(String tableName, String columnName, String tenantidvalue, String databasename)
			throws SQLException {
		String sql;
		if (!tenantidvalue.equals(null)) {
			sql = "INSERT INTO " + tableName
					+ " (tenant_id, database_name) VALUES ($1, $2) ON CONFLICT(tenant_id) DO UPDATE SET tenant_id = EXCLUDED.tenant_id";
			pgClient.preparedQuery(sql).executeAndForget(Tuple.of(tenantidvalue, databasename));
		} else {
			sql = "DELETE FROM " + tableName + " WHERE id = $1";
			pgClient.preparedQuery(sql).executeAndForget(Tuple.of(tenantidvalue));
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
				e.printStackTrace();
			}
		} else {
			tenant = null;
		}
		return tenant;

	}
	protected List<String> getTenants() {
		ArrayList<String> result = new ArrayList<String>();
		List<Map<String, Object>> temp= new ArrayList<Map<String, Object>>();
		//try {
			pgClient.query("SELECT tenant_id FROM tenant").executeAndAwait().forEach(t -> {
				
			});
		//} catch (DataAccessException e) {
		//	throw new AssertionError("Your database setup is corrupte", e);
		//}
		for (Map<String, Object> entry : temp) {
			result.add(entry.get("tenant_id").toString());
		}

		return result;
	}

	private String validateDataBaseNameByTenantId(String tenantid) {
		if (tenantid == null)
			return null;
		String databasename = "ngb" + tenantid;
		if (tenant2Client.containsKey(tenantid)) {
			return databasename;
		} else {
			ArrayList<String> data = new ArrayList<String>();
			pgClient.query("SELECT datname FROM pg_database").executeAndAwait().forEach(t -> {
				data.add(t.getString(0));
			});
			if (data.contains(databasename)) {
				return databasename;
			} else {
				return null;
			}
		}
	}

}
