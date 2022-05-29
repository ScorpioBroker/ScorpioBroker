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

import com.google.common.collect.Lists;

import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.DBConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.DBUtil;
import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.AgroalDataSourceConfiguration.DataSourceImplementation;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.api.security.NamePrincipal;
import io.agroal.api.security.SimplePassword;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.Context;
import io.vertx.mutiny.pgclient.PgPool;
import io.vertx.mutiny.sqlclient.SqlConnection;
import io.vertx.mutiny.sqlclient.Tuple;

@Singleton
public class ClientManager {

	Logger logger = LoggerFactory.getLogger(ClientManager.class);

	@Inject
	PgPool pgClient;

	@Inject
	AgroalDataSource writerDataSource;

	private Map<Object, DataSource> resolvedDataSources = new HashMap<>();

	protected HashMap<String, PgPool> tenant2Client = new HashMap<String, PgPool>();

	@PostConstruct
	void loadTenantClients() {
		tenant2Client.put(AppConstants.INTERNAL_NULL_KEY, pgClient);
	}

	public PgPool getClient(String tenant, boolean create) {
		if (tenant == null) {
			return tenant2Client.get(AppConstants.INTERNAL_NULL_KEY);
		}
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
		PgPool result = null;
		if (tenant == null) {
			result = pgClient;
		} else {
			if (tenant2Client.containsKey(tenant)) {
				result = tenant2Client.get(tenant);
			} else {
				DataSource finalDataSource;
				try {
					finalDataSource = determineTargetDataSource(tenant);
					result = pgClient.connectionProvider(
							(Function<Context, Uni<SqlConnection>>) finalDataSource.getConnection());
					tenant2Client.put(tenant, result);
				} catch (SQLException exception) {
					exception.printStackTrace();
				}

			}

		}
		return result;
	}

	public DataSource determineTargetDataSource(String tenantidvalue) throws SQLException {

		if (tenantidvalue == null)
			return writerDataSource;

		DataSource tenantDataSource = resolvedDataSources.get(tenantidvalue);
		if (tenantDataSource == null) {
			try {
				tenantDataSource = createDataSourceForTenantId(tenantidvalue);
			} catch (ResponseException e) {
				logger.error(e.getLocalizedMessage());
			}
			flywayMigrate(tenantDataSource);
			resolvedDataSources.put(tenantidvalue, tenantDataSource);
		}

		return tenantDataSource;
	}

	private DataSource createDataSourceForTenantId(String tenantidvalue) throws ResponseException, SQLException {
		String tenantDatabaseName = findDataBaseNameByTenantId(tenantidvalue);
		if (tenantDatabaseName == null) {
			throw new ResponseException(ErrorType.TenantNotFound, tenantidvalue + " not found");
		}

		String tenantJdbcURL = DBUtil.databaseURLFromPostgresJdbcUrl("jdbc:postgresql://localhost:5432/ngb",
				tenantDatabaseName);
		AgroalDataSourceConfigurationSupplier configuration = new AgroalDataSourceConfigurationSupplier()
				.dataSourceImplementation(DataSourceImplementation.AGROAL).metricsEnabled(false)
				.connectionPoolConfiguration(cp -> cp.minSize(5).maxSize(20).initialSize(10)
						.connectionFactoryConfiguration(cf -> cf.jdbcUrl(tenantJdbcURL)
								.connectionProviderClassName("org.postgresql.Driver").autoCommit(false)
								.principal(new NamePrincipal("ngb")).credential(new SimplePassword("ngb"))));
		AgroalDataSource agroaldataSource = AgroalDataSource.from(configuration);
		return agroaldataSource;
	}

	public HashMap<String, PgPool> getAllClients() {
		return tenant2Client;
	}

	public String findDataBaseNameByTenantId(String tenantidvalue) {
		if (tenantidvalue == null)
			return null;
		String databasename = "ngb" + tenantidvalue;
		List<Uni<Object>> unis = Lists.newArrayList();
		unis.add(pgClient.query("SELECT datname FROM pg_database").execute().onItem()
				.transform(pgRowSet -> pgRowSet.iterator().next()));
		if (unis.contains(databasename)) {
			return databasename;
		} else {
			String modifydatabasename = " \"" + databasename + "\"";
			String sql = "create database " + modifydatabasename + "";
			pgClient.preparedQuery(sql).executeAndForget();			
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


	protected List<String> getTenants() {
		ArrayList<String> result = new ArrayList<String>();
		pgClient.query("SELECT tenant_id FROM tenant").executeAndAwait().forEach(t -> {
			result.add(t.getString(0));

		});
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

	public Boolean flywayMigrate(DataSource tenantDataSource) {
		try {
			Flyway flyway = Flyway.configure().dataSource(tenantDataSource).locations("classpath:db/migration")
					.baselineOnMigrate(true).outOfOrder(true).load();
			flyway.repair();
			flyway.migrate();
		} catch (Exception e) {
			logger.error("failed to create tenant database", e);
			return false;
		}

		return true;
	}

}
