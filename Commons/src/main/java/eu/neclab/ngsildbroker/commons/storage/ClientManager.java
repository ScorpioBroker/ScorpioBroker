package eu.neclab.ngsildbroker.commons.storage;

import java.sql.SQLException;
import java.util.HashMap;
import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Maps;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.DBUtil;
import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.AgroalDataSourceConfiguration.DataSourceImplementation;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.api.security.NamePrincipal;
import io.agroal.api.security.SimplePassword;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.unchecked.Unchecked;
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

	// private Map<Object, DataSource> resolvedDataSources = Maps.newHashMap();

	protected HashMap<String, Uni<PgPool>> tenant2Client = Maps.newHashMap();

	@PostConstruct
	void loadTenantClients() {
		tenant2Client.put(AppConstants.INTERNAL_NULL_KEY, Uni.createFrom().item(pgClient));
	}

	public Uni<PgPool> getClient(String tenant, boolean create) {
		if (tenant == null) {
			return tenant2Client.get(AppConstants.INTERNAL_NULL_KEY);
		}
		Uni<PgPool> result = tenant2Client.get(tenant);
		if (result == null) {
			result = getTenant(tenant, create);
			tenant2Client.put(tenant, result);
		}
		return result;
	}

	private Uni<PgPool> getTenant(String tenant, boolean createDB) {
		return determineTargetDataSource(tenant, createDB).onItem()
				.transformToUni(Unchecked.function(finalDataSource -> {

					Uni<PgPool> result = Uni.createFrom().item(pgClient.connectionProvider(t -> {
						try {
							return Uni.createFrom().item((SqlConnection) finalDataSource.getConnection());
						} catch (SQLException e) {
							return Uni.createFrom().failure(e);
						}
					}));
					tenant2Client.put(tenant, result);
					return result;
				}));
	}

	public Uni<DataSource> determineTargetDataSource(String tenantidvalue, boolean createDB) {
		return createDataSourceForTenantId(tenantidvalue, createDB).onItem().transform(tenantDataSource -> {
			flywayMigrate(tenantDataSource);
			return tenantDataSource;
		});
	}

	private Uni<DataSource> createDataSourceForTenantId(String tenantidvalue, boolean createDB) {
		return findDataBaseNameByTenantId(tenantidvalue, createDB).onItem()
				.transform(Unchecked.function(tenantDatabaseName -> {
					// TODO this needs to be from the config not hardcoded!!!
					String tenantJdbcURL = DBUtil.databaseURLFromPostgresJdbcUrl("jdbc:postgresql://localhost:5432/ngb",
							tenantDatabaseName);
					AgroalDataSourceConfigurationSupplier configuration = new AgroalDataSourceConfigurationSupplier()
							.dataSourceImplementation(DataSourceImplementation.AGROAL).metricsEnabled(false)
							.connectionPoolConfiguration(cp -> cp.minSize(5).maxSize(20).initialSize(10)
									.connectionFactoryConfiguration(cf -> cf.jdbcUrl(tenantJdbcURL)
											.connectionProviderClassName("org.postgresql.Driver").autoCommit(false)
											.principal(new NamePrincipal("ngb"))
											.credential(new SimplePassword("ngb"))));
					AgroalDataSource agroaldataSource = AgroalDataSource.from(configuration);
					return agroaldataSource;
				}));

	}

	public HashMap<String, Uni<PgPool>> getAllClients() {
		return tenant2Client;
	}

	public Uni<String> findDataBaseNameByTenantId(String tenant, boolean create) {
		String databasename = "ngb" + tenant;
		return pgClient.preparedQuery("SELECT datname FROM pg_database where datname = $1")
				.execute(Tuple.of(databasename)).onItem().transformToUni(pgRowSet -> {
					if (pgRowSet.size() == 0) {
						if (create) {
							return pgClient.preparedQuery("create database \"" + databasename + "\"").execute().onItem()
									.transform(t -> databasename);
						} else {
							return Uni.createFrom().failure(
									new ResponseException(ErrorType.TenantNotFound, tenant + " tenant was not found"));

						}
					} else {
						return Uni.createFrom().item(databasename);
					}

				});
	}

	// TODO check not called by anyone... is there a good reason to record tenants
	// like this instead of just getting the databases?
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
