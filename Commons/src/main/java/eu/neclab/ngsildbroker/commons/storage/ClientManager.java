package eu.neclab.ngsildbroker.commons.storage;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import javax.sql.DataSource;
import com.google.common.collect.Maps;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.FlywayException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.DBUtil;
import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.AgroalDataSourceConfiguration.DataSourceImplementation;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.api.security.NamePrincipal;
import io.agroal.api.security.SimplePassword;
import io.quarkus.arc.Arc;
import io.quarkus.flyway.runtime.FlywayContainer;
import io.quarkus.flyway.runtime.FlywayContainerProducer;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.pgclient.PgPool;
import io.vertx.mutiny.sqlclient.Tuple;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.SslMode;
import io.vertx.sqlclient.PoolOptions;

@Singleton
public class ClientManager {

	Logger logger = LoggerFactory.getLogger(ClientManager.class);

	// @Inject
	// Create local/custom pgPool from quarkus.datasource properties, so all pgPools are created in the same way
	PgPool pgClient;

	@Inject
	AgroalDataSource writerDataSource;

	@Inject
	Vertx vertx;

	@ConfigProperty(name = "quarkus.datasource.jdbc.url")
	String jdbcBaseUrl;
	@ConfigProperty(name = "quarkus.datasource.jdbc.driver")
	String jdbcDriver;
	@ConfigProperty(name = "quarkus.datasource.username")
	String username;
	@ConfigProperty(name = "quarkus.datasource.password")
	String password;

	@ConfigProperty(name = "quarkus.flyway.migrate-at-start")
	boolean dbMigrateAtStart;
	@ConfigProperty(name = "quarkus.flyway.repair-at-start")
	boolean dbRepairAtStart;

	@ConfigProperty(name = "quarkus.datasource.reactive.url")
	String reactiveDsDefaultUrl;
	@ConfigProperty(name = "quarkus.datasource.reactive.postgresql.ssl-mode")
	String reactiveDsPostgresqlSslMode;
	@ConfigProperty(name = "quarkus.datasource.reactive.trust-all")
	boolean reactiveDsPostgresqlSslTrustAll;
	@ConfigProperty(name = "quarkus.datasource.reactive.shared")
	boolean reactiveDsShared;
	@ConfigProperty(name = "quarkus.datasource.reactive.cache-prepared-statements")
	boolean reactiveDsCachePreparedStatements;

	@ConfigProperty(name = "quarkus.datasource.reactive.max-size")
	int reactiveMaxSize;
	@ConfigProperty(name = "quarkus.datasource.reactive.idle-timeout")
	Duration idleTime;

	@ConfigProperty(name = "quarkus.transaction-manager.default-transaction-timeout")
	Duration connectionTime;

	@ConfigProperty(name = "pool.minsize")
	int minsize;
	@ConfigProperty(name = "pool.maxsize")
	int maxsize;
	@ConfigProperty(name = "pool.initialSize")
	int initialSize;

	@ConfigProperty(name = "ngsild.create-tenant-datasource-at-start", defaultValue = "false")
	boolean createTenantDatasourceAtStart;

	@ConfigProperty(name = "ngsild.datasource-test_query", defaultValue = "SELECT 1")
	String datasourceTestQuery;

	protected ConcurrentMap<String, Uni<PgPool>> tenant2Client = Maps.newConcurrentMap();

	@PostConstruct
	void loadTenantClients() throws URISyntaxException {
		logger.warn("Using custom reactive datasource connection pool!");

		logger.info("Base jdbc url: {}", new URI(jdbcBaseUrl));
		logger.info("Default reactive jdbc url: {}, sslmode: {}", new URI(reactiveDsDefaultUrl), reactiveDsPostgresqlSslMode);

		// Migration of the database defined in quarkus.datasource.jdbc.url is done automatically, no need to do it here.
		logger.info("Creating custom default reactive datasource connection pool: {}", reactiveDsDefaultUrl);
		pgClient = createPgPool("scorpio_default_pool", reactiveDsDefaultUrl);
		testPgPoolSync(pgClient, "scorpio_default_pool");
		tenant2Client.put(AppConstants.INTERNAL_NULL_KEY, Uni.createFrom().item(pgClient));
		logger.debug("Created custom default reactive datasource connection pool: {}", reactiveDsDefaultUrl);
		if (createTenantDatasourceAtStart) {
			// All tenant databases are migrated as part of the client pool creation.
			createAllTenantConnections(pgClient);
		}
	}

	public Uni<PgPool> getClient(String tenant, boolean create) {
		if (tenant == null || AppConstants.INTERNAL_NULL_KEY.equals(tenant)) {
			return tenant2Client.get(AppConstants.INTERNAL_NULL_KEY);
		}
		if (tenant2Client.containsKey(tenant)) {
			logger.trace("Tenant client cache hit for tenant {}", tenant);
			return tenant2Client.get(tenant);
		} else {
			logger.debug("Tenant client cache miss for tenant {}; asynchronously creating connection pool", tenant);
			return findDataBaseNameByTenantId(tenant, create).onItem().transformToUni(dbName -> {
			try {
				return Uni.createFrom().item(migrateDbAndCreatePgPool(tenant, dbName))
					.invoke(p -> {
						tenant2Client.put(tenant, Uni.createFrom().item(p));
						logger.debug("Cached new tenant '{}' client pool.", tenant);
					});
			} catch (SQLException e) {
				return Uni.createFrom().failure(e);
			}
		 });
		}
	}

	private String getClientPoolName(String tenant) {
		return "scorpio_tenant_" + tenant + "_pool";
	}

	private void createAllTenantConnections(PgPool pool) {
		logger.debug("Creating all reactive datasource pools");
		pool.query("SELECT tenant_id, database_name FROM public.tenant").execute().await().atMost(Duration.ofSeconds(10)).forEach(r ->
			{
				String tenant = r.getString("tenant_id");
				try {
					PgPool tenantPool = migrateDbAndCreatePgPool(tenant, r.getString("database_name"));
					tenant2Client.put(tenant, Uni.createFrom().item(tenantPool));
				} catch (SQLException e) {
					logger.error("Error creating ractive datasource pool for tenant '{}': ", tenant, e);
					e.printStackTrace();
				}
			}
		);
	}

	private PgPool createPgPool(String poolName, String databaseUrl) {
		logger.debug("Creating reactive datasource pool '{}'; database: {}, sslmode: {}", poolName, databaseUrl, reactiveDsPostgresqlSslMode);
		return PgPool.pool(vertx,
				getPgConnectOptions(databaseUrl),
				new PoolOptions()
				.setName(poolName)
				.setShared(reactiveDsShared)
				.setMaxSize(reactiveMaxSize)
				.setIdleTimeout((int) idleTime.getSeconds())
				.setIdleTimeoutUnit(TimeUnit.SECONDS)
				.setConnectionTimeout((int) connectionTime.getSeconds())
				.setConnectionTimeoutUnit(TimeUnit.SECONDS)
			);
	}

	private boolean testPgPoolSync(PgPool pool, String poolName) {
		boolean testResult = pool.query(datasourceTestQuery).execute().await().atMost(Duration.ofSeconds(5)).rowCount()==1;
		logger.debug("Reactive datasource pool {} test query {} ({})", poolName, testResult?"OK":"ERROR", pool);
		return testResult;
	}

	private PgPool migrateDbAndCreatePgPool(String tenant, String dbName) throws SQLException {
		String dbUrl = DBUtil.databaseURLFromPostgresJdbcUrl(reactiveDsDefaultUrl, dbName);
		try {
			logger.info("Creating reactive database client pool for tenant '{}': {}", tenant, dbUrl);
			flywayMigrate(tenant, dbName);
			PgPool pool = createPgPool(getClientPoolName(tenant), dbUrl);
			logger.debug("Created reactive database client pool for tenant '{}': {} ({})", tenant, dbUrl, pool);
			return pool;
		} catch (SQLException e) {
			logger.error("Client pool creation for tenant '{}' failed: Database migration error: {}", tenant, e);
			throw e;
		}
	}

	private AgroalDataSource createDatasourceForTenant(String tenant, String dbName) throws SQLException {
		String tenantJdbcURL = "jdbc:" + DBUtil.databaseURLFromPostgresJdbcUrl(jdbcBaseUrl, dbName);
		logger.debug("Creating datasource for tenant '{}' with jdbc url: {}", tenant, tenantJdbcURL);
		return createDatasource(tenantJdbcURL);
	}

	private AgroalDataSource createDatasource(String jdbcURL) throws SQLException {
		// TODO this needs to be from the config not hardcoded!!!
		AgroalDataSourceConfigurationSupplier configuration = new AgroalDataSourceConfigurationSupplier()
				.dataSourceImplementation(DataSourceImplementation.AGROAL).metricsEnabled(false)
				.connectionPoolConfiguration(
						cp -> cp.minSize(minsize).maxSize(maxsize).initialSize(initialSize)
								.connectionFactoryConfiguration(cf -> cf.jdbcUrl(jdbcURL)
										.connectionProviderClassName(jdbcDriver)
										.autoCommit(false)
										.principal(new NamePrincipal(username))
										.credential(new SimplePassword(password))));
		return AgroalDataSource.from(configuration);
	}

	private Uni<String> findDataBaseNameByTenantId(String tenant, boolean create) {
		String databasename = "ngb" + tenant.hashCode();
		String databasenameWithoutHash = "ngb" + tenant;
		return pgClient.preparedQuery("SELECT datname FROM pg_database where datname = $1 OR datname = $2")
				.execute(Tuple.of(databasename, databasenameWithoutHash)).onItem().transformToUni(pgRowSet -> {
					if (pgRowSet.size() == 0) {
						if (create) {
							return pgClient.preparedQuery("CREATE DATABASE \"" + databasename + "\"").execute().onItem()
									.transformToUni(t -> {
										return storeTenantdata(tenant, databasename).onItem()
												.transform(t2 -> databasename);
									});
						} else {
							return Uni.createFrom().failure(
									new ResponseException(ErrorType.TenantNotFound, tenant + " tenant was not found"));
						}
					} else {
						return pgClient.preparedQuery("SELECT datname FROM pg_database where datname = $1")
								.execute(Tuple.of(databasenameWithoutHash)).onItem().transformToUni(rowSet -> {
									if (rowSet.size() != 0) {
										return Uni.createFrom().item(databasenameWithoutHash);
									} else
										return Uni.createFrom().item(databasename);
								});
					}
				});
	}

	private Uni<Void> storeTenantdata(String tenantidvalue, String databasename) {
		return pgClient.preparedQuery(
				"INSERT INTO tenant (tenant_id, database_name) VALUES ($1, $2) ON CONFLICT(tenant_id) DO UPDATE SET tenant_id = EXCLUDED.tenant_id")
				.execute(Tuple.of(tenantidvalue, databasename)).onItem().ignore().andContinueWithNull();
	}

	private void flywayMigrate(String tenant, String dbName) throws FlywayException, SQLException {
		logger.debug("Running database migration for tenant '{}' on database '{}'", tenant, dbName);
		try (AgroalDataSource tenantDataSource = createDatasourceForTenant(tenant, dbName);) {
			flywayMigrate(tenant, tenantDataSource);
			logger.debug("Tenant '{}' database migration finished", tenant);
		}
	}

	private void flywayMigrate(String tenant, DataSource tenantDataSource) throws FlywayException {
		FlywayContainerProducer flywayProducer = Arc.container().instance(FlywayContainerProducer.class).get();
		FlywayContainer flywayContainer = flywayProducer.createFlyway(tenantDataSource, "scoprpio_tenant_" + tenant + "_datasource", true, true);
		Flyway flyway = flywayContainer.getFlyway();
		try {
			flyway.migrate();
		} catch (FlywayException e) {
			if (dbRepairAtStart) {
				logger.warn("Tenant '{}' database migration failed, attempting repair.", tenant, e);
				try {
					flyway.repair();
					flyway.migrate();
				} catch (FlywayException fe) {
					logger.error("Tenant '{}' database repair and migration failed!", tenant, fe);
					throw fe;
				}
			} else {
				logger.error("Tenant '{}' database migration failed!", tenant, e);
				throw e;
			}
		}
	}

    public PgConnectOptions getDefaultPgConnectionOptions() {
        return getPgConnectOptions(reactiveDsDefaultUrl);
    }

	private PgConnectOptions getPgConnectOptions(String databaseUrl) {
			return PgConnectOptions.fromUri(databaseUrl)
			.setUser(username)
			.setPassword(password)
			.setCachePreparedStatements(reactiveDsCachePreparedStatements)
			.setSslMode(SslMode.valueOf(reactiveDsPostgresqlSslMode.toUpperCase()))
			.setTrustAll(reactiveDsPostgresqlSslTrustAll);
	}
}
