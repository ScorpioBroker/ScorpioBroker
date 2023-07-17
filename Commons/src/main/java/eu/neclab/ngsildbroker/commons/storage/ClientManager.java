package eu.neclab.ngsildbroker.commons.storage;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import jakarta.annotation.PostConstruct;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import javax.sql.DataSource;
import com.google.common.collect.Maps;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.flywaydb.core.Flyway;
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
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.mutiny.unchecked.Unchecked;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.pgclient.PgPool;
import io.vertx.mutiny.sqlclient.Tuple;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.sqlclient.PoolOptions;

@Singleton
public class ClientManager {

	Logger logger = LoggerFactory.getLogger(ClientManager.class);

	@Inject
	PgPool pgClient;

	@Inject
	AgroalDataSource writerDataSource;

	@Inject
	Vertx vertx;

	@ConfigProperty(name = "quarkus.datasource.reactive.url")
	String reactiveDefaultUrl;
	@ConfigProperty(name = "quarkus.datasource.jdbc.url")
	String jdbcBaseUrl;
	@ConfigProperty(name = "quarkus.datasource.jdbc.driver")
	String jdbcDriver;
	@ConfigProperty(name = "quarkus.datasource.username")
	String username;
	@ConfigProperty(name = "quarkus.datasource.password")
	String password;

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

	private String reactiveBaseUrl;
	protected ConcurrentMap<String, Uni<PgPool>> tenant2Client = Maps.newConcurrentMap();

	@PostConstruct
	void loadTenantClients() throws URISyntaxException {
		URI uri = new URI(reactiveDefaultUrl);
		reactiveBaseUrl = uri.getScheme() + "://" + uri.getHost() + ":" + uri.getPort() + "/";
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
		return determineTargetDataSource(tenant, createDB).onItem().transformToUni(Unchecked.function(finalDataBase -> {
			PoolOptions options = new PoolOptions();
			options.setName(finalDataBase);
			options.setShared(true);
			options.setMaxSize(reactiveMaxSize);
			options.setIdleTimeout((int) idleTime.getSeconds());
			options.setIdleTimeoutUnit(TimeUnit.SECONDS);
			options.setConnectionTimeout((int) connectionTime.getSeconds());
			options.setConnectionTimeoutUnit(TimeUnit.SECONDS);

			PgPool pool = PgPool.pool(vertx, PgConnectOptions.fromUri(reactiveBaseUrl + finalDataBase).setUser(username)
					.setPassword(password).setCachePreparedStatements(true), options);
			Uni<PgPool> result = Uni.createFrom().item(pool);
			tenant2Client.put(tenant, result);
			return result;
		}));
	}

	public Uni<String> determineTargetDataSource(String tenantidvalue, boolean createDB) {
		return createDataSourceForTenantId(tenantidvalue, createDB).onItem().transform(tenantDataSource -> {
			flywayMigrate(tenantDataSource.getItem1());
			tenantDataSource.getItem1().close();
			return tenantDataSource.getItem2();
		});
	}

	private Uni<Tuple2<AgroalDataSource, String>> createDataSourceForTenantId(String tenantidvalue, boolean createDB) {
		return findDataBaseNameByTenantId(tenantidvalue, createDB).onItem()
				.transform(Unchecked.function(tenantDatabaseName -> {
					// TODO this needs to be from the config not hardcoded!!!
					String tenantJdbcURL = DBUtil.databaseURLFromPostgresJdbcUrl(jdbcBaseUrl, tenantDatabaseName);
					AgroalDataSourceConfigurationSupplier configuration = new AgroalDataSourceConfigurationSupplier()
							.dataSourceImplementation(DataSourceImplementation.AGROAL).metricsEnabled(false)
							.connectionPoolConfiguration(
									cp -> cp.minSize(minsize).maxSize(maxsize).initialSize(initialSize)
											.connectionFactoryConfiguration(cf -> cf.jdbcUrl(tenantJdbcURL)
													.connectionProviderClassName(jdbcDriver).autoCommit(false)
													.principal(new NamePrincipal(username))
													.credential(new SimplePassword(password))));
					AgroalDataSource agroaldataSource = AgroalDataSource.from(configuration);
					return Tuple2.of(agroaldataSource, tenantDatabaseName);
				}));

	}

	public ConcurrentMap<String, Uni<PgPool>> getAllClients() {
		return tenant2Client;
	}

	public Uni<String> findDataBaseNameByTenantId(String tenant, boolean create) {
		String databasename = "ngb" + tenant.hashCode();
		String databasenameWithoutHash = "ngb" + tenant;
		return pgClient.preparedQuery("SELECT datname FROM pg_database where datname = $1 OR datname = $2")
				.execute(Tuple.of(databasename, databasenameWithoutHash)).onItem().transformToUni(pgRowSet -> {
					if (pgRowSet.size() == 0) {
						if (create) {
							return pgClient.preparedQuery("create database \"" + databasename + "\"").execute().onItem()
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

	public Boolean flywayMigrate(DataSource tenantDataSource) {
		Flyway flyway = Flyway.configure().dataSource(tenantDataSource).locations("classpath:db/migration")
				.baselineOnMigrate(true).outOfOrder(true).cleanDisabled(true).load();
		try {
			flyway.migrate();
		} catch (Exception e) {
			logger.warn("failed to create tenant database attempting repair", e);
			try {
				flyway.repair();
				flyway.migrate();
			} catch (Exception e1) {
				logger.error("repair failed", e);
				return false;
			}

		}

		return true;
	}

}
