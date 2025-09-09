package eu.neclab.ngsildbroker.commons.storage;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import java.util.concurrent.TimeUnit;

import jakarta.enterprise.event.Observes;
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
import io.quarkus.arc.Arc;
import io.quarkus.flyway.runtime.FlywayContainer;
import io.quarkus.flyway.runtime.FlywayContainerProducer;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Uni;

import io.smallrye.mutiny.unchecked.Unchecked;
import io.vertx.mutiny.core.Vertx;

import io.vertx.mutiny.sqlclient.Pool;

import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.Tuple;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.sqlclient.PoolOptions;

@Singleton
public class ConnectionManager {

	Logger logger = LoggerFactory.getLogger(ConnectionManager.class);

	@Inject
	Pool pgClient;

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

	@ConfigProperty(name = "scorpio.postgres.username")
	String dbUser;

	@ConfigProperty(name = "scorpio.postgres.disablejit", defaultValue = "true")
	boolean disableJIT;

	void onStart(@Observes StartupEvent ev) throws URISyntaxException {
		if (disableJIT) {
			executeQuery(null, "ALTER USER " + dbUser + " SET jit = off;", null, false).await().indefinitely();
		} else {
			executeQuery(null, "ALTER USER " + dbUser + " SET jit = on;", null, false).await().indefinitely();
		}

		URI uri = new URI(reactiveDefaultUrl);
		reactiveBaseUrl = uri.getScheme() + "://" + uri.getHost() + ":" + uri.getPort() + "/";
		tenant2Client.put(AppConstants.INTERNAL_NULL_KEY, pgClient);
	}

	private String reactiveBaseUrl;

	private Map<String, Pool> tenant2Client = Maps.newHashMap();

	public Uni<RowSet<Row>> executeQuery(String tenant, String sql, Tuple tuple, boolean createTenant) {
		Pool client;
		if (tenant == null) {
			client = pgClient;
		} else {
			client = tenant2Client.get(tenant);
		}

		if (client != null) {
			if (tuple != null) {
				return client.preparedQuery(sql).execute(tuple);
			} else {
				return client.preparedQuery(sql).execute();
			}
		} else {
			if (!createTenant) {
				return Uni.createFrom().failure(
						new ResponseException(ErrorType.TenantNotFound, tenant + " tenant was not found"));
			}
			return getTenant(tenant, createTenant).onItem().transformToUni(tenantClient -> {
				if (tuple != null) {
					return tenantClient.preparedQuery(sql).execute(tuple);
				} else {
					return tenantClient.preparedQuery(sql).execute();
				}
			});
		}
	}

	public Uni<RowSet<Row>> executeBatchQuery(String tenant, String sql, List<Tuple> tuples, boolean createTenant) {
		Pool client;
		if (tenant == null) {
			client = pgClient;
		} else {
			client = tenant2Client.get(tenant);
		}
		if (client != null) {
			return client.preparedQuery(sql).executeBatch(tuples);
		} else {
			if (!createTenant) {
				return Uni.createFrom().failure(
						new ResponseException(ErrorType.TenantNotFound, tenant + " tenant was not found"));
			}
			return getTenant(tenant, createTenant).onItem().transformToUni(tenantClient -> {
				return tenantClient.preparedQuery(sql).executeBatch(tuples);
			});
		}
	}

	public Uni<Void> executeBatchQueryWithPreStep(String tenant, String sql, List<Tuple> tuples, String preStepSql,
			Tuple preStepTuple, boolean createTenant) {
		Pool client;
		if (tenant == null) {
			client = pgClient;
		} else {
			client = tenant2Client.get(tenant);
		}
		if (client != null) {
			return client.getConnection().onItem().transformToUni(conn -> {
				Uni<RowSet<Row>> tmp;
				if (preStepTuple != null) {
					tmp = conn.preparedQuery(preStepSql).execute(preStepTuple);
				} else {
					tmp = conn.preparedQuery(preStepSql).execute();
				}
				return tmp.onItem().transformToUni(ignored -> {
					if (tuples == null || tuples.isEmpty()) {
						return conn.close();
					}
					return conn.preparedQuery(sql).executeBatch(tuples).onItem().transformToUni(ignoredToo -> {
						return conn.close();
					});
				});
			});

		} else {
			if (!createTenant) {
				return Uni.createFrom().failure(
						new ResponseException(ErrorType.TenantNotFound, tenant + " tenant was not found"));
			}
			return getTenant(tenant, createTenant).onItem().transformToUni(tenantClient -> {
				return tenantClient.getConnection().onItem().transformToUni(conn -> {
					Uni<RowSet<Row>> tmp;
					if (preStepTuple != null) {
						tmp = conn.preparedQuery(preStepSql).execute(preStepTuple);
					} else {
						tmp = conn.preparedQuery(preStepSql).execute();
					}
					return tmp.onItem().transformToUni(ignored -> {
						if (tuples == null) {
							return conn.close();
						}
						return conn.preparedQuery(sql).executeBatch(tuples).onItem().transformToUni(ignoredToo -> {
							return conn.close();
						});
					});
				});
			});
		}
	}

	private Uni<Pool> getTenant(String tenant, boolean createDB) {
		return determineTargetDataSource(tenant, createDB).onItem().transform(Unchecked.function(finalDataBase -> {
			PoolOptions options = new PoolOptions();
			options.setName(finalDataBase);
			options.setShared(true);
			options.setMaxSize(reactiveMaxSize);
			options.setIdleTimeout((int) idleTime.getSeconds());
			options.setIdleTimeoutUnit(TimeUnit.SECONDS);
			options.setConnectionTimeout((int) connectionTime.getSeconds());
			options.setConnectionTimeoutUnit(TimeUnit.SECONDS);

			Pool pool = Pool.pool(vertx, PgConnectOptions.fromUri(reactiveBaseUrl + finalDataBase).setUser(username)
					.setPassword(password).setCachePreparedStatements(true), options);

			tenant2Client.put(tenant, pool);
			return pool;
		}));
	}

	public Uni<String> determineTargetDataSource(String tenantidvalue, boolean createDB) {
		return createDataSourceForTenantId(tenantidvalue, createDB).onItem().transform(tenant -> {
			return tenant;
		});
	}

	private Uni<String> createDataSourceForTenantId(String tenantidvalue, boolean createDB) {
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
					flywayMigrate(agroaldataSource);
					return tenantDatabaseName;
				}));

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

		FlywayContainerProducer flywayProducer = Arc.container().instance(FlywayContainerProducer.class).get();
		FlywayContainer flywayContainer = flywayProducer.createFlyway(tenantDataSource, "<default>", true, true);
		Flyway flyway = flywayContainer.getFlyway();
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
