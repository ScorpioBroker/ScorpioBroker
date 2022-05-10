package eu.neclab.ngsildbroker.commons.storage;

import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLTransientConnectionException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.DBConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.DBWriteTemplates;
import eu.neclab.ngsildbroker.commons.datatypes.HistoryAttribInstance;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.EntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.HistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.results.QueryResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.StorageFunctionsInterface;
import eu.neclab.ngsildbroker.commons.tools.DBUtil;
import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.AgroalDataSourceConfiguration.DataSourceImplementation;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.api.security.NamePrincipal;
import io.agroal.api.security.SimplePassword;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.pgclient.PgPool;
import io.vertx.mutiny.sqlclient.Tuple;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowSet;

public abstract class StorageDAO {
	private final static Logger logger = LoggerFactory.getLogger(StorageDAO.class);

//	@Autowired
//	private JdbcTemplate writerJdbcTemplate;

	@Inject
	protected ClientManager clientManager;

	@Inject
	AgroalDataSource writerDataSource;
//
//	@Autowired
//	private HikariConfig hikariConfig;
	private Map<Object, DataSource> resolvedDataSources = new HashMap<>();
//	private TransactionTemplate writerTransactionTemplate;
//	private JdbcTemplate writerJdbcTemplateWithTransaction;
//	private DBWriteTemplates defaultTemplates;
//	private HashMap<String, DBWriteTemplates> tenant2Templates = new HashMap<String, DBWriteTemplates>();

	protected abstract StorageFunctionsInterface getStorageFunctions();

	StorageFunctionsInterface storageFunctions;

	@PostConstruct
	public void init() {
//		writerJdbcTemplate.execute("SELECT 1"); // create connection pool and connect to database
//		DataSourceTransactionManager transactionManager = new DataSourceTransactionManager(writerDataSource);
//		writerJdbcTemplateWithTransaction = new JdbcTemplate(transactionManager.getDataSource());
//		writerTransactionTemplate = new TransactionTemplate(transactionManager);
//		this.defaultTemplates = new DBWriteTemplates(writerJdbcTemplateWithTransaction, writerTransactionTemplate,
//				writerJdbcTemplate);
		storageFunctions = getStorageFunctions();
	}

	public boolean storeTenantdata(String tableName, String columnName, String tenantidvalue, String databasename)
			throws SQLException {
		try {
			String sql;
			int n = 0;
			if (!tenantidvalue.equals(null)) {
				sql = "INSERT INTO " + tableName
						+ " (tenant_id, database_name) VALUES (?, ?) ON CONFLICT(tenant_id) DO UPDATE SET tenant_id = EXCLUDED.tenant_id";
				synchronized (writerJdbcTemplate) {
					n = writerJdbcTemplate.update(sql, tenantidvalue, databasename);
				}
			} else {
				sql = "DELETE FROM " + tableName + " WHERE id = ?";
				synchronized (writerJdbcTemplate) {
					n = writerJdbcTemplate.update(sql, tenantidvalue);
				}
			}
			logger.trace("Rows affected: " + Integer.toString(n));
			return true; // (n>0);
		} catch (Exception e) {
			logger.error("Exception ::", e);
			e.printStackTrace();
		}
		return false;
	}

	protected DBWriteTemplates getJDBCTemplates(BaseRequest request) {
		return getJDBCTemplates(getTenant(request));
	}

	protected DBWriteTemplates getJDBCTemplates(String tenant) {
		DBWriteTemplates result;
		if (tenant == null) {
			result = defaultTemplates;
		} else {
			if (tenant2Templates.containsKey(tenant)) {
				result = tenant2Templates.get(tenant);
			} else {
				DataSource finalDataSource = determineTargetDataSource(tenant);
				DataSourceTransactionManager transactionManager = new DataSourceTransactionManager(finalDataSource);
				result = new DBWriteTemplates(new JdbcTemplate(transactionManager.getDataSource()),
						new TransactionTemplate(transactionManager), new JdbcTemplate(finalDataSource));
				tenant2Templates.put(tenant, result);
			}

		}
		return result;
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

	public String findDataBaseNameByTenantId(String tenantidvalue) {
		if (tenantidvalue == null)
			return null;
		try {
			String databasename = "ngb" + tenantidvalue;
			List<String> data;
			data = writerJdbcTemplate.queryForList("SELECT datname FROM pg_database", String.class);
			if (data.contains(databasename)) {
				return databasename;
			} else {
				String modifydatabasename = " \"" + databasename + "\"";
				String sql = "create database " + modifydatabasename + "";
				writerJdbcTemplate.execute(sql);
				return databasename;
			}
		} catch (EmptyResultDataAccessException e) {
			return null;
		}
	}

	public DataSource determineTargetDataSource(String tenantidvalue) {

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
		// HikariConfig tenantHikariConfig = new HikariConfig();
		// hikariConfig.copyStateTo(tenantHikariConfig);
		String tenantJdbcURL = DBUtil.databaseURLFromPostgresJdbcUrl("jdbc:postgresql://localhost:5432/ngb",
				tenantDatabaseName);
		AgroalDataSourceConfigurationSupplier configuration = new AgroalDataSourceConfigurationSupplier()
				.dataSourceImplementation(DataSourceImplementation.AGROAL).metricsEnabled(false)
				.connectionPoolConfiguration(cp -> cp.minSize(5).maxSize(20).initialSize(10)
						.connectionFactoryConfiguration(cf -> cf.jdbcUrl(tenantJdbcURL)
								.connectionProviderClassName("org.postgresql.Driver").autoCommit(false)
								.principal(new NamePrincipal("ngb")).credential(new SimplePassword("ngb"))));
		// tenantHikariConfig.setJdbcUrl(tenantJdbcURL);
		// tenantHikariConfig.setPoolName(tenantDatabaseName + "-db-pool");
		// return new HikariDataSource(tenantHikariConfig);
		AgroalDataSource agroaldataSource = AgroalDataSource.from(configuration);
		return agroaldataSource;
	}

	public Boolean flywayMigrate(DataSource tenantDataSource) {
		try {
			Flyway flyway = Flyway.configure().dataSource(tenantDataSource).locations("classpath:db/migration")
					.baselineOnMigrate(true).outOfOrder(true).load();
			flyway.repair();
			flyway.migrate();
		} catch (Exception e) {
			return false;
		}

		return true;
	}

	public Uni<QueryResult> query(QueryParams qp){
		// JdbcTemplate template;
		PgPool client = clientManager.getClient(qp.getTenant(), false);

		QueryResult queryResult = new QueryResult(null, null, ErrorType.None, -1, true);
		try {
			String tenantId = qp.getTenant();
			if (tenantId != null) {
				String tenantDatabaseName = validateDataBaseNameByTenantId(tenantId);
				if (tenantDatabaseName == null) {
					throw new ResponseException(ErrorType.TenantNotFound, tenantId + " not found");
				}
			}
			template = getJDBCTemplates(tenantId).getWriterJdbcTemplate();
		} catch (Exception e) {
			throw new ResponseException(ErrorType.TenantNotFound, "tenant was not found");
		}
		try {
			if (qp.getCheck() != null) {
				String sqlQuery = storageFunctions.typesAndAttributeQuery(qp);
				List<String> list = template.queryForList(sqlQuery, String.class);
				queryResult.setDataString(list);
				queryResult.setActualDataString(list);
				return queryResult;
			}
			if (qp.getCountResult() == true) {
				if (qp.getLimit() == 0) {
					String sqlQueryCount = storageFunctions.translateNgsildQueryToCountResult(qp);
					Integer count = template.queryForObject(sqlQueryCount, Integer.class);
					queryResult.setCount(count);
					return queryResult;
				}
				String sqlQuery = storageFunctions.translateNgsildQueryToSql(qp);
				List<String> list = template.queryForList(sqlQuery, String.class);
				queryResult.setDataString(list);
				queryResult.setActualDataString(list);
				String sqlQueryCount = storageFunctions.translateNgsildQueryToCountResult(qp);
				Integer count = template.queryForObject(sqlQueryCount, Integer.class);
				queryResult.setCount(count);
				return queryResult;
			} else {
				String sqlQuery = storageFunctions.translateNgsildQueryToSql(qp);
				List<String> list = template.queryForList(sqlQuery, String.class);
				queryResult.setDataString(list);
				queryResult.setActualDataString(list);
				return queryResult;
			}
		} catch (DataIntegrityViolationException e) {
			// Empty result don't worry
			logger.debug("SQL Result Exception::", e);
			return queryResult;
		} catch (Exception e) {
			logger.error("Exception ::", e);
		}
		return queryResult;
	}

	public Uni<Void> storeTemporalEntity(HistoryEntityRequest request) throws SQLException {

		PgPool client = clientManager.getClient(request.getTenant(), true);

		if (request instanceof DeleteHistoryEntityRequest) {
			return doTemporalSqlAttrInsert(client, "null", request.getId(), request.getType(),
					((DeleteHistoryEntityRequest) request).getResolvedAttrId(), request.getCreatedAt(),
					request.getModifiedAt(), ((DeleteHistoryEntityRequest) request).getInstanceId(), null);
		} else {
			List<Uni<Void>> unis = Lists.newArrayList();
			for (HistoryAttribInstance entry : request.getAttribs()) {
				unis.add(doTemporalSqlAttrInsert(client, entry.getElementValue(), entry.getEntityId(),
						entry.getEntityType(), entry.getAttributeId(), entry.getEntityCreatedAt(),
						entry.getEntityModifiedAt(), entry.getInstanceId(), entry.getOverwriteOp()));
			}
			return Uni.combine().all().unis(unis).discardItems();
		}

	}

	public Uni<Void> storeRegistryEntry(CSourceRequest request) throws SQLException, ResponseException {
		PgPool client = clientManager.getClient(request.getTenant(), true);
		String value = request.getResultCSourceRegistrationString();
		String sql;
		Uni uni = null;
		if (value != null && !value.equals("null")) {
			if (request.getRequestType() == AppConstants.OPERATION_CREATE_ENTITY) {

				sql = "INSERT INTO " + DBConstants.DBTABLE_CSOURCE + " (id, " + DBConstants.DBCOLUMN_DATA
						+ ") VALUES ($1, '" + value + "'::jsonb) ON CONFLICT(id) DO NOTHING";
				uni = client.preparedQuery(sql).execute(Tuple.of(request.getId())).onFailure().retry().atMost(3)
						.onItem().ignore().andContinueWithNull();
				if (uni == null) {
					throw new ResponseException(ErrorType.AlreadyExists, "CSource already exists");
				}
			} else if (request.getRequestType() == AppConstants.OPERATION_APPEND_ENTITY) {
				sql = "INSERT INTO " + DBConstants.DBTABLE_CSOURCE + " (id, " + DBConstants.DBCOLUMN_DATA
						+ ") VALUES ($1, '" + value + "'::jsonb) ON CONFLICT(id) DO UPDATE SET "
						+ DBConstants.DBCOLUMN_DATA + " = EXCLUDED." + DBConstants.DBCOLUMN_DATA;
				uni = client.preparedQuery(sql).execute(Tuple.of(request.getId())).onFailure().retry().atMost(3)
						.onItem().ignore().andContinueWithNull();
			}
		} else {
			sql = "DELETE FROM " + DBConstants.DBTABLE_CSOURCE + " WHERE id = $1";
			uni = client.preparedQuery(sql).execute(Tuple.of(request.getId())).onFailure().retry().atMost(3).onItem()
					.ignore().andContinueWithNull();
		}
		return uni;
	}

	private Uni<Void> doTemporalSqlAttrInsert(PgPool client, String value, String entityId, String entityType,
			String attributeId, String entityCreatedAt, String entityModifiedAt, String instanceId,
			Boolean overwriteOp) {
		if (!value.equals("null")) {
			return client.withTransaction(conn -> {
				String sql;
				List<Uni<RowSet<Row>>> unis = Lists.newArrayList();
				if (entityId != null && entityType != null && entityCreatedAt != null && entityModifiedAt != null) {
					sql = "INSERT INTO " + DBConstants.DBTABLE_TEMPORALENTITY
							+ " (id, type, createdat, modifiedat) VALUES ($1, $2, $3::timestamp, $4::timestamp) ON CONFLICT(id) DO UPDATE SET type = EXCLUDED.type, createdat = EXCLUDED.createdat, modifiedat = EXCLUDED.modifiedat";
					unis.add(conn.preparedQuery(sql)
							.execute(Tuple.of(entityId, entityType, entityCreatedAt, entityModifiedAt)));
				}
				if (entityId != null && attributeId != null) {
					if (overwriteOp != null && overwriteOp) {
						sql = "DELETE FROM " + DBConstants.DBTABLE_TEMPORALENTITY_ATTRIBUTEINSTANCE
								+ " WHERE temporalentity_id = $1 AND attributeid = $2";
						unis.add(conn.preparedQuery(sql).execute(Tuple.of(entityId, attributeId)));
					}
					sql = "INSERT INTO " + DBConstants.DBTABLE_TEMPORALENTITY_ATTRIBUTEINSTANCE
							+ " (temporalentity_id, attributeid, data) VALUES ($1, $2, '" + value
							+ "'::jsonb) ON CONFLICT(temporalentity_id, attributeid, instanceid) DO UPDATE SET data = EXCLUDED.data";
					unis.add(conn.preparedQuery(sql).execute(Tuple.of(entityId, attributeId)));
					// update modifiedat field in temporalentity
					sql = "UPDATE " + DBConstants.DBTABLE_TEMPORALENTITY
							+ " SET modifiedat = $1::timestamp WHERE id = $2";
					unis.add(conn.preparedQuery(sql).execute(Tuple.of(entityModifiedAt, entityId)));
				}

				return Uni.combine().all().unis(unis).discardItems().onFailure().recoverWithItem(t -> {

					if (t instanceof SQLIntegrityConstraintViolationException) {

						List<Uni<RowSet<Row>>> recoverUnis = Lists.newArrayList();
						logger.info("Failed to create attribute instance because of data inconsistency");
						logger.info("Attempting recovery");
						String selectSql = "SELECT type, createdat, modifiedat FROM " + DBConstants.DBTABLE_ENTITY
								+ " WHERE id = $1";
						conn.preparedQuery(selectSql).execute(Tuple.of(entityId)).onItem().call(rows -> {
							if (rows.size() == 0) {
								logger.error("Recovery failed");
								return Uni.createFrom().nullItem();
							}
							Row row = rows.iterator().next();
							String recoverSql;
							recoverSql = "INSERT INTO " + DBConstants.DBTABLE_TEMPORALENTITY
									+ " (id, type, createdat, modifiedat) VALUES ($1, $2, $3::timestamp, $4::timestamp) ON CONFLICT(id) DO UPDATE SET type = EXCLUDED.type, createdat = EXCLUDED.createdat, modifiedat = EXCLUDED.modifiedat";
							recoverUnis.add(conn.preparedQuery(recoverSql).execute(Tuple.of(entityId,
									row.getString("type"), row.getString("createdat"), row.getString("modifiedat"))));
							recoverSql = "INSERT INTO " + DBConstants.DBTABLE_TEMPORALENTITY_ATTRIBUTEINSTANCE
									+ " (temporalentity_id, attributeid, data) VALUES ($1, $2, '" + value
									+ "'::jsonb) ON CONFLICT(temporalentity_id, attributeid, instanceid) DO UPDATE SET data = EXCLUDED.data";
							recoverUnis.add(conn.preparedQuery(recoverSql).execute(Tuple.of(entityId, attributeId)));
							// update modifiedat field in temporalentity
							recoverSql = "UPDATE " + DBConstants.DBTABLE_TEMPORALENTITY + " SET modifiedat = '"
									+ entityModifiedAt + "'::timestamp WHERE id = $1";
							recoverUnis.add(conn.preparedQuery(recoverSql).execute(Tuple.of(entityId)));
							logger.info("Recovery successful");
							return Uni.combine().all().unis(recoverUnis).discardItems();
						});
					} else {
						logger.error("Recovery failed", t);
					}
					return null;
				});
			});
		} else {
			String sql;
			if (entityId != null && attributeId != null && instanceId != null) {
				sql = "DELETE FROM " + DBConstants.DBTABLE_TEMPORALENTITY_ATTRIBUTEINSTANCE
						+ " WHERE temporalentity_id = $1 AND attributeid = $2 AND instanceid = $3";
				return client.preparedQuery(sql).execute(Tuple.of(entityId, attributeId, instanceId)).replaceWithVoid();
			} else if (entityId != null && attributeId != null) {
				sql = "DELETE FROM " + DBConstants.DBTABLE_TEMPORALENTITY_ATTRIBUTEINSTANCE
						+ " WHERE temporalentity_id = $1 AND attributeid = $2";
				return client.preparedQuery(sql).execute(Tuple.of(entityId, attributeId)).replaceWithVoid();
			} else if (entityId != null) {
				sql = "DELETE FROM " + DBConstants.DBTABLE_TEMPORALENTITY + " WHERE id = $1";
				return client.preparedQuery(sql).execute(Tuple.of(entityId)).replaceWithVoid();
			}
			return Uni.createFrom().nullItem();
		}

	}

	public Uni<Void> storeEntity(EntityRequest request) {

		String sql;
		String key = request.getId();
		String value = request.getWithSysAttrs();
		String valueWithoutSysAttrs = request.getEntityWithoutSysAttrs();
		String kvValue = request.getKeyValue();
		int n = 0;
		Uni uni;
		PgPool client = clientManager.getClient(request.getTenant(), true);
		if (value != null && !value.equals("null")) {
			if (request.getRequestType() == AppConstants.OPERATION_CREATE_ENTITY) {
				sql = "INSERT INTO " + DBConstants.DBTABLE_ENTITY + " (id, " + DBConstants.DBCOLUMN_DATA + ", "
						+ DBConstants.DBCOLUMN_DATA_WITHOUT_SYSATTRS + ",  " + DBConstants.DBCOLUMN_KVDATA
						+ ") VALUES ($1, '" + value + "'::jsonb, '" + valueWithoutSysAttrs + "'::jsonb, '" + kvValue
						+ "'::jsonb) ON CONFLICT(id) DO NOTHING";
				uni = client.preparedQuery(sql).execute(Tuple.of(key)).onFailure().retry().atMost(3).onItem().ignore()
						.andContinueWithNull();
				//TODO check this failure should go up failure stream
				/*if (uni == null) {
					throw new ResponseException(ErrorType.AlreadyExists, request.getId() + " already exists");

				}*/
			} else {
				sql = "UPDATE " + DBConstants.DBTABLE_ENTITY + " SET " + DBConstants.DBCOLUMN_DATA + " = '" + value
						+ "'::jsonb , " + DBConstants.DBCOLUMN_DATA_WITHOUT_SYSATTRS + " = '" + valueWithoutSysAttrs
						+ "'::jsonb , " + DBConstants.DBCOLUMN_KVDATA + " = '" + kvValue + "'::jsonb WHERE "
						+ DBConstants.DBCOLUMN_ID + "='" + key + "'";
				uni = client.preparedQuery(sql).execute(Tuple.of(key)).onFailure().retry().atMost(3).onItem().ignore()
						.andContinueWithNull();
			}
		} else {

			sql = "DELETE FROM " + DBConstants.DBTABLE_ENTITY + " WHERE id = $1";
			uni = client.preparedQuery(sql).execute(Tuple.of(key)).onFailure().retry().atMost(3).onItem().ignore()
					.andContinueWithNull();
		}
		logger.trace("Rows affected: " + Integer.toString(n));
		return uni; // (n>0);

	}

	protected JdbcTemplate getJDBCTemplate(String tenantId) {
		return getJDBCTemplates(tenantId).getWriterJdbcTemplate();
	}

	protected List<String> getTenants() {
		ArrayList<String> result = new ArrayList<String>();
		List<Map<String, Object>> temp;
		try {
			temp = getJDBCTemplate(null).queryForList("SELECT tenant_id FROM tenant");
		} catch (DataAccessException e) {
			throw new AssertionError("Your database setup is corrupte", e);
		}
		for (Map<String, Object> entry : temp) {
			result.add(entry.get("tenant_id").toString());
		}

		return result;
	}

	protected String getTenant(String tenantId) {
		if (tenantId == null) {
			return null;
		}
		if (AppConstants.INTERNAL_NULL_KEY.equals(tenantId)) {
			return null;
		}
		return tenantId;
	}

	private String validateDataBaseNameByTenantId(String tenantid) {
		if (tenantid == null)
			return null;
		try {
			String databasename = "ngb" + tenantid;
			if (tenant2Templates.containsKey(tenantid)) {
				return databasename;
			} else {
				List<String> data;
				data = writerJdbcTemplate.queryForList("SELECT datname FROM pg_database", String.class);
				if (data.contains(databasename)) {
					return databasename;
				} else {
					return null;
				}
			}
		} catch (EmptyResultDataAccessException e) {
			return null;
		}
	}
}
