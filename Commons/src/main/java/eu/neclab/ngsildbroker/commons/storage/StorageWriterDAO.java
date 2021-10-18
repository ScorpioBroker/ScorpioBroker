package eu.neclab.ngsildbroker.commons.storage;

import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.flywaydb.core.Flyway;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import eu.neclab.ngsildbroker.commons.constants.DBConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.DBWriteTemplates;
import eu.neclab.ngsildbroker.commons.datatypes.DeleteHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.EntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.HistoryAttribInstance;
import eu.neclab.ngsildbroker.commons.datatypes.HistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tenant.DBUtil;

@Repository
@ConditionalOnProperty(value = "writer.enabled", havingValue = "true", matchIfMissing = false)
public class StorageWriterDAO {

	private final static Logger logger = LogManager.getLogger(StorageWriterDAO.class);
//	public static final Gson GSON = DataSerializer.GSON;

	@Autowired
	private JdbcTemplate writerJdbcTemplate;

	@Autowired
	private DataSource writerDataSource;

	@Autowired
	private HikariConfig hikariConfig;

	private Map<Object, DataSource> resolvedDataSources = new HashMap<>();

	private TransactionTemplate writerTransactionTemplate;
	private JdbcTemplate writerJdbcTemplateWithTransaction;

	private DBWriteTemplates defaultTemplates;
	private HashMap<String, DBWriteTemplates> tenant2Templates = new HashMap<String, DBWriteTemplates>();

	@PostConstruct
	public void init() {
		writerJdbcTemplate.execute("SELECT 1"); // create connection pool and connect to database

		// https://gist.github.com/mdellabitta/1444003
		DataSourceTransactionManager transactionManager = new DataSourceTransactionManager(writerDataSource);
		writerJdbcTemplateWithTransaction = new JdbcTemplate(transactionManager.getDataSource());
		writerTransactionTemplate = new TransactionTemplate(transactionManager);
		this.defaultTemplates = new DBWriteTemplates(writerJdbcTemplateWithTransaction, writerTransactionTemplate,
				writerJdbcTemplate);
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

	public boolean store(String tableName, String columnName, String key, String value, String tenantvalue)
			throws SQLException {
		try {
			String sql;
			int n = 0;
			DBWriteTemplates templates = getJDBCTemplates(tenantvalue);

			if (!value.equals("null")) {
				sql = "INSERT INTO " + tableName + " (id, " + columnName
						+ ") VALUES (?, ?::jsonb) ON CONFLICT(id) DO UPDATE SET " + columnName + " = EXCLUDED."
						+ columnName;
				n = templates.getWriterJdbcTemplate().update(sql, key, value);
			} else {
				sql = "DELETE FROM " + tableName + " WHERE id = ?";
				n = templates.getWriterJdbcTemplate().update(sql, key);
			}

			logger.trace("Rows affected: " + Integer.toString(n));
			return true; // (n>0);
		} catch (Exception e) {
			logger.error("Exception ::", e);
			e.printStackTrace();
		}

		return false;
	}

	public boolean storeTemporalEntity(HistoryEntityRequest request) throws SQLException {
		boolean result = true;
		DBWriteTemplates templates = getJDBCTemplates(request);
		// TemporalEntityStorageKey tesk =
		// DataSerializer.getTemporalEntityStorageKey(key);

		/*
		 * String entityId = request.getId(); String entityType = request.getType();
		 * String entityCreatedAt = request.getCreatedAt(); String entityModifiedAt =
		 * request.getModifiedAt();
		 */
		String instanceId = request.getInstanceId();
		if (request instanceof DeleteHistoryEntityRequest) {
			result = doTemporalSqlAttrInsert(templates, "null", request.getId(), request.getType(), null,
					request.getCreatedAt(), request.getModifiedAt(), instanceId, null);
		} else {
			for (HistoryAttribInstance entry : request.getAttribs()) {
				result = result && doTemporalSqlAttrInsert(templates, entry.getElementValue(), entry.getEntityId(),
						entry.getEntityType(), entry.getAttributeId(), entry.getEntityCreatedAt(),
						entry.getEntityModifiedAt(), instanceId, entry.getOverwriteOp());
			}
		}
		return result;
	}

	private boolean doTemporalSqlAttrInsert(DBWriteTemplates templates, String value, String entityId,
			String entityType, String attributeId, String entityCreatedAt, String entityModifiedAt, String instanceId,
			Boolean overwriteOp) {
		try {
			Integer n = 0;

			if (!value.equals("null")) {
				// https://gist.github.com/mdellabitta/1444003
				try {
					n = templates.getWriterTransactionTemplate().execute(new TransactionCallback<Integer>() {
						@Override
						public Integer doInTransaction(TransactionStatus status) {
							String sql;
							Integer tn = 0;
							if (entityId != null && entityType != null && entityCreatedAt != null
									&& entityModifiedAt != null) {
								sql = "INSERT INTO " + DBConstants.DBTABLE_TEMPORALENTITY
										+ " (id, type, createdat, modifiedat) VALUES (?, ?, ?::timestamp, ?::timestamp) ON CONFLICT(id) DO UPDATE SET type = EXCLUDED.type, createdat = EXCLUDED.createdat, modifiedat = EXCLUDED.modifiedat";
								tn = templates.getWriterJdbcTemplateWithTransaction().update(sql, entityId, entityType,
										entityCreatedAt, entityModifiedAt);
							}

							if (entityId != null && attributeId != null) {
								if (overwriteOp != null && overwriteOp) {
									sql = "DELETE FROM " + DBConstants.DBTABLE_TEMPORALENTITY_ATTRIBUTEINSTANCE
											+ " WHERE temporalentity_id = ? AND attributeid = ?";
									tn += templates.getWriterJdbcTemplateWithTransaction().update(sql, entityId,
											attributeId);
								}
								sql = "INSERT INTO " + DBConstants.DBTABLE_TEMPORALENTITY_ATTRIBUTEINSTANCE
										+ " (temporalentity_id, attributeid, data) VALUES (?, ?, ?::jsonb) ON CONFLICT(temporalentity_id, attributeid, instanceid) DO UPDATE SET data = EXCLUDED.data";
								tn += templates.getWriterJdbcTemplateWithTransaction().update(sql, entityId,
										attributeId, value);
								// update modifiedat field in temporalentity
								sql = "UPDATE " + DBConstants.DBTABLE_TEMPORALENTITY
										+ " SET modifiedat = ?::timestamp WHERE id = ?";
								tn += templates.getWriterJdbcTemplateWithTransaction().update(sql, entityModifiedAt,
										entityId);

							}

							return tn;

						}
					});
				} catch (DataIntegrityViolationException e) {
					logger.info("Failed to create attribute instance because of data inconsistency");
					logger.info("Attempting recovery");
					try {
						n = templates.getWriterTransactionTemplate().execute(new TransactionCallback<Integer>() {
							@Override
							public Integer doInTransaction(TransactionStatus status) {
								String sql;
								Integer tn = 0;
								sql = "SELECT type, createdat, modifiedat FROM " + DBConstants.DBTABLE_ENTITY
										+ " WHERE id = ?";
								List<Map<String, Object>> tempResult = templates.getWriterJdbcTemplateWithTransaction()
										.queryForList(sql, entityId);
								if (tempResult.isEmpty()) {
									logger.error("Recovery failed");
									return tn;
								}
								Map<String, Object> entitySql = tempResult.get(0);
								sql = "INSERT INTO " + DBConstants.DBTABLE_TEMPORALENTITY
										+ " (id, type, createdat, modifiedat) VALUES (?, ?, ?::timestamp, ?::timestamp) ON CONFLICT(id) DO UPDATE SET type = EXCLUDED.type, createdat = EXCLUDED.createdat, modifiedat = EXCLUDED.modifiedat";
								tn = templates.getWriterJdbcTemplateWithTransaction().update(sql, entityId,
										entitySql.get("type"), entitySql.get("createdat"), entitySql.get("modifiedat"));
								sql = "INSERT INTO " + DBConstants.DBTABLE_TEMPORALENTITY_ATTRIBUTEINSTANCE
										+ " (temporalentity_id, attributeid, data) VALUES (?, ?, ?::jsonb) ON CONFLICT(temporalentity_id, attributeid, instanceid) DO UPDATE SET data = EXCLUDED.data";
								tn += templates.getWriterJdbcTemplateWithTransaction().update(sql, entityId,
										attributeId, value);
								// update modifiedat field in temporalentity
								sql = "UPDATE " + DBConstants.DBTABLE_TEMPORALENTITY
										+ " SET modifiedat = ?::timestamp WHERE id = ?";
								tn += templates.getWriterJdbcTemplateWithTransaction().update(sql, entityModifiedAt,
										entityId);
								return tn;
							}
						});
						logger.info("Recovery successful");

					} catch (Exception e1) {
						logger.error("Recovery failed", e1);
					}
				}
			} else {
				String sql;
				if (entityId != null && attributeId != null && instanceId != null) {
					sql = "DELETE FROM " + DBConstants.DBTABLE_TEMPORALENTITY_ATTRIBUTEINSTANCE
							+ " WHERE temporalentity_id = ? AND attributeid = ? AND instanceid = ?";
					n = templates.getWriterJdbcTemplate().update(sql, entityId, attributeId, instanceId);
				} else if (entityId != null && attributeId != null) {
					sql = "DELETE FROM " + DBConstants.DBTABLE_TEMPORALENTITY_ATTRIBUTEINSTANCE
							+ " WHERE temporalentity_id = ? AND attributeid = ?";
					n = templates.getWriterJdbcTemplate().update(sql, entityId, attributeId);
				} else if (entityId != null) {
					sql = "DELETE FROM " + DBConstants.DBTABLE_TEMPORALENTITY + " WHERE id = ?";
					n = templates.getWriterJdbcTemplate().update(sql, entityId);
				}
			}
			logger.debug("Rows affected: " + Integer.toString(n));
			return true;
		} catch (Exception e) {
			logger.error("Exception ::", e);
			return false;
		}

	}

	public boolean storeEntity(EntityRequest request) throws SQLTransientConnectionException {

		String sql;
		String key = request.getId();
		String value = request.getWithSysAttrs();
		String valueWithoutSysAttrs = request.getEntityWithoutSysAttrs();
		String kvValue = request.getKeyValue();
		int n = 0;
		DBWriteTemplates templates = getJDBCTemplates(request);
		if (value != null && !value.equals("null")) {
			sql = "INSERT INTO " + DBConstants.DBTABLE_ENTITY + " (id, " + DBConstants.DBCOLUMN_DATA + ", "
					+ DBConstants.DBCOLUMN_DATA_WITHOUT_SYSATTRS + ",  " + DBConstants.DBCOLUMN_KVDATA
					+ ") VALUES (?, ?::jsonb, ?::jsonb, ?::jsonb) ON CONFLICT(id) DO UPDATE SET ("
					+ DBConstants.DBCOLUMN_DATA + ", " + DBConstants.DBCOLUMN_DATA_WITHOUT_SYSATTRS + ",  "
					+ DBConstants.DBCOLUMN_KVDATA + ") = (EXCLUDED." + DBConstants.DBCOLUMN_DATA + ", EXCLUDED."
					+ DBConstants.DBCOLUMN_DATA_WITHOUT_SYSATTRS + ",  EXCLUDED." + DBConstants.DBCOLUMN_KVDATA + ")";
			n = templates.getWriterJdbcTemplate().update(sql, key, value, valueWithoutSysAttrs, kvValue);
		} else {
			sql = "DELETE FROM " + DBConstants.DBTABLE_ENTITY + " WHERE id = ?";
			n = templates.getWriterJdbcTemplate().update(sql, key);
		}
		logger.trace("Rows affected: " + Integer.toString(n));
		return true; // (n>0);

	}

	/*
	 * private void setJDBCTemplate(BaseRequest request) { synchronized
	 * (writerJdbcTemplate) { String tenant = getTenant(request); DataSource
	 * finalDataSource; if (tenant != null) { finalDataSource =
	 * tenantAwareDataSource.determineTargetDataSource(); } else { finalDataSource =
	 * writerDataSource; } DataSourceTransactionManager transactionManager = new
	 * DataSourceTransactionManager(finalDataSource);
	 * writerJdbcTemplateWithTransaction = new
	 * JdbcTemplate(transactionManager.getDataSource()); writerTransactionTemplate =
	 * new TransactionTemplate(transactionManager); writerJdbcTemplate = new
	 * JdbcTemplate(finalDataSource); } }
	 */
	private DBWriteTemplates getJDBCTemplates(BaseRequest request) {
		return getJDBCTemplates(getTenant(request));
	}

	private DBWriteTemplates getJDBCTemplates(String tenant) {
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
		if (request.getHeaders().containsKey(NGSIConstants.TENANT_HEADER)) {
			tenant = request.getHeaders().get(NGSIConstants.TENANT_HEADER).get(0);
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
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			flywayMigrate(tenantDataSource);
			resolvedDataSources.put(tenantidvalue, tenantDataSource);
		}

		return tenantDataSource;
	}

	private DataSource createDataSourceForTenantId(String tenantidvalue) throws ResponseException {
		String tenantDatabaseName = findDataBaseNameByTenantId(tenantidvalue);
		if (tenantDatabaseName == null)
			throw new ResponseException(ErrorType.TenantNotFound);
		// throw new IllegalArgumentException("Given tenant id is not valid : " +
		// tenantidvalue);
		HikariConfig tenantHikariConfig = new HikariConfig();
		hikariConfig.copyStateTo(tenantHikariConfig);
		String tenantJdbcURL = DBUtil.databaseURLFromPostgresJdbcUrl(hikariConfig.getJdbcUrl(), tenantDatabaseName);
		tenantHikariConfig.setJdbcUrl(tenantJdbcURL);
		tenantHikariConfig.setPoolName(tenantDatabaseName + "-db-pool");
		return new HikariDataSource(tenantHikariConfig);
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

}
