package eu.neclab.ngsildbroker.commons.storage;

import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;

import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import com.google.common.collect.ArrayListMultimap;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

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

public abstract class StorageDAO {
	private final static Logger logger = LoggerFactory.getLogger(StorageDAO.class);

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

	protected abstract StorageFunctionsInterface getStorageFunctions();

	StorageFunctionsInterface storageFunctions;

	@PostConstruct
	public void init() {
		writerJdbcTemplate.execute("SELECT 1"); // create connection pool and connect to database
		DataSourceTransactionManager transactionManager = new DataSourceTransactionManager(writerDataSource);
		writerJdbcTemplateWithTransaction = new JdbcTemplate(transactionManager.getDataSource());
		writerTransactionTemplate = new TransactionTemplate(transactionManager);
		this.defaultTemplates = new DBWriteTemplates(writerJdbcTemplateWithTransaction, writerTransactionTemplate,
				writerJdbcTemplate);
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

	private DataSource createDataSourceForTenantId(String tenantidvalue) throws ResponseException {
		String tenantDatabaseName = findDataBaseNameByTenantId(tenantidvalue);
		if (tenantDatabaseName == null) {
			throw new ResponseException(ErrorType.TenantNotFound, tenantidvalue + " not found");
		}
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

	public QueryResult query(QueryParams qp) throws ResponseException {
		JdbcTemplate template;
		QueryResult queryResult = new QueryResult(null, null, ErrorType.None, -1, true);
		try {

			String tenantId = qp.getTenant();
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

	public boolean storeTemporalEntity(HistoryEntityRequest request) throws SQLException{
		boolean result = true;
		DBWriteTemplates templates = getJDBCTemplates(request);

		if (request instanceof DeleteHistoryEntityRequest) {
			result = doTemporalSqlAttrInsert(templates, "null", request.getId(), request.getType(),
					((DeleteHistoryEntityRequest) request).getResolvedAttrId(), request.getCreatedAt(),
					request.getModifiedAt(), ((DeleteHistoryEntityRequest) request).getInstanceId(), null);
		} else {
			for (HistoryAttribInstance entry : request.getAttribs()) {
				result = result && doTemporalSqlAttrInsert(templates, entry.getElementValue(), entry.getEntityId(),
						entry.getEntityType(), entry.getAttributeId(), entry.getEntityCreatedAt(),
						entry.getEntityModifiedAt(), entry.getInstanceId(), entry.getOverwriteOp());
			}
		}
		return result;
	}

	public boolean storeRegistryEntry(CSourceRequest request) throws SQLException, ResponseException {
		DBWriteTemplates templates = getJDBCTemplates(request);
		String value = request.getResultCSourceRegistrationString();
		String sql;
		int n = 0;
		if (value != null && !value.equals("null")) {
			if(request.getRequestType()==0){
			
			sql = "INSERT INTO " + DBConstants.DBTABLE_CSOURCE + " (id, " + DBConstants.DBCOLUMN_DATA
					+ ") VALUES (?, ?::jsonb) ON CONFLICT(id) DO NOTHING";
			n = templates.getWriterJdbcTemplate().update(sql, request.getId(), value);
			if (n == 0) {
				throw new ResponseException(ErrorType.AlreadyExists, "CSource already exists");
			}
			}
			else if(request.getRequestType()==2) {
				sql = "INSERT INTO " + DBConstants.DBTABLE_CSOURCE + " (id, " + DBConstants.DBCOLUMN_DATA
						+ ") VALUES (?, ?::jsonb) ON CONFLICT(id) DO UPDATE SET " + DBConstants.DBCOLUMN_DATA
						+ " = EXCLUDED." + DBConstants.DBCOLUMN_DATA;
				n = templates.getWriterJdbcTemplate().update(sql, request.getId(), value);
			}
		} else {
			sql = "DELETE FROM " + DBConstants.DBTABLE_CSOURCE + " WHERE id = ?";
			n = templates.getWriterJdbcTemplate().update(sql, request.getId());
		}
		return n > 0;
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
				}
				catch (DataIntegrityViolationException e) {
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

	public boolean storeEntity(EntityRequest request) throws SQLTransientConnectionException, ResponseException {

		String sql;
		String key = request.getId();
		String value = request.getWithSysAttrs();
		String valueWithoutSysAttrs = request.getEntityWithoutSysAttrs();
		String kvValue = request.getKeyValue();
		int n = 0;
		DBWriteTemplates templates = getJDBCTemplates(request);
		if (value != null && !value.equals("null")) {
			if (request.getRequestType() == 0) {
				sql = "INSERT INTO " + DBConstants.DBTABLE_ENTITY + " (id, " + DBConstants.DBCOLUMN_DATA + ", "
						+ DBConstants.DBCOLUMN_DATA_WITHOUT_SYSATTRS + ",  " + DBConstants.DBCOLUMN_KVDATA
						+ ") VALUES (?, ?::jsonb, ?::jsonb, ?::jsonb) ON CONFLICT(id) DO NOTHING";
				n = templates.getWriterJdbcTemplate().update(sql, key, value, valueWithoutSysAttrs, kvValue);
				if (n == 0) {
					throw new ResponseException(ErrorType.AlreadyExists, request.getId() + " already exists");

				}
			} else {
				sql = "INSERT INTO " + DBConstants.DBTABLE_ENTITY + " (id, " + DBConstants.DBCOLUMN_DATA + ", "
						+ DBConstants.DBCOLUMN_DATA_WITHOUT_SYSATTRS + ",  " + DBConstants.DBCOLUMN_KVDATA
						+ ") VALUES (?, ?::jsonb, ?::jsonb, ?::jsonb) ON CONFLICT(id) DO UPDATE SET ("
						+ DBConstants.DBCOLUMN_DATA + ", " + DBConstants.DBCOLUMN_DATA_WITHOUT_SYSATTRS + ",  "
						+ DBConstants.DBCOLUMN_KVDATA + ") = (EXCLUDED." + DBConstants.DBCOLUMN_DATA + ", EXCLUDED."
						+ DBConstants.DBCOLUMN_DATA_WITHOUT_SYSATTRS + ",  EXCLUDED." + DBConstants.DBCOLUMN_KVDATA
						+ ")";
				n = templates.getWriterJdbcTemplate().update(sql, key, value, valueWithoutSysAttrs, kvValue);

			}
		} else {

			sql = "DELETE FROM " + DBConstants.DBTABLE_ENTITY + " WHERE id = ?";
			n = templates.getWriterJdbcTemplate().update(sql, key);
		}
		logger.trace("Rows affected: " + Integer.toString(n));
		return true; // (n>0);

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
	
	protected String getTenant(String tenantId){
		if (tenantId == null) {
			return null;
		}
		if (AppConstants.INTERNAL_NULL_KEY.equals(tenantId)) {
			return null;
		}
		return tenantId;
	}
}
