package eu.neclab.ngsildbroker.entityhandler.services;

import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import com.google.gson.Gson;

import eu.neclab.ngsildbroker.commons.constants.DBConstants;
import eu.neclab.ngsildbroker.commons.datatypes.TemporalEntityStorageKey;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;

@Repository("emstorage")
@ConditionalOnProperty(value = "writer.enabled", havingValue = "true", matchIfMissing = false)
public class StorageWriterDAO {

	private final static Logger logger = LogManager.getLogger(StorageWriterDAO.class);
//	public static final Gson GSON = DataSerializer.GSON;

	@Autowired
	private JdbcTemplate writerJdbcTemplate;

	@Autowired
	private DataSource writerDataSource;

	private TransactionTemplate writerTransactionTemplate;
	private JdbcTemplate writerJdbcTemplateWithTransaction;

	@PostConstruct
	public void init() {
		writerJdbcTemplate.execute("SELECT 1"); // create connection pool and connect to database

		// https://gist.github.com/mdellabitta/1444003
		DataSourceTransactionManager transactionManager = new DataSourceTransactionManager(writerDataSource);
		writerJdbcTemplateWithTransaction = new JdbcTemplate(transactionManager.getDataSource());
		writerTransactionTemplate = new TransactionTemplate(transactionManager);
	}

	public boolean store(String tableName, String columnName, String key, String value) {
		try {
			String sql;
			int n = 0;
			if (value != null && !value.equals("null")) {
				sql = "INSERT INTO " + tableName + " (id, " + columnName
						+ ") VALUES (?, ?::jsonb) ON CONFLICT(id) DO UPDATE SET " + columnName + " = EXCLUDED."
						+ columnName;
				n = writerJdbcTemplate.update(sql, key, value);
			} else {
				sql = "DELETE FROM " + tableName + " WHERE id = ?";
				n = writerJdbcTemplate.update(sql, key);
			}
			logger.trace("Rows affected: " + Integer.toString(n));
			return true; // (n>0);
		} catch (Exception e) {
			logger.error("Exception ::", e);
			e.printStackTrace();
		}
		return false;
	}

	public boolean storeEntity(String key, String value, String valueWithoutSysAttrs, String kvValue)
			throws SQLTransientConnectionException {
		String sql;
		int n = 0;
		if (value != null && !value.equals("null")) {
			sql = "INSERT INTO " + DBConstants.DBTABLE_ENTITY + " (id, " + DBConstants.DBCOLUMN_DATA + ", "
					+ DBConstants.DBCOLUMN_DATA_WITHOUT_SYSATTRS + ",  " + DBConstants.DBCOLUMN_KVDATA
					+ ") VALUES (?, ?::jsonb, ?::jsonb, ?::jsonb) ON CONFLICT(id) DO UPDATE SET ("
					+ DBConstants.DBCOLUMN_DATA + ", " + DBConstants.DBCOLUMN_DATA_WITHOUT_SYSATTRS + ",  "
					+ DBConstants.DBCOLUMN_KVDATA + ") = (EXCLUDED." + DBConstants.DBCOLUMN_DATA + ", EXCLUDED."
					+ DBConstants.DBCOLUMN_DATA_WITHOUT_SYSATTRS + ",  EXCLUDED." + DBConstants.DBCOLUMN_KVDATA + ")";
			n = writerJdbcTemplate.update(sql, key, value, valueWithoutSysAttrs, kvValue);
		} else {
			sql = "DELETE FROM " + DBConstants.DBTABLE_ENTITY + " WHERE id = ?";
			n = writerJdbcTemplate.update(sql, key);
		}
		logger.trace("Rows affected: " + Integer.toString(n));
		return true; // (n>0);

	}

	public boolean storeTemporalEntity(String key, String value) throws SQLException {
		try {

			TemporalEntityStorageKey tesk = DataSerializer.getTemporalEntityStorageKey(key);

			String entityId = tesk.getEntityId();
			String entityType = tesk.getEntityType();
			String entityCreatedAt = tesk.getEntityCreatedAt();
			String entityModifiedAt = tesk.getEntityModifiedAt();

			String attributeId = tesk.getAttributeId();
			String instanceId = tesk.getInstanceId();
			Boolean overwriteOp = tesk.getOverwriteOp();

			Integer n = 0;

			if (!value.equals("null")) {
				// https://gist.github.com/mdellabitta/1444003
				n = writerTransactionTemplate.execute(new TransactionCallback<Integer>() {
					@Override
					public Integer doInTransaction(TransactionStatus status) {
						String sql;
						Integer tn = 0;
						if (entityId != null && entityType != null && entityCreatedAt != null
								&& entityModifiedAt != null) {
							sql = "INSERT INTO " + DBConstants.DBTABLE_TEMPORALENTITY
									+ " (id, type, createdat, modifiedat) VALUES (?, ?, ?::timestamp, ?::timestamp) ON CONFLICT(id) DO UPDATE SET type = EXCLUDED.type, createdat = EXCLUDED.createdat, modifiedat = EXCLUDED.modifiedat";
							tn = writerJdbcTemplateWithTransaction.update(sql, entityId, entityType, entityCreatedAt,
									entityModifiedAt);
						}

						if (entityId != null && attributeId != null) {
							if (overwriteOp != null && overwriteOp) {
								sql = "DELETE FROM " + DBConstants.DBTABLE_TEMPORALENTITY_ATTRIBUTEINSTANCE
										+ " WHERE temporalentity_id = ? AND attributeid = ?";
								tn += writerJdbcTemplateWithTransaction.update(sql, entityId, attributeId);
							}
							sql = "INSERT INTO " + DBConstants.DBTABLE_TEMPORALENTITY_ATTRIBUTEINSTANCE
									+ " (temporalentity_id, attributeid, data) VALUES (?, ?, ?::jsonb) ON CONFLICT(temporalentity_id, attributeid, instanceid) DO UPDATE SET data = EXCLUDED.data";
							tn += writerJdbcTemplateWithTransaction.update(sql, entityId, attributeId, value);
							// update modifiedat field in temporalentity
							sql = "UPDATE " + DBConstants.DBTABLE_TEMPORALENTITY
									+ " SET modifiedat = ?::timestamp WHERE id = ?";
							tn += writerJdbcTemplateWithTransaction.update(sql, entityModifiedAt, entityId);
						}
						return tn;

					}
				});
			} else {
				String sql;
				if (entityId != null && attributeId != null && instanceId != null) {
					sql = "DELETE FROM " + DBConstants.DBTABLE_TEMPORALENTITY_ATTRIBUTEINSTANCE
							+ " WHERE temporalentity_id = ? AND attributeid = ? AND instanceid = ?";
					n = writerJdbcTemplate.update(sql, entityId, attributeId, instanceId);
				} else if (entityId != null && attributeId != null) {
					sql = "DELETE FROM " + DBConstants.DBTABLE_TEMPORALENTITY_ATTRIBUTEINSTANCE
							+ " WHERE temporalentity_id = ? AND attributeid = ?";
					n = writerJdbcTemplate.update(sql, entityId, attributeId);
				} else if (entityId != null) {
					sql = "DELETE FROM " + DBConstants.DBTABLE_TEMPORALENTITY + " WHERE id = ?";
					n = writerJdbcTemplate.update(sql, entityId);
				}
			}

			logger.debug("Rows affected: " + Integer.toString(n));
			return true;
		} catch (Exception e) {
			logger.error("Exception ::", e);
			e.printStackTrace();
		}
		return false;
	}

}
