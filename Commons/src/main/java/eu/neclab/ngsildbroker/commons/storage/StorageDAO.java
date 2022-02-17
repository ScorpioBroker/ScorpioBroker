package eu.neclab.ngsildbroker.commons.storage;

import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLTransientConnectionException;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.sql.DataSource;

import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import eu.neclab.ngsildbroker.commons.constants.DBConstants;
import eu.neclab.ngsildbroker.commons.datatypes.HistoryAttribInstance;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.EntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.HistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.results.QueryResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.StorageFunctionsInterface;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.pgclient.PgPool;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.Tuple;

public abstract class StorageDAO {
	private final static Logger logger = LoggerFactory.getLogger(StorageDAO.class);

	protected abstract StorageFunctionsInterface getStorageFunctions();

	StorageFunctionsInterface storageFunctions;



	@Inject
	protected ClientManager clientManager;

	@PostConstruct
	void init() {
		storageFunctions = getStorageFunctions();
	}

//	public String findDataBaseNameByTenantId(String tenantidvalue) {
//		if (tenantidvalue == null)
//			return null;
//		try {
//			String databasename = "ngb" + tenantidvalue;
//			List<String> data;
//			data = writerJdbcTemplate.queryForList("SELECT datname FROM pg_database", String.class);
//			if (data.contains(databasename)) {
//				return databasename;
//			} else {
//				String modifydatabasename = " \"" + databasename + "\"";
//				String sql = "create database " + modifydatabasename + "";
//				writerJdbcTemplate.execute(sql);
//				return databasename;
//			}
//		} catch (EmptyResultDataAccessException e) {
//			return null;
//		}
//	}
//
//	public DataSource determineTargetDataSource(String tenantidvalue) {
//
//		if (tenantidvalue == null)
//			return writerDataSource;
//
//		DataSource tenantDataSource = resolvedDataSources.get(tenantidvalue);
//		if (tenantDataSource == null) {
//			try {
//				tenantDataSource = createDataSourceForTenantId(tenantidvalue);
//			} catch (ResponseException e) {
//				logger.error(e.getLocalizedMessage());
//			}
//			
//			flywayMigrate(tenantDataSource);
//			resolvedDataSources.put(tenantidvalue, tenantDataSource);
//		}
//
//		return tenantDataSource;
//	}
//
//	private DataSource createDataSourceForTenantId(String tenantidvalue) throws ResponseException {
//		String tenantDatabaseName = findDataBaseNameByTenantId(tenantidvalue);
//		if (tenantDatabaseName == null) {
//			throw new ResponseException(ErrorType.TenantNotFound, tenantidvalue + " not found");
//		}
//
//		
//		
//		HikariConfig tenantHikariConfig = new HikariConfig();
//		hikariConfig.copyStateTo(tenantHikariConfig);
//		String tenantJdbcURL = DBUtil.databaseURLFromPostgresJdbcUrl(hikariConfig.getJdbcUrl(), tenantDatabaseName);
//		tenantHikariConfig.setJdbcUrl(tenantJdbcURL);
//		tenantHikariConfig.setPoolName(tenantDatabaseName + "-db-pool");
//		return new HikariDataSource(tenantHikariConfig);
//	}

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
		PgPool client = clientManager.getClient(qp.getTenant(), false);
		if (client == null) {
			throw new ResponseException(ErrorType.TenantNotFound, qp.getTenant() + " tenant was not found");
		}
		QueryResult queryResult = new QueryResult(null, null, ErrorType.None, -1, true);

		if (qp.getCheck() != null) {
			String sqlQuery = storageFunctions.typesAndAttributeQuery(qp);
			if (sqlQuery != null && !sqlQuery.isEmpty()) {
				List<String> list = Lists.newArrayList();
				client.query(sqlQuery).executeAndAwait().forEach(t -> {
					list.add(((JsonObject) t.getJson(0)).encode());
				});
				queryResult.setDataString(list);
				queryResult.setActualDataString(list);
			}
			return queryResult;
		}
		if (qp.getCountResult()) {

			String sqlQueryCount = storageFunctions.translateNgsildQueryToCountResult(qp);
			System.err.println("2");
			System.err.println(sqlQueryCount);
			Integer count = client.query(sqlQueryCount).executeAndAwait().iterator().next().getInteger(0);
			queryResult.setCount(count);
		}
		if (qp.getLimit() == 0 && qp.getCountResult()) {
			return queryResult;
		}

		String sqlQuery = storageFunctions.translateNgsildQueryToSql(qp);
		List<String> list = Lists.newArrayList();
		System.err.println("3");
		System.err.println(sqlQuery);
		client.query(sqlQuery).executeAndAwait().forEach(t -> {
			list.add(((JsonObject) t.getJson(0)).encode());
		});
		queryResult.setDataString(list);
		queryResult.setActualDataString(list);
		return queryResult;
	}

	public void storeTemporalEntity(HistoryEntityRequest request) throws SQLException {
		PgPool client = clientManager.getClient(request.getTenant(), true);
		if (request instanceof DeleteHistoryEntityRequest) {
			doTemporalSqlAttrInsert(client, "null", request.getId(), request.getType(),
					((DeleteHistoryEntityRequest) request).getResolvedAttrId(), request.getCreatedAt(),
					request.getModifiedAt(), ((DeleteHistoryEntityRequest) request).getInstanceId(), null);
		} else {
			for (HistoryAttribInstance entry : request.getAttribs()) {
				doTemporalSqlAttrInsert(client, entry.getElementValue(), entry.getEntityId(), entry.getEntityType(),
						entry.getAttributeId(), entry.getEntityCreatedAt(), entry.getEntityModifiedAt(),
						entry.getInstanceId(), entry.getOverwriteOp());
			}
		}
	}

	public void storeRegistryEntry(CSourceRequest request) throws SQLException {
		PgPool client = clientManager.getClient(request.getTenant(), true);
		String value = request.getResultCSourceRegistrationString();
		String sql;
		int n;
		if (value != null && !value.equals("null")) {
			sql = "INSERT INTO " + DBConstants.DBTABLE_CSOURCE + " (id, " + DBConstants.DBCOLUMN_DATA
					+ ") VALUES ($1, '" + value + "'::jsonb) ON CONFLICT(id) DO UPDATE SET " + DBConstants.DBCOLUMN_DATA
					+ " = EXCLUDED." + DBConstants.DBCOLUMN_DATA;
			client.preparedQuery(sql).executeAndForget(Tuple.of(request.getId()));
		} else {
			sql = "DELETE FROM " + DBConstants.DBTABLE_CSOURCE + " WHERE id = $1";
			client.preparedQuery(sql).executeAndForget(Tuple.of(request.getId()));
		}
	}

	private void doTemporalSqlAttrInsert(PgPool client, String value, String entityId, String entityType,
			String attributeId, String entityCreatedAt, String entityModifiedAt, String instanceId,
			Boolean overwriteOp) {
		if (!value.equals("null")) {
			client.withTransaction(conn -> {
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
							recoverSql = "UPDATE " + DBConstants.DBTABLE_TEMPORALENTITY
									+ " SET modifiedat = $1::timestamp WHERE id = $2";
							recoverUnis
									.add(conn.preparedQuery(recoverSql).execute(Tuple.of(entityModifiedAt, entityId)));
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
				client.preparedQuery(sql).executeAndForget(Tuple.of(entityId, attributeId, instanceId));
			} else if (entityId != null && attributeId != null) {
				sql = "DELETE FROM " + DBConstants.DBTABLE_TEMPORALENTITY_ATTRIBUTEINSTANCE
						+ " WHERE temporalentity_id = $1 AND attributeid = $2";
				client.preparedQuery(sql).executeAndForget(Tuple.of(entityId, attributeId));
			} else if (entityId != null) {
				sql = "DELETE FROM " + DBConstants.DBTABLE_TEMPORALENTITY + " WHERE id = $1";
				client.preparedQuery(sql).executeAndForget(Tuple.of(entityId));
			}
		}

	}

	public void storeEntity(EntityRequest request) throws SQLTransientConnectionException {

		String sql;
		String key = request.getId();
		String value = request.getWithSysAttrs();
		String valueWithoutSysAttrs = request.getEntityWithoutSysAttrs();
		String kvValue = request.getKeyValue();
		PgPool client = clientManager.getClient(request.getTenant(), true);
		if (value != null && !value.equals("null")) {
			sql = "INSERT INTO " + DBConstants.DBTABLE_ENTITY + " (id, " + DBConstants.DBCOLUMN_DATA + ", "
					+ DBConstants.DBCOLUMN_DATA_WITHOUT_SYSATTRS + ",  " + DBConstants.DBCOLUMN_KVDATA
					+ ") VALUES ($1, '" + value + "'::jsonb, '" + valueWithoutSysAttrs + "'::jsonb, '" + kvValue
					+ "'::jsonb) ON CONFLICT(id) DO UPDATE SET (" + DBConstants.DBCOLUMN_DATA + ", "
					+ DBConstants.DBCOLUMN_DATA_WITHOUT_SYSATTRS + ",  " + DBConstants.DBCOLUMN_KVDATA
					+ ") = (EXCLUDED." + DBConstants.DBCOLUMN_DATA + ", EXCLUDED."
					+ DBConstants.DBCOLUMN_DATA_WITHOUT_SYSATTRS + ",  EXCLUDED." + DBConstants.DBCOLUMN_KVDATA + ")";
			client.preparedQuery(sql).executeAndForget(Tuple.of(key));

		} else {
			sql = "DELETE FROM " + DBConstants.DBTABLE_ENTITY + " WHERE id = $1";
			client.preparedQuery(sql).executeAndForget(Tuple.of(key));
		}

	}

}
