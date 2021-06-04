package eu.neclab.ngsildbroker.commons.tenant;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.flywaydb.core.Flyway;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

public class TenantAwareDataSource extends AbstractRoutingDataSource {

	private Map<Object, DataSource> resolvedDataSources = new HashMap<>();

	@Autowired
	public DataSource masterDataSource;

	@Autowired
	private HikariConfig hikariConfig;

	@Autowired
	private JdbcTemplate writerJdbcTemplate;

	@Override
	public void afterPropertiesSet() {
		super.setDefaultTargetDataSource(masterDataSource);
		super.setTargetDataSources(new HashMap<>());
		super.afterPropertiesSet();
	}

	@Override
	protected Object determineCurrentLookupKey() {
		return TenantContext.getCurrentTenant();
	}

	@Override
	public DataSource determineTargetDataSource() {
		String tenantidvalue = (String) determineCurrentLookupKey();
		if (tenantidvalue == null)
			return masterDataSource;

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

	public boolean storeTenantdata(String tableName, String columnName, String tenantidvalue, String databasename)
			throws SQLException {
		synchronized (writerJdbcTemplate) {
			writerJdbcTemplate = new JdbcTemplate(masterDataSource);
		}

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

	public String findDataBaseNameByTenantId(String tenantidvalue) {
		if (tenantidvalue == null)
			return null;
		try {
			String databasename;
			synchronized (writerJdbcTemplate) {
				databasename = writerJdbcTemplate.queryForObject("SELECT database_name FROM tenant WHERE tenant_id = ?",
						String.class, tenantidvalue);
			}
			List<String> data;
			synchronized (writerJdbcTemplate) {
				data = writerJdbcTemplate.queryForList("SELECT datname FROM pg_database", String.class);
			}
			if (data.contains(databasename)) {
				return databasename;
			} else {
				String modifydatabasename = " \"" + databasename + "\"";
				String sql = "create database " + modifydatabasename + "";
				synchronized (writerJdbcTemplate) {
					writerJdbcTemplate.execute(sql);
				}
				return databasename;
			}
		} catch (EmptyResultDataAccessException e) {
			return null;
		}
	}

}