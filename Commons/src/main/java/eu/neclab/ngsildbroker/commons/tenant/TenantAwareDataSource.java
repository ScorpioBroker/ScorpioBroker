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

public class TenantAwareDataSource extends AbstractRoutingDataSource {

	private Map<Object, DataSource> resolvedDataSources = new HashMap<>();

	@Autowired
	public DataSource masterDataSource;

	@Autowired
	private HikariConfig hikariConfig;
	
	@Autowired
	private JdbcTemplate writerJdbcTemplate;

//	@Autowired
//	private TenantDatabaseMigrationService tenantDatabaseMigrationService;

//	@Autowired
//	TenantWriterDAO tenantWriterDAO;

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
			tenantDataSource = createDataSourceForTenantId(tenantidvalue);
			//tenantDatabaseMigrationService.flywayMigrate(tenantDataSource);
			flywayMigrate(tenantDataSource);
			resolvedDataSources.put(tenantidvalue, tenantDataSource);
		}

		return tenantDataSource;
	}

	private DataSource createDataSourceForTenantId(String tenantidvalue) {
		//String tenantDatabaseName = tenantWriterDAO.findDataBaseNameByTenantId(tenantidvalue);
		String tenantDatabaseName = findDataBaseNameByTenantId(tenantidvalue);
		if (tenantDatabaseName == null)
			throw new IllegalArgumentException("Given tenant id is not valid : " + tenantidvalue);

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
		writerJdbcTemplate = new JdbcTemplate(masterDataSource);

		try {
			String sql;
			int n = 0;
			if (!tenantidvalue.equals(null)) {
				sql = "INSERT INTO " + tableName
						+ " (tenant_id, database_name) VALUES (?, ?) ON CONFLICT(tenant_id) DO UPDATE SET tenant_id = EXCLUDED.tenant_id";
				n = writerJdbcTemplate.update(sql, tenantidvalue, databasename);
			} else {
				sql = "DELETE FROM " + tableName + " WHERE id = ?";
				n = writerJdbcTemplate.update(sql, tenantidvalue);
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

			// SELECT EXISTS(SELECT datname FROM pg_database WHERE datname = 'tenant2');
			String databasename = writerJdbcTemplate.queryForObject(
					"SELECT database_name FROM tenant WHERE tenant_id = ?", String.class, tenantidvalue);
			List<String> data = writerJdbcTemplate.queryForList("SELECT datname FROM pg_database", String.class);
			if (data.contains(databasename)) {
				return databasename;
			} else {
				String sql = "create database " + databasename + "";
				writerJdbcTemplate.execute(sql);
				return databasename;
			}
		} catch (EmptyResultDataAccessException e) {
			return null;
		}
	}

}