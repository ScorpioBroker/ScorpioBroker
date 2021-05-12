package eu.neclab.ngsildbroker.storagemanager.tenant;

import java.util.HashMap;
import java.util.Map;
import javax.sql.DataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import eu.neclab.ngsildbroker.commons.tenant.DBUtil;
import eu.neclab.ngsildbroker.commons.tenant.TenantContext;
import eu.neclab.ngsildbroker.storagemanager.repository.StorageWriterDAO;

public class TenantAwareDataSource extends AbstractRoutingDataSource {

	private Map<Object, DataSource> resolvedDataSources = new HashMap<>();

	@Autowired
	public DataSource masterDataSource;

	@Autowired
	private HikariConfig hikariConfig;

	@Autowired
	private TenantDatabaseMigrationService tenantDatabaseMigrationService;

	@Autowired
	StorageWriterDAO storageWriterDao;

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
			tenantDatabaseMigrationService.flywayMigrate(tenantDataSource);
			resolvedDataSources.put(tenantidvalue, tenantDataSource);
		}

		return tenantDataSource;
	}

	private DataSource createDataSourceForTenantId(String tenantidvalue) {
		String tenantDatabaseName = storageWriterDao.findDataBaseNameByTenantId(tenantidvalue);
		if (tenantDatabaseName == null)
			throw new IllegalArgumentException("Given tenant id is not valid : " + tenantidvalue);

		HikariConfig tenantHikariConfig = new HikariConfig();
		hikariConfig.copyStateTo(tenantHikariConfig);
		String tenantJdbcURL = DBUtil.databaseURLFromPostgresJdbcUrl(hikariConfig.getJdbcUrl(), tenantDatabaseName);
		tenantHikariConfig.setJdbcUrl(tenantJdbcURL);
		tenantHikariConfig.setPoolName(tenantDatabaseName + "-db-pool");
		return new HikariDataSource(tenantHikariConfig);
	}

}