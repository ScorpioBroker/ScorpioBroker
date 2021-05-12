package eu.neclab.ngsildbroker.storagemanager.tenant;

import javax.sql.DataSource;

import org.flywaydb.core.Flyway;
import org.springframework.stereotype.Service;

@Service
public class TenantDatabaseMigrationService {

	public Boolean flywayMigrate(DataSource tenantDataSource) {
		try {
			Flyway flyway = Flyway.configure().dataSource(tenantDataSource).locations("classpath:db/migration/tenant")
					.baselineOnMigrate(true).outOfOrder(true).load();
			flyway.repair();
			flyway.migrate();
		} catch (Exception e) {
			return false;
		}

		return true;
	}
}
