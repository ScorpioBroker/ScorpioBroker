package eu.neclab.ngsildbroker.commons.storage;

import java.time.Duration;
import java.util.Map;

import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.HealthCheckResponseBuilder;
import org.eclipse.microprofile.health.Readiness;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@Readiness
@ApplicationScoped
public class TenantDatabaseHealthCheck implements HealthCheck {

	@Inject
	ConnectionManager connectionManager;

	@Override
	public HealthCheckResponse call() {
		HealthCheckResponseBuilder builder = HealthCheckResponse.named("Tenant database connections health check");
		boolean allUp = true;

		Map<String, String> tenantClientsStatus = connectionManager.testTenantClients();
		if (tenantClientsStatus.isEmpty()) {
			return builder.up().withData("tenants", "no tenant connections").build();
		}

		for (Map.Entry<String, String> entry : tenantClientsStatus.entrySet()) {
			String tenantId = entry.getKey();
			String status = entry.getValue();
			String displayName = AppConstants.INTERNAL_NULL_KEY.equals(tenantId) ? "default" : tenantId;
            builder.withData(displayName, status);
            if (!"UP".equals(status)) {
                allUp = false;
            }
		}

		builder.status(allUp);
		return builder.build();
	}
}
