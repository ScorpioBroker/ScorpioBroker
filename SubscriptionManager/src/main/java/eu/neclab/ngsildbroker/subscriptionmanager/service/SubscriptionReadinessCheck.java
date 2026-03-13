package eu.neclab.ngsildbroker.subscriptionmanager.service;

import org.eclipse.microprofile.health.HealthCheck;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.eclipse.microprofile.health.Readiness;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@Readiness
@ApplicationScoped
public class SubscriptionReadinessCheck implements HealthCheck {

	@Inject
	SubscriptionService subscriptionService;

	@Override
	public HealthCheckResponse call() {
		return HealthCheckResponse.named("Startup: subscriptions loaded")
				.status(subscriptionService.isReady())
				.build();
	}
}
