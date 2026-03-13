package eu.neclab.ngsildbroker.subscriptionmanager.controller;

import java.util.Map;

import eu.neclab.ngsildbroker.subscriptionmanager.service.SubscriptionService;
import io.quarkus.test.junit.QuarkusTestProfile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomProfile implements  QuarkusTestProfile {

	private final static Logger logger = LoggerFactory.getLogger(SubscriptionService.class);

    @Override
    public Map<String, String> getConfigOverrides() {
        logger.info("Using custom test profile: " + getConfigProfile());
        return Map.of(
            "profile", "kafka", 
            "scorpio.gateway.url", "http://localhost:9090"
            );        
    }
    @Override
    public String getConfigProfile() {
        return "kafka";
    }
}
