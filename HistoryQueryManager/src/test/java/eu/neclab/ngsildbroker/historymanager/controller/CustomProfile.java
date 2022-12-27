package eu.neclab.ngsildbroker.historymanager.controller;

import java.util.Map;

import io.quarkus.test.junit.QuarkusTestProfile;

public class CustomProfile implements  QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
        return Map.of("profile", "kafka");
    }
    @Override
    public String getConfigProfile() {
        return "kafka";
    }
}
