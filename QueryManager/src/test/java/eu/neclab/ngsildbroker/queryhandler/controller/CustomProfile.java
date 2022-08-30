package eu.neclab.ngsildbroker.queryhandler.controller;

import java.util.Map;

 import io.quarkus.test.junit.QuarkusTestProfile;

 public class CustomProfile implements  QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
        return Map.of("profile", "in-memory");
    }
    @Override
    public String getConfigProfile() {
        return "in-memory";
    }
}
